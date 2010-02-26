#!/usr/bin/python
#
# Copyright (c) 2010 Red Hat, Inc.
#
# Authors: Mike McCune, John Matthews
#
# This software is licensed to you under the GNU General Public License,
# version 2 (GPLv2). There is NO WARRANTY for this software, express or
# implied, including the implied warranties of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. You should have received a copy of GPLv2
# along with this software; if not, see
# http://www.gnu.org/licenses/old-licenses/gpl-2.0.txt.
#
# Red Hat trademarks are not licensed under GPLv2. No permission is
# granted to use or replicate Red Hat trademarks that are incorporated
# in this software or its documentation.
#
import sys
import pdb

import httplib
import urlparse
import time
import commands
import rpmUtils
import rpmUtils.miscutils
try:
    import hashlib as md5
except:
    import md5
import logging
import signal
from optparse import Option, OptionParser
from xmlrpclib import Fault
from rhn_api import RhnApi
from rhn_api import getRhnApi
from rhn_transport import RHNTransport
from ParallelFetch import ParallelFetch
from PackageFetch import PackageFetch
from GrinderExceptions import *
from SatDumpClient import SatDumpClient

GRINDER_LOG_FILENAME = "./log-grinder.out"
LOG = logging.getLogger("grinder")

def setupLogging(verbose):
    
    logging.basicConfig(filename=GRINDER_LOG_FILENAME, level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M', filemode='w')
    console = logging.StreamHandler()
    if verbose:
        console.setLevel(logging.DEBUG)
    else:
        console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)

def processCommandline():
    "process the commandline, setting the OPTIONS object"
    optionsTable = [
        Option('-a', '--all', action='store_true', 
            help='Fetch ALL packages from a channel, not just latest'),
        Option('-c', '--cert', action='store', help='Entitlement Certificate',
            default='/etc/sysconfig/rhn/entitlement-cert.xml'),
        Option('-L', '--listchannels', action='store_true', help='List all channels we have access to synchronize', default=""),
        Option('-p', '--password', action='store', help='RHN Passowrd'),
        Option('-P', '--parallel', action='store', 
            help='Number of threads to fetch in parallel.  Defaults to 1', default=1),
        Option('-s', '--systemid', action='store', help='System ID',
            default='/etc/sysconfig/rhn/systemid'),
        Option('-u', '--username', action='store', help='RHN User Account'),
        Option('-U', '--url', action='store', help='Red Hat Server URL'),
        Option('-v', '--verbose',  action='store_true', help='verbose output', default=False),

    ]
    optionParser = OptionParser(option_list=optionsTable, usage="%prog [OPTION] channel_label1 channel_label2, etc")
    global OPTIONS, args
    OPTIONS, args = optionParser.parse_args()

class Grinder:
    def __init__(self, url, username, password, cert, systemid, parallel, verbose):
        self.baseURL = url
        self.cert = open(cert, 'r').read()
        self.systemid = open(systemid, 'r').read()
        self.username = username
        self.password = password
        self.parallel = parallel
        self.fetchAll = False     #default is only fetch latest packages
        self.parallelFetch = None
        self.skipProductList = []
        self.skipPackageList = []
        self.verbose = verbose

    def getFetchAllPackages(self):
        return self.fetchAll
    
    def setFetchAllPackages(self, val):
        self.fetchAll = val
    
    def getSkipProductList(self):
        return self.skipProductList

    def setSkipProductList(self, skipProductList):
        self.skipProductList = skipProductList
    
    def getSkipPackageList(self):
        return self.skipPackageList

    def setSkipPackageList(self, skipPackageList):
        self.skipPackageList = skipPackageList

    def deactivate(self):
        SATELLITE_URL = "%s/rpc/api" % (self.baseURL)
        client = getRhnApi(SATELLITE_URL, verbose=0)
        key = client.auth.login(self.username, self.password)
        retval = client.satellite.deactivateSatellite(self.systemid)
        print "retval from deactivation: %s"  % retval
        client.auth.logout(key)        
        print "Deactivated!"

    def activate(self):
        rhn = RHNTransport()    
        satClient = getRhnApi(self.baseURL + "/SAT", 
            verbose=self.verbose, transport=rhn)
        # First check if we are active
        active = False
        retval = satClient.authentication.check(self.systemid)
        LOG.debug("AUTH CHECK: %s " % str(retval))
        if (retval == 1):
            LOG.debug("We are activated ... continue!")
            active = True
        else:
            LOG.debug("Not active")
            
        if (not active): 
            SATELLITE_URL = "%s/rpc/api" % (self.baseURL)
            client = RhnApi(SATELLITE_URL, verbose=0)
            key = client.auth.login(self.username, self.password)
            retval = client.satellite.activateSatellite(self.systemid, self.cert)
            LOG.debug("retval from activation: %s"  % retval)
            if (retval != 1):
                raise CantActivateException()
            client.auth.logout(key)        
            LOG.debug("Activated!")

    def stop(self):
        if (self.parallelFetch):
            self.parallelFetch.stop()

    def checkChannels(self, channelsToSync):
        """
        Input:
            channelsToSync - list of channels to sync
        Output:
             list containing bad channel names
        """
        satDumpClient = SatDumpClient(self.baseURL)
        channelFamilies = satDumpClient.getChannelFamilies(self.systemid)
        badChannel = []
        for channelLabel in channelsToSync:
            found = False
            for d in channelFamilies.values():
                if channelLabel in d["channel_labels"]:
                    LOG.debug("Found %s under %s" % (channelLabel, d["label"]))
                    found = True
                    break
            if not found:
                LOG.debug("Unable to find %s, adding it to badChannel list" % (channelLabel))
                badChannel.append(channelLabel)
        return badChannel


    def displayListOfChannels(self):
        satDumpClient = SatDumpClient(self.baseURL)
        channelFamilies = satDumpClient.getChannelFamilies(self.systemid)
        print("List of channels:")
        for d in channelFamilies.values():
            if (d["label"] in self.skipProductList):
                LOG.debug("Skipping display of %s because it is in product skip list" % (d["label"]))
                continue
            print("\nProduct Family: %s" % (d["label"]))
            for lbl in d["channel_labels"]:
                print("\tChannel Label: %s" % (lbl))


    def sync(self, channelLabel, verbose=0):
        startTime = time.time()
        if channelLabel == "":
            LOG.critical("No channel label specified to sync, abort sync.")
            raise NoChannelLabelException()
        LOG.info("sync(%s, %s) invoked" % (channelLabel, verbose))
        satDumpClient = SatDumpClient(self.baseURL, verbose=verbose)
        LOG.debug("*** calling product_names ***")
        packages = satDumpClient.getChannelPackages(self.systemid, channelLabel)
        LOG.info("%s packages are available, getting list of short metadata now." % (len(packages)))
        pkgInfo = satDumpClient.getShortPackageInfo(self.systemid, packages, filterLatest = not self.fetchAll)
        LOG.info("%s packages have been marked to be fetched" % (len(pkgInfo.values())))

        fetched = []
        errors = []
        numThreads = int(self.parallel)
        LOG.info("Running in parallel fetch mode with %s threads" % (numThreads))
        self.parallelFetch = ParallelFetch(self.systemid, self.baseURL, 
                channelLabel, numThreads=numThreads)
        self.parallelFetch.addPkgList(pkgInfo.values())
        self.parallelFetch.start()
        fetched, errors = self.parallelFetch.waitForFinish()
        endTime = time.time()
        LOG.info("Sync'd <%s> %s packages, %s errors, completed in %s seconds" \
                % (channelLabel, len(fetched), len(errors), (endTime-startTime)))
        return fetched, errors

    def createRepo(self, dir):
        startTime = time.time()
        status, out = commands.getstatusoutput('createrepo --update %s' % dir)

        class CreateRepoError:
            def __init__(self, output):
                self.output = output

            def __str__(self):
                return self.output

        if status != 0:
            raise CreateRepoError(out)
        endTime = time.time()
        LOG.info("createrepo on %s finished in %s seconds" % (dir, (endTime-startTime)))
        return status, out



if __name__ != '__main__':
    raise ImportError, "module cannot be imported"

_LIBPATH = "/usr/share/"
# add to the path if need be
if _LIBPATH not in sys.path:
    sys.path.append(_LIBPATH)


#
# Registering a signal handler on SIGINT to help in the 
# parallel case, when we need a way to stop all the threads 
# when someone CTRL-C's
#
def handleKeyboardInterrupt(signalNumer, frame):
    LOG.error("SIGINT caught, stopping synchronization.")
    GRINDER.stop()
    # grinder's stop() call will wait for the child threads to realize 
    # they need to be stopped.  then after that we need to stop completely
    # otherwise the remainder of the __main__ will continue
    sys.exit()

signal.signal(signal.SIGINT, handleKeyboardInterrupt)

def main():
    LOG.debug("Main executed")
    processCommandline()
    allPackages = OPTIONS.all
    username = OPTIONS.username
    password = OPTIONS.password
    cert = OPTIONS.cert
    systemid = OPTIONS.systemid
    parallel = OPTIONS.parallel
    listchannels = OPTIONS.listchannels
    verbose = OPTIONS.verbose
    url = OPTIONS.url
    if not url:
        url = "https://satellite.rhn.redhat.com"
    setupLogging(verbose)
    global GRINDER 
    GRINDER = Grinder(url, username, password, cert, 
        systemid, parallel, verbose)
    GRINDER.setFetchAllPackages(allPackages)
    GRINDER.setSkipProductList(["rh-public", "k12ltsp", "education"])
    GRINDER.activate()
    if (listchannels):
        GRINDER.displayListOfChannels()
        sys.exit(0)
    badChannels = GRINDER.checkChannels(args)
    for b in badChannels:
        print "'%s' can not be found as a channel available to download" % (b)
    if len(badChannels) > 0:
        sys.exit(1)
    for cl in args:
        GRINDER.sync(cl, verbose)
        GRINDER.createRepo(cl)

if __name__ == "__main__":
    main()


