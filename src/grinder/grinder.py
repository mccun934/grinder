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
import os
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
        Option('-b', '--basepath', action='store', help='Path RPMs are stored'),
        Option('-c', '--cert', action='store', help='Entitlement Certificate'),
        Option('-C', '--config', action='store', help='Configuration file',
            default='/etc/grinder/grinder.yml'),
        Option('-L', '--listchannels', action='store_true', help='List all channels we have access to synchronize'),
        Option('-p', '--password', action='store', help='RHN Passowrd'),
        Option('-P', '--parallel', action='store', 
            help='Number of threads to fetch in parallel.'),
        Option('-r', '--removeold', action='store_true', help='Remove older rpms', default=False),
        Option('-s', '--systemid', action='store', help='System ID'),
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
        self.killcount = 0
        self.removeOldPackages = False

    def setRemoveOldPackages(self, value):
        self.removeOldPackages = value

    def getRemoveOldPackages(self):
        return self.removeOldPackages

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
            if(not self.username or not self.password):
                raise SystemNotActivatedException()
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
            print("\nProduct : %s\n" % (d["label"]))
            for lbl in d["channel_labels"]:
                print("    %s" % (lbl))


    def sync(self, channelLabel, savePath=None, verbose=0):
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
                channelLabel, numThreads=numThreads, savePath=savePath)
        self.parallelFetch.addPkgList(pkgInfo.values())
        self.parallelFetch.start()
        fetched, errors = self.parallelFetch.waitForFinish()
        endTime = time.time()
        LOG.info("Sync'd <%s> %s packages, %s errors, completed in %s seconds" \
                % (channelLabel, len(fetched), len(errors), (endTime-startTime)))
        if self.removeOldPackages:
            self.runRemoveOldPackages(savePath)
        return fetched, errors

    def runRemoveOldPackages(self, path):
        """ Will scan input directory and remove all but the latest rpm version """
        rpms = {}
        import glob
        # Get list of *.rpms in directory
        rpmFiles = glob.glob(path+"/*.rpm")
        for filename in rpmFiles:
            # Split into NEVRA
            name, version, release, epoch, arch = rpmUtils.miscutils.splitFilename(filename)
            key = name + "." + arch
            if not rpms.has_key(key):
                rpms[name+"."+arch] = (name, version, release, epoch, arch, filename)
            else:
                # Check to see if current rpm is newer than we have in the dict
                name2, version2, release2, epoch2, arch2, filename2 = rpms[key]
                cmpVal = rpmUtils.miscutils.compareEVR(
                    (epoch, version, release), 
                    (epoch2, version2, release2))
                if cmpVal == 1:
                    LOG.debug("Remove %s because it's older than %s" % (filename2, filename))
                    os.remove(filename2)
                    rpms[name+"."+arch] = (name, version, release, epoch, arch, filename)
                else:
                    LOG.debug("Remove %s because it's older than %s" % (filename, filename2))
                    os.remove(filename)



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
    if (GRINDER.killcount > 0):
        LOG.error("force quitting.")
        sys.exit()
    if (GRINDER.killcount == 0):
        GRINDER.killcount = 1
        msg = "SIGINT caught, will finish currently downloading" + \
              " packages and exit. Press CTRL+C again to force quit"
        LOG.error(msg)
        GRINDER.stop()

signal.signal(signal.SIGINT, handleKeyboardInterrupt)

def main():
    LOG.debug("Main executed")
    processCommandline()
    verbose = OPTIONS.verbose
    setupLogging(verbose)
    
    configFile = OPTIONS.config
    configInfo = {}
    if os.path.isfile(configFile):
        try:
            import yaml
            raw = open(configFile).read()
            configInfo = yaml.load(raw)
        except:
            LOG.info("Unable to parse config file: %s. Using command line options only." % (configFile))
            configInfo = {}

    if OPTIONS.all:
        allPackages = OPTIONS.all
    elif configInfo.has_key("all"):
        allPackages = configInfo["all"]
    else:
        allPackages = False
    LOG.debug("allPackages = %s" % (allPackages))

    if OPTIONS.username:
        username = OPTIONS.username
    elif configInfo.has_key("username"):
        username = configInfo["username"]
    else:
        username = None
    LOG.debug("username = %s" % (username))

    if OPTIONS.password:
        password = OPTIONS.password
        LOG.debug("password = from command line")
    elif configInfo.has_key("password"):
        password = configInfo["password"]
        LOG.debug("password = from config file")
    else:
        password = None
        LOG.debug("password never specified")

    if OPTIONS.cert:
        cert = OPTIONS.cert
    elif configInfo.has_key("cert"):
        cert = configInfo["cert"]
    else:
        cert = "/etc/sysconfig/rhn/entitlement-cert.xml"
    LOG.debug("cert = %s" % (cert))

    if OPTIONS.systemid:
        systemid = OPTIONS.systemid
    elif configInfo.has_key("systemid"):
        systemid = configInfo["systemid"]
    else:
        systemid = "/etc/sysconfig/rhn/systemid"
    LOG.debug("systemid = %s" % (systemid))

    if OPTIONS.parallel:
        parallel = OPTIONS.parallel
    elif configInfo.has_key("parallel"):
        parallel = configInfo["parallel"]
    else:
        parallel = 5
    LOG.debug("parallel = %s" % (parallel))

    if OPTIONS.url:
        url = OPTIONS.url
    elif configInfo.has_key("url"):
        url = configInfo["url"]
    else:
        url = "https://satellite.rhn.redhat.com"
    LOG.debug("url = %s" % (url))

    if OPTIONS.removeold:
        removeold = OPTIONS.removeold
    elif configInfo.has_key("removeold"):
        removeold = configInfo["removeold"]
    else:
        removeold = False

    if allPackages and removeold:
        print "Conflicting options specified.  Fetch ALL packages AND remove older packages."
        print "This combination of options is not supported."
        print "Please remove one of these options and re-try"
        sys.exit(1)

    if OPTIONS.basepath:
        basepath = OPTIONS.basepath
    elif configInfo.has_key("basepath"):
        basepath = configInfo["basepath"]
    else:
        basepath = "./"
    LOG.debug("basepath = %s" % (basepath))

    channelLabels = {}
    if configInfo.has_key("channels"):
        channels = configInfo["channels"]
        for c in channels:
            channelLabels[c['label']] = c['relpath']

    # Add extra arguments from CLI to channelLabels
    # extra arguments will default to a save path of their channel label
    for a in args:
        channelLabels[a] = a

    listchannels = OPTIONS.listchannels
    global GRINDER 
    GRINDER = Grinder(url, username, password, cert, 
        systemid, parallel, verbose)
    GRINDER.setFetchAllPackages(allPackages)
    GRINDER.setRemoveOldPackages(removeold)
    GRINDER.setSkipProductList(["rh-public", "k12ltsp", "education"])
    GRINDER.activate()
    if (listchannels):
        GRINDER.displayListOfChannels()
        sys.exit(0)
    badChannels = GRINDER.checkChannels(channelLabels.keys())
    for b in badChannels:
        print "'%s' can not be found as a channel available to download" % (b)
    if len(badChannels) > 0:
        sys.exit(1)
    if len(channelLabels.keys()) < 1:
        print "No channels specified to sync"
        sys.exit(1)
    for cl in channelLabels.keys():
        dirPath = os.path.join(basepath, channelLabels[cl])
        LOG.info("Syncing '%s' to '%s'" % (cl, dirPath))
        GRINDER.sync(cl, savePath=dirPath, verbose=verbose)
        if (GRINDER.killcount == 0):
            GRINDER.createRepo(dirPath)

if __name__ == "__main__":
    main()


