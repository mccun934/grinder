#!/usr/bin/python
#
# Copyright (c) 2010 Red Hat, Inc.
#
# Authors: Mike McCune
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
import xmlrpclib
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
        Option('-l', '--label', action='store', help='Channel Label ex: rhel-i386-server-vt', default=""),
        Option('-p', '--password', action='store', help='RHN Passowrd'),
        Option('-P', '--parallel', action='store', help='Number of threads to fetch in parallel'),
        Option('-s', '--systemid', action='store', help='System ID',
            default='/etc/sysconfig/rhn/systemid'),
        Option('-u', '--username', action='store', help='RHN User Account'),
        Option('-U', '--url', action='store', help='Red Hat Server URL'),
        Option('-v', '--verbose',  action='store_true', help='verbose output', default=False),

    ]
    optionParser = OptionParser(option_list=optionsTable, usage="%prog [OPTION] [<package>]")
    global OPTIONS, files
    OPTIONS, files = optionParser.parse_args()

class Grinder:
    def __init__(self, url, username, password, cert, systemid, parallel):
        self.baseURL = url
        self.cert = open(cert, 'r').read()
        self.systemid = open(systemid, 'r').read()
        self.username = username
        self.password = password
        self.parallel = parallel
        self.fetchAll = False     #default is only fetch latest packages
        self.parallelFetch = None
    
    def getFetchAllPackages(self):
        return self.fetchAll
    
    def setFetchAllPackages(self, val):
        self.fetchAll = val

    def activate(self):
        SATELLITE_URL = "%s/rpc/api" % (self.baseURL)
        client = xmlrpclib.Server(SATELLITE_URL, verbose=0)
        key = client.auth.login(self.username, self.password)
        retval = client.satellite.activateSatellite(self.systemid, self.cert)
        print "retval from activation: %s"  % retval
        client.auth.logout(key)        
        print "Activated!"

    def stop(self):
        if (self.parallelFetch):
            self.parallelFetch.stop()

    def sync(self, channelLabel, verbose=0):
        startTime = time.time()
        if channelLabel == "":
            LOG.critical("No channel label specified to sync, abort sync.")
            raise NoChannelLabelException()
        LOG.info("sync(%s, %s) invoked" % (channelLabel, verbose))
        satClient = xmlrpclib.ServerProxy(self.baseURL + "/SAT", verbose=verbose)
        # print "set trace"
        # pdb.set_trace()
        try:
            retval = satClient.authentication.check(self.systemid)
            LOG.debug("Returned from auth check : %s" % retval)
        except xmlrpclib.Fault, err:
            LOG.critical("Unable to authenticate this systemId: %s" % (self.systemid))
            LOG.critical(err.faultString)
            LOG.critical(err.faultCode)
            raise BadSystemIdException()

        satDumpClient = SatDumpClient(self.baseURL, verbose=verbose)
        LOG.debug("*** calling product_names ***")
        packages = satDumpClient.getChannelPackages(self.systemid, channelLabel)
        LOG.info("%s packages are available, getting list of short metadata now." % (len(packages)))
        pkgInfo = satDumpClient.getShortPackageInfo(self.systemid, packages, filterLatest = not self.fetchAll)
        LOG.info("%s packages have been marked to be fetched" % (len(pkgInfo.values())))

        fetched = []
        errors = []
        if self.parallel:
            #
            # Trying new parallel approach
            #
            numThreads = int(self.parallel)
            LOG.info("Running in parallel fetch mode with %s threads" % (numThreads))
            self.parallelFetch = ParallelFetch(self.systemid, self.baseURL, 
                    channelLabel, numThreads=numThreads)
            self.parallelFetch.addPkgList(pkgInfo.values())
            self.parallelFetch.start()
            fetched, errors = self.parallelFetch.waitForFinish()
        else:
            LOG.info("Running in serial fetch mode")
            pf = PackageFetch(self.systemid, SATELLITE_URL, channelLabel)
            for index, pkg in enumerate(pkgInfo.values()):
                LOG.info("%s packages left to fetch" % (len(pkgInfo.values()) - index))
                if pf.fetchRPM(pkg):
                    fetched.append(pkg)
                else:
                    errors.append(pkg)
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


# Global instance of Grinder so we can issue a stop command 
# in a signal handler for CTRL-C
GRINDER = None
#
# Registering a signal handler on SIGINT to help in the 
# parallel case, when we need a way to stop all the threads 
# when someone CTRL-C's
#
def handleKeyboardInterrupt(signalNumer, frame):
    LOG.error("SIGINT caught, will stop process.")
    GRINDER.stop()

signal.signal(signal.SIGINT, handleKeyboardInterrupt)

if __name__ == '__main__':
    LOG.debug("Main executed")
    processCommandline()
    allPackages = OPTIONS.all
    username = OPTIONS.username
    password = OPTIONS.password
    cert = OPTIONS.cert
    systemid = OPTIONS.systemid
    parallel = OPTIONS.parallel
    label = OPTIONS.label
    verbose = OPTIONS.verbose
    url = OPTIONS.url
    if not url:
        url = "https://satellite.rhn.redhat.com"
    setupLogging(verbose)
    GRINDER = Grinder(url, username, password, cert, systemid, parallel)
    GRINDER.setFetchAllPackages(allPackages)
    # GRINDER.activate()
    # TODO:
    # Assumption:  we are writing packages to current directory as "channel-label"
    # Add an option so we can specifiy a base directory to store packages at
    GRINDER.sync(label, verbose)
    GRINDER.createRepo(label)
