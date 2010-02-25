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
        Option('-u', '--username', action='store', help='RHN User Account'),
        Option('-p', '--password', action='store', help='RHN Passowrd'),
        Option('-c', '--cert', action='store', help='Entitlement Certificate',
            default='/etc/sysconfig/rhn/entitlement-cert.xml'),
        Option('-s', '--systemid', action='store', help='System ID',
            default='/etc/sysconfig/rhn/systemid'),
        Option('-P', '--parallel', action='store', help='Number of threads to fetch in parallel'),
        Option('-l', '--label', action='store', help='Channel Label ex: rhel-i386-server-vt', default=""),
        Option('-v', '--verbose',  action='store_true', help='verbose output', default=False),
    ]
    optionParser = OptionParser(option_list=optionsTable, usage="%prog [OPTION] [<package>]")
    global OPTIONS, files
    OPTIONS, files = optionParser.parse_args()

class Grinder:
    def __init__(self, username, password, cert, systemid, parallel):
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
        SATELLITE_URL = "https://satellite.rhn.redhat.com/rpc/api"

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
        if channelLabel == "":
            LOG.critical("No channel label specified to sync, abort sync.")
            raise NoChannelLabelException()
        LOG.info("sync(%s, %s) invoked" % (channelLabel, verbose))
        SATELLITE_URL = "http://satellite.rhn.redhat.com/"
        rhn = RHNTransport()    
        
        satClient = xmlrpclib.ServerProxy(SATELLITE_URL + "/SAT", verbose=verbose, transport=rhn)
        
        # print "set trace"
        # pdb.set_trace()
        
        retval = satClient.authentication.check(self.systemid)
        # result = (Map) satHandler.execute("authentication.login", params);
        LOG.debug("Returned from auth check : %s" % retval)
        
        #auth_map = satClient.authentication.login(self.systemid)
        # print "KEY: %s " % key

        trans = RHNTransport()
        trans.addProperty("X-RHN-Satellite-XML-Dump-Version", "3.4")
        #um = xmlrpclib.Unmarshaller()
        #sparser = xmlrpclib.SlowParser(um)
        dumpClient = xmlrpclib.ServerProxy(SATELLITE_URL + "/SAT-DUMP/", verbose=verbose, transport=trans)
        LOG.debug("*** calling product_names ***")
        chan_fam_xml = dumpClient.dump.product_names(self.systemid)
        LOG.debug(str(chan_fam_xml))
        packages = self.getChannelPackages(dumpClient, self.systemid, channelLabel)
        #print "Available packages = ", packages
        LOG.info("%s packages are available, getting list of short metadata now." % (len(packages)))
        pkgInfo = self.getShortPackageInfo(dumpClient, self.systemid, packages, fetchAll=self.fetchAll)
        LOG.info("%s packages have been marked to be fetched" % (len(pkgInfo.values())))
        #print "PackageInfo = ", pkgInfo

        fetched = []
        errors = []
        if self.parallel:
            #
            # Trying new parallel approach
            #
            numThreads = int(self.parallel)
            LOG.info("Running in parallel fetch mode with %s threads" % (numThreads))
            self.parallelFetch = ParallelFetch(self.systemid, SATELLITE_URL, 
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
        return fetched, errors

    def getChannelFamilies(self, client, systemId):
        return client.dump.channel_families(systemId)

    def getChannelPackages(self, client, systemId, channelLabel):
        dom = client.dump.channels(systemId, [channelLabel])
        rhn_channel = dom.getElementsByTagName("rhn-channel")[0]
        packages = rhn_channel.getAttribute("packages")
        return packages.split(" ")

    def getShortPackageInfo(self, client, systemId, listOfPackages, fetchAll=False):
        dom = client.dump.packages_short(systemId, listOfPackages)
        #Example of data
        # <rhn-package-short name="perl-Sys-Virt" package-size="137602" md5sum="dfd888260a1618e0a2cb6b3b5b1feff9" 
        #  package-arch="i386" last-modified="1251397645" epoch="" version="0.2.0" release="4.el5" id="rhn-package-492050"/>
        #
        rhn_package_shorts = dom.getElementsByTagName("rhn-package-short")
        packages = {}
        for pkgShort in rhn_package_shorts:
            pkgName, nevra, info = self.convertPkgShortToDict(pkgShort)
            if not fetchAll:
                # only fetching latest packages, so dict key of 
                # 'name'.'arch' is what we want to be unique
                pkgKey = pkgName+"." + info["arch"]
                if not packages.has_key(pkgKey):
                    LOG.debug("Adding package %s to queue", nevra)
                    packages[pkgKey] = info
                else:
                    #package already in our dict, so check to keep only latest nevra
                    potentialOld = packages.get(pkgKey)
                    LOG.debug("A version for %s already exists, will need to compare to determine latest" \
                        % (pkgKey))
                    LOG.debug("Existing: %s, new addition: %s" % (potentialOld["nevra"], nevra))
                    if self.isNewerEVR(info, potentialOld):
                        LOG.debug("Removing %s and adding %s" % (potentialOld["nevra"], nevra))
                        packages[pkgKey] = info
            else:
                # Fetching all packages, not just latest.  
                # dict key needs to contain full nevra to be unique now
                packages[nevra] = info
        return packages


    def isNewerEVR(self, pkgOne, pkgTwo):
        # Only check for packages of same arch
        if pkgOne["arch"] != pkgTwo["arch"]:
            return False
        e1 = pkgOne["epoch"]
        v1 = pkgOne["version"]
        r1 = pkgOne["release"]

        e2 = pkgTwo["epoch"]
        v2 = pkgTwo["version"]
        r2 = pkgTwo["release"]

        if rpmUtils.miscutils.compareEVR((e1,v1,r1), (e2,v2,r2)) == 1:
            return True
        return False
    
    def isPkgShortNewer(self, pkgOne, pkgTwo):
        # Only check for packages of same arch
        if pkgOne["arch"] != pkgTwo["arch"]:
            return False
        if pkgOne["epoch"] < pkgTwo["epoch"]:
            return False
        if pkgOne["epoch"] > pkgTwo["epoch"]:
            return True
        if pkgOne["version"] > pkgTwo["version"]:
            return True
        if pkgOne["version"] == pkgTwo["version"]:
            if pkgOne["release"] > pkgTwo["release"]:
                return True
        return False

    def formNEVRA(self, info):
        nevra = info["name"]
        epoch = info["epoch"]
        if epoch:
            nevra += "-" + epoch + ":"
        nevra += "-" + info["version"] + "-" + info["release"]
        arch = info["arch"]
        if arch:
            nevra += "." + arch
        nevra += ".rpm"
        return nevra

    def formFetchName(self, info):
        release_epoch = info["release"] + ":" + info["epoch"]
        return info["name"] + "-" + info["version"] + "-" + release_epoch + "." + info["arch"] + ".rpm"

    def convertPkgShortToDict(self, pkgShort):
        info = {}
        name = pkgShort.getAttribute("name")
        info["name"] = name
        info["package_size"] = pkgShort.getAttribute("package-size")
        info["md5sum"] = pkgShort.getAttribute("md5sum")
        info["arch"] = pkgShort.getAttribute("package-arch")
        info["last_modified"] = pkgShort.getAttribute("last-modified")
        info["epoch"] = pkgShort.getAttribute("epoch")
        info["version"] = pkgShort.getAttribute("version")
        info["release"] = pkgShort.getAttribute("release")
        info["id"] = pkgShort.getAttribute("id")
        info["fetch_name"] = self.formFetchName(info)
        nevra = self.formNEVRA(info)
        info["nevra"] = nevra
        return name, nevra, info



    def createRepo(dir):
        status, out = commands.getstatusoutput('createrepo %s' % dir)

        class CreateRepoError:
            def __init__(self, output):
                self.output = output

            def __str__(self):
                return self.output

        if status != 0:
            raise CreateRepoError(out)

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
    setupLogging(verbose)
    GRINDER = Grinder(username, password, cert, systemid, parallel)
    GRINDER.setFetchAllPackages(allPackages)
    # GRINDER.activate()
    GRINDER.sync(label, verbose)
