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
import pdb
import xmlrpclib
import httplib
import urlparse
import time
import commands
try:
    import hashlib as md5
except:
    import md5

from optparse import Option, OptionParser

from rhn_transport import RHNTransport
from ParallelFetch import ParallelFetch
from PackageFetch import PackageFetch

def processCommandline():
    "process the commandline, setting the OPTIONS object"
    optionsTable = [
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
    
    def activate(self):
        SATELLITE_URL = "https://satellite.rhn.redhat.com/rpc/api"

        client = xmlrpclib.Server(SATELLITE_URL, verbose=0)
        key = client.auth.login(self.username, self.password)
        retval = client.satellite.activateSatellite(self.systemid, self.cert)
        print "retval from activation: %s"  % retval
        client.auth.logout(key)        
        print "Activated!"

    def sync(self, channelName, verbose=0):
        if channelName == "":
            print "No channel name specified to sync."
            return None
        print "Sync!!"
        SATELLITE_URL = "http://satellite.rhn.redhat.com/"
        rhn = RHNTransport()    
        
        satClient = xmlrpclib.ServerProxy(SATELLITE_URL + "/SAT", verbose=verbose, transport=rhn)
        
        # print "set trace"
        # pdb.set_trace()
        
        retval = satClient.authentication.check(self.systemid)
        # result = (Map) satHandler.execute("authentication.login", params);
        print "Returned from auth check : %s" % retval
        
        #auth_map = satClient.authentication.login(self.systemid)
        # print "KEY: %s " % key

        trans = RHNTransport()
        trans.addProperty("X-RHN-Satellite-XML-Dump-Version", "3.4")
        #um = xmlrpclib.Unmarshaller()
        #sparser = xmlrpclib.SlowParser(um)
        dumpClient = xmlrpclib.ServerProxy(SATELLITE_URL + "/SAT-DUMP/", verbose=verbose, transport=trans)
        print "*** calling product_names ***"
        chan_fam_xml = dumpClient.dump.product_names(self.systemid)
        print str(chan_fam_xml)
        packages = self.getChannelPackages(dumpClient, self.systemid, channelName)
        #print "Available packages = ", packages
        pkgInfo = self.getShortPackageInfo(dumpClient, self.systemid, packages)
        #print "PackageInfo = ", pkgInfo

        fetched = []
        errors = []
        if self.parallel:
            #
            # Trying new parallel approach
            #
            pf = ParallelFetch(self.systemid, SATELLITE_URL, channelName, numThreads=int(self.parallel))
            pf.addPkgList(pkgInfo.values())
            pf.start()
            fetched, errors = pf.waitForFinish()
        else:
            pf = PackageFetch(self.systemid, SATELLITE_URL, channelName)
            for index, pkg in enumerate(pkgInfo.values()):
                print "%s packages left to fetch" % (len(pkgInfo.values()) - index)
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

    def getShortPackageInfo(self, client, systemId, listOfPackages):
        dom = client.dump.packages_short(systemId, listOfPackages)
        #Example of data
        # <rhn-package-short name="perl-Sys-Virt" package-size="137602" md5sum="dfd888260a1618e0a2cb6b3b5b1feff9" 
        #  package-arch="i386" last-modified="1251397645" epoch="" version="0.2.0" release="4.el5" id="rhn-package-492050"/>
        #
        rhn_package_shorts = dom.getElementsByTagName("rhn-package-short")
        packages = {}
        for pkgShort in rhn_package_shorts:
            name, info = self.convertPkgShortToDict(pkgShort)
            packages[name] = info

        return packages

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
        info["name"] = pkgShort.getAttribute("name")
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
        return nevra, info



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

import sys
def systemExit(code, msgs=None):
    "Exit with a code and optional message(s). Saved a few lines of code."

    if msgs:
        if type(msgs) not in [type([]), type(())]:
            msgs = (msgs, )
        for msg in msgs:
            sys.stderr.write(str(msg)+'\n')
    sys.exit(code)

try:
    import os
    import socket
except KeyboardInterrupt:
    systemExit(0, "\nUser interrupted process.")

_LIBPATH = "/usr/share/"
# add to the path if need be
if _LIBPATH not in sys.path:
    sys.path.append(_LIBPATH)

def main():
    # execute
    try:
        print "Main executed"
        processCommandline()
        username = OPTIONS.username
        password = OPTIONS.password
        cert = OPTIONS.cert
        systemid = OPTIONS.systemid
        parallel = OPTIONS.parallel
        label = OPTIONS.label
        verbose = OPTIONS.verbose
        cs = Grinder(username, password, cert, systemid, parallel)
        # cs.activate()
        cs.sync(label, verbose)
        
    except KeyboardInterrupt:
        systemExit(0, "\nUser interrupted process.")

    return 0


if __name__ == '__main__':
    try:
        sys.exit(abs(main() or 0))
    except KeyboardInterrupt:
        systemExit(0, "\nUser interrupted process.")
