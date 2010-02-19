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
import gzip
import pdb
import StringIO
import xmlrpclib
import xml.dom.minidom

from optparse import Option, OptionParser

def processCommandline():
    "process the commandline, setting the OPTIONS object"
    optionsTable = [
        Option('-u', '--username',            action='store',
            help=' RHN User Account'),
        Option('-p', '--password',        action='store',
            help='RHN Passowrd'),
        Option('-v', '--verbose',         action='store_true',
            help='verbose output'),
    ]
    optionParser = OptionParser(option_list=optionsTable, usage="%prog [OPTION] [<package>]")
    global OPTIONS, files
    OPTIONS, files = optionParser.parse_args()


#Issues
#1) Need add custom header
#2) Data comes back gzipped, need to read it (seems like latest python in svn has support, yet python 2.6 doesn't)
#3) xml message doesn't conform to xmlrpc standard
#
class GzipDecodedString(gzip.GzipFile if gzip else object):
    """a file-like object to decode a response encoded with the gzip
    method, as described in RFC 1952.
    """
    def __init__(self, data):
        if not gzip:
            raise NotImplementedError
        self.stringio = StringIO.StringIO(data)
        gzip.GzipFile.__init__(self, mode="rb", fileobj=self.stringio)

    def close(self):
        gzip.GzipFile.close(self)
        self.stringio.close()


##
## Look at "extra_headers in Transport"  
##
class RHNTransport(xmlrpclib.Transport):
    def __init__(self):
        xmlrpclib.Transport.__init__(self)
        self.props = {}
        self.addProperty("X-RHN-Satellite-XML-Dump-Version", "3.3")

    def addProperty(self, key, value):
        self.props[key] = value

    def getProperty(self, key):
        return self.props[key]

    def send_host(self, connection, host):
        # print "self.props = ", self.props
        for key in self.props:
            # print "setting header for %s = %s" % (key, self.props[key])
            connection.putheader(key, self.props[key])

    ##
    # Parse response (alternate interface).  This is similar to the
    # parse_response method, but also provides direct access to the
    # underlying socket object (where available).
    #
    # @param file Stream.
    # @param sock Socket handle (or None, if the socket object
    #    could not be accessed).
    # @return Response tuple and target method.

    def _parse_response(self, file, sock):
        # read response from input file/socket, and parse it
        response = ""
        while 1:
            if sock:
                snippet = sock.recv(1024)
            else:
                snippet = file.read(1024)
            if not snippet:
                break
            response += snippet

        #
        # TODO:
        # Would prefer to grab header and read if data is gzip or not before doing below, but not
        # sure how to access header here.
        #
        try: 
            gzipDecodeStr = GzipDecodedString(response)
            unzippedResponse = gzipDecodeStr.read()
        except IOError, e:
            #Error messages from RHN are not gzipped, bust most (maybe all) data calls will be
            print "Caught exception when trying to ungzip response"
            unzippedResponse = response

        if self.verbose:
            print "body:", repr(unzippedResponse)
        
        dom = xml.dom.minidom.parseString(unzippedResponse)
        if file:
            file.close()
        if sock:
            sock.close()

        #HACK, __request expects what we return to support "len()", so throwing this in a list for now
        return [dom]


class Grinder():
    def __init__(self, username, password):
        self.cert = open('/etc/sysconfig/rhn/entitlement-cert.xml', 'r').read()
        self.systemid = open('/etc/sysconfig/rhn/systemid', 'r').read()
        self.username = username
        self.password = password

    
    def activate(self):
        SATELLITE_URL = "https://satellite.rhn.redhat.com/rpc/api"

        client = xmlrpclib.Server(SATELLITE_URL, verbose=0)
        key = client.auth.login(self.username, self.password)
        retval = client.satellite.activateSatellite(self.systemid, self.cert)
        print "retval from activation: %s"  % retval
        client.auth.logout(key)        
        print "Activated!"

    def sync(self):
        print "Sync!!"
        SATELLITE_URL = "http://satellite.rhn.redhat.com/"
        rhn = RHNTransport()    
        
        satClient = xmlrpclib.ServerProxy(SATELLITE_URL + "/SAT", verbose=0, transport=rhn)
        
        # print "set trace"
        # pdb.set_trace()
        
        retval = satClient.authentication.check(self.systemid)
        # result = (Map) satHandler.execute("authentication.login", params);
        print "Returned from auth check : %s" % retval
        
        auth_map = satClient.authentication.login(self.systemid)
        # print "KEY: %s " % key

        rhn = RHNTransport()
        um = xmlrpclib.Unmarshaller()
        sparser = xmlrpclib.SlowParser(um)
        dumpClient = xmlrpclib.ServerProxy(SATELLITE_URL + "/SAT-DUMP/", verbose=0, transport=rhn)
        print "*** calling product_names ***"
        chan_fam_xml = dumpClient.dump.product_names(self.systemid)
        # print str(chan_fam_xml)
        packages = self.getChannelPackages(dumpClient, self.systemid, "rhel-i386-server-vt-5")
        #print "Available packages = ", packages
        pkgInfo = self.getShortPackageInfo(dumpClient, self.systemid, packages)
        print "PackageInfo = ", pkgInfo


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
        nevra += info["version"] + "-" + info["release"]
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
        return nevra, info



    

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
        cs = Grinder(username, password)
        # cs.activate()
        cs.sync()
        
    except KeyboardInterrupt:
        systemExit(0, "\nUser interrupted process.")

    return 0


if __name__ == '__main__':
    try:
        sys.exit(abs(main() or 0))
    except KeyboardInterrupt:
        systemExit(0, "\nUser interrupted process.")
