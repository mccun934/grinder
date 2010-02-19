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
import hashlib 
import httplib
import urlparse
import time

from optparse import Option, OptionParser

def processCommandline():
    "process the commandline, setting the OPTIONS object"
    optionsTable = [
        Option('-u', '--username', action='store', help='RHN User Account'),
        Option('-p', '--password', action='store', help='RHN Passowrd'),
        Option('-c', '--cert', action='store', help='Entitlement Certificate',
            default='/etc/sysconfig/rhn/entitlement-cert.xml'),
        Option('-s', '--systemid', action='store', help='System ID',
            default='/etc/sysconfig/rhn/systemid'),
        Option('-v', '--verbose',  action='store_true', help='verbose output'),
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


class TransportWithHeaders(xmlrpclib.Transport):
    def __init__(self):
        xmlrpclib.Transport.__init__(self)
        self.props = {}

    def addProperty(self, key, value):
        self.props[key] = value

    def getProperty(self, key):
        return self.props[key]

    def send_host(self, connection, host):
        for key in self.props:
            connection.putheader(key, self.props[key])

class RHNTransport(TransportWithHeaders):
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
    def __init__(self, username, password, cert, systemid):
        self.cert = open(cert, 'r').read()
        self.systemid = open(systemid, 'r').read()
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
        
        #auth_map = satClient.authentication.login(self.systemid)
        # print "KEY: %s " % key

        trans = RHNTransport()
        trans.addProperty("X-RHN-Satellite-XML-Dump-Version", "3.3")
        #um = xmlrpclib.Unmarshaller()
        #sparser = xmlrpclib.SlowParser(um)
        dumpClient = xmlrpclib.ServerProxy(SATELLITE_URL + "/SAT-DUMP/", verbose=1, transport=trans)
        print "*** calling product_names ***"
        chan_fam_xml = dumpClient.dump.product_names(self.systemid)
        print str(chan_fam_xml)
        channelName = "rhel-i386-server-vt-5"
        packages = self.getChannelPackages(dumpClient, self.systemid, channelName)
        #print "Available packages = ", packages
        pkgInfo = self.getShortPackageInfo(dumpClient, self.systemid, packages)
        #print "PackageInfo = ", pkgInfo
        fetched, errors = self.fetchRPMs(self.systemid, SATELLITE_URL, channelName, pkgInfo)

    def login(self, baseURL, systemId):
        client = xmlrpclib.Server(baseURL+"/SAT", verbose=0)
        authMap = client.authentication.login(systemId)
        return authMap

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

    def getFetchURL(self, channelName, fetchName):
        return "/SAT/$RHN/" + channelName + "/getPackage/" + fetchName;

    def _storeRPM(self, rpmName, response, verbose=False, dirPath="./packages"):
        if not os.path.isdir(dirPath):
            print "Creating directory: ", dirPath
            os.mkdir(dirPath)

        toRead = 64 * 1024
        bytesRead = 0
        md5Hash = hashlib.md5()
        file = open(os.path.join(dirPath, rpmName), "wb")
        while 1:
            startTime = time.time()
            data = response.read(toRead)
            endTime = time.time()
            if not data:
                break
            file.write(data)
            md5Hash.update(data)
            bytesRead += len(data)
            if verbose:
                print "%s Estimated bandwidth: %s KB/sec" % (rpmName, len(data)/((endTime-startTime)*1000))
        file.close()
        return bytesRead, md5Hash.hexdigest()

    def fetchRPMs(self, systemId, baseURL, channelName, pkgInfo):
        """
        Will return a tuple (fetched, errors)
         fetched is a list of fetched packages
         errors is a list of packages which had errors while being fetched
        """
        fetched = []
        errors = []
        authMap = self.login(baseURL, systemId)
        r = urlparse.urlsplit(baseURL)
        netloc = r.netloc
        conn = httplib.HTTPConnection(netloc)
        for nevra in pkgInfo:
            pkg = pkgInfo[nevra]
            fetchName = pkg["fetch_name"]
            fetchURL = self.getFetchURL(channelName, fetchName)
            print "Will fetch RPM for %s, from: %s" % (nevra, fetchURL)
            conn.request("GET", fetchURL, headers=authMap)
            resp = conn.getresponse()
            size, md5sum = self._storeRPM(nevra, resp)
            if size != int(pkg["package_size"]):
                print "Size mismatch, read: %s bytes, was expecting %s bytes" % (size, pkg["package_size"])
                errors.append(pkg)
                #TODO: delete bad rpm
            elif md5sum != pkg["md5sum"]:
                print "md5sum mismatch, read md5sum of: %s expected md5sum of %s" %(md5sum, pkg["md5sum"])
                errors.append(pkg)
                #TODO: delete bad rpm
            else:
                fetched.append(pkg)
        return fetched, errors

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
        cs = Grinder(username, password, cert, systemid)
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
