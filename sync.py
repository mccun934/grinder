#!/usr/bin/env python

import xmlrpclib
import gzip
import StringIO
import hashlib
import httplib
import urlparse
import xml.dom.minidom
import time
import os

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
        print "self.props = ", self.props
        for key in self.props:
            print "setting header for %s = %s" % (key, self.props[key])
            connection.putheader(key, self.props[key])

##
## Look at "extra_headers in Transport"  
##
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


def getSystemId(sysIdPath):
    return file(sysIdPath, "r").read()

def createLoginClient(url):
    trans = TransportWithHeaders()
    trans.addProperty("X-RHN-Satellite-XML-Dump-Version", "3.3")
    return xmlrpclib.Server(url, transport=trans, verbose=True)


def createClient(url):
    trans = RHNTransport()
    trans.addProperty("X-RHN-Satellite-XML-Dump-Version", "3.3")
    return xmlrpclib.Server(url, transport=trans, verbose=True)

def getChannelFamilies(client, systemId):
    return client.dump.channel_families(systemId)

def getChannelPackages(client, systemId, channelLabel):
    dom = client.dump.channels(systemId, [channelLabel])
    rhn_channel = dom.getElementsByTagName("rhn-channel")[0]
    packages = rhn_channel.getAttribute("packages")
    return packages.split(" ")

def getShortPackageInfo(client, systemId, listOfPackages):
    dom = client.dump.packages_short(systemId, listOfPackages)
    #Example of data
    # <rhn-package-short name="perl-Sys-Virt" package-size="137602" md5sum="dfd888260a1618e0a2cb6b3b5b1feff9" 
    #  package-arch="i386" last-modified="1251397645" epoch="" version="0.2.0" release="4.el5" id="rhn-package-492050"/>
    #
    rhn_package_shorts = dom.getElementsByTagName("rhn-package-short")
    packages = {}
    for pkgShort in rhn_package_shorts:
        name, info = convertPkgShortToDict(pkgShort)
        packages[name] = info

    return packages

def formNEVRA(info):
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

def formFetchName(info):
    release_epoch = info["release"] + ":" + info["epoch"]
    return info["name"] + "-" + info["version"] + "-" + release_epoch + "." + info["arch"] + ".rpm"

def convertPkgShortToDict(pkgShort):
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
    info["fetch_name"] = formFetchName(info)
    nevra = formNEVRA(info)
    info["nevra"] = nevra
    return nevra, info

def getFetchURL(channelName, fetchName):
    return "/SAT/$RHN/" + channelName + "/getPackage/" + fetchName;

def _storeRPM(rpmName, response, verbose=False, dirPath="./packages"):
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
            print "Estimated bandwidth: %s KB/sec" % (len(data)/((endTime-startTime)*1000))
    file.close()
    return bytesRead, md5Hash.hexdigest()


def fetchRPMs(systemId, baseURL, channelName, pkgInfo):
    """
    Will return a tuple (fetched, errors)
     fetched is a list of fetched packages
     errors is a list of packages which had errors while being fetched
    """
    fetched = []
    errors = []
    authMap = login(baseURL, systemId)
    r = urlparse.urlsplit(baseURL)
    netloc = r.netloc
    conn = httplib.HTTPConnection(netloc)
    for nevra in pkgInfo:
        pkg = pkgInfo[nevra]
        # Copy over headers from authMap
        # Form GET request
        fetchName = pkg["fetch_name"]
        fetchURL = getFetchURL(channelName, fetchName)
        print "Will fetch RPM for %s, from: %s" % (nevra, fetchURL)
        conn.request("GET", fetchURL, headers=authMap)
        resp = conn.getresponse()
        size, md5sum = _storeRPM(nevra, resp)
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


def login(baseURL, systemId):
    client = createLoginClient(baseURL + "/SAT")
    authMap = client.authentication.login(systemId)
    return authMap

if __name__ == "__main__":
    baseUrl = "http://satellite.rhn.redhat.com"
    channelName = "rhel-i386-server-vt-5"
    systemId = getSystemId("./systemid")
    client = createClient(baseUrl + "/SAT-DUMP")
    packages = getChannelPackages(client, systemId, channelName)
    #print "Available packages = ", packages
    pkgInfo = getShortPackageInfo(client, systemId, packages)
    #print "PackageInfo = ", pkgInfo
    fetched, errors = fetchRPMs(systemId, baseUrl, channelName, pkgInfo)


