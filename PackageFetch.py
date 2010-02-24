#!/usr/bin/env python
#
# Copyright (c) 2010 Red Hat, Inc.
#
# Authors: John Matthews
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
import xmlrpclib
import httplib
import urlparse
import time
try:
    import hashlib as md5
except:
    import md5

import logging

LOG = logging.getLogger("PackageFetch")


class PackageFetch(object):
    def __init__(self, systemId, baseURL, channelLabel):
        self.authMap = None
        self.systemId = systemId
        self.baseURL = baseURL
        self.channelLabel = channelLabel

    #
    # TODO:  Consider making this a static method so all threads/instances will
    # share same cached value.
    #
    def login(self, refresh=False):
        """
        Input: refresh  default value is False
          if refresh is True we will force a login call and refresh the 
          cached authentication map
        Output: dict of authentication credentials to be placed in header 
          for future package fetch 'GET' calls
        Note:
          The authentication data returned is cached, it is only updated on the
          first call, or when "refresh=True" is passed.
        Background:
            If we make too many login calls to RHN we could make the referring
            systemid be flagged as abusive.  Current metrics allow ~100 logins a day
        """
        if self.authMap and not refresh:
            return self.authMap
        client = xmlrpclib.Server(self.baseURL+"/SAT", verbose=0)
        self.authMap = client.authentication.login(self.systemId)
        self.authMap["X-RHN-Satellite-XML-Dump-Version"] = "3.5"
        return self.authMap

    def getFetchURL(self, channelLabel, fetchName):
        return "/SAT/$RHN/" + channelLabel + "/getPackage/" + fetchName;


    def verifyFile(self, filePath, size, md5sum):
        statinfo = os.stat(filePath)
        if statinfo.st_size == size:
            md5Hash = md5.md5()
            file  = open(filePath, "rb")
            while 1:
                data = file.read(64*1024)
                if not data:
                    break
                md5Hash.update(data)
            file.close()
            if md5Hash.hexdigest() == md5sum:
                return True
        return False

    def storeRPM(self, rpmName, response, size, md5sum, dirPath="./packages", verbose=False):
        """
        Returns True if downloaded file matched expected size and md5sum.
         False is there was an error, or downloaded data didn't match expected values
        """
        if not os.path.isdir(dirPath):
            LOG.info("Creating directory: %s" % dirPath)
            os.mkdir(dirPath)
        filePath = os.path.join(dirPath, rpmName)
        if os.path.exists(filePath) and self.verifyFile(filePath, size, md5sum):
            LOG.debug("%s exists with correct size and md5sum, no need to fetch." % (filePath))
            return True

        toRead = 64 * 1024
        bytesRead = 0
        md5Hash = md5.md5()
        file = open(filePath, "wb")
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
                LOG.debug("%s Estimated bandwidth: %s KB/sec" \
                        % (rpmName, len(data)/((endTime-startTime)*1000)))
        file.close()
        calcMd5sum = md5Hash.hexdigest()
        if bytesRead != int(size):
            LOG.error("%s size mismatch, read: %s bytes, was expecting %s bytes" \
                % (rpmName, bytesRead, size))
            os.remove(filePath)
            return False
        elif calcMd5sum != md5sum:
            LOG.error("%s md5sum mismatch, read md5sum of: %s expected md5sum of %s" \
                %(rpmName, calcMd5sum, md5sum))
            os.remove(filePath)
            return False
        return True

    def fetchRPM(self, pkg, dirPath=None, retryTimes=2):
        """
        Input:
            pkg = dict containing 'fetch_name', 'package_size', 'md5'
        Will return a true/false if package was fetched successfully 
        """
        authMap = self.login()
        r = urlparse.urlsplit(self.baseURL)
        if hasattr(r, 'netloc'):
            netloc = r.netloc
        else:
            netloc = r[1]
        conn = httplib.HTTPConnection(netloc)

        nevra = pkg["nevra"]
        fetchName = pkg["fetch_name"]
        fetchURL = self.getFetchURL(self.channelLabel, fetchName)
        LOG.info("Fetching: %s %s" % (self.channelLabel, nevra))

        conn.request("GET", fetchURL, headers=authMap)
        resp = conn.getresponse()
        if resp.status == 401:
            LOG.warn("Got a response of %s:%s, Will refresh authentication credentials and retry" \
                % (resp.status, resp.reason))
            authMap = self.login(refresh=True)
            conn.request("GET", fetchURL, headers=authMap)
            resp = conn.getresponse()
        if resp.status != 200:
            LOG.critical("ERROR: fetching %s.  Our Authentication Info is : %s" \
                % (fetchURL, authMap))
            conn.close()
            return False

        size = int(pkg['package_size'])
        md5sum = pkg['md5sum']
        d = dirPath
        if d == None:
            d = self.channelLabel
        #
        # Incase of a network glitch or issue with RHN, retry the rpm fetch
        #
        status = self.storeRPM(nevra, resp, size, md5sum, dirPath=d)
        conn.close()
        if not status and retryTimes > 0:
            retryTimes -= 1
            LOG.warn("Retrying fetch of: %s with %s retry attempts left." % (nevra, retryTimes))
            return self.fetchRPM(pkg, dirPath, retryTimes)
        return status

if __name__ == "__main__":
    systemId = open("/etc/sysconfig/rhn/systemid").read()
    baseURL = "http://satellite.rhn.redhat.com"
    channelLabel = "rhel-i386-server-vt-5"
    pf = PackageFetch(systemId, baseURL, channelLabel)
    pkg = {}
    pkg['nevra'] = "Virtualization-es-ES-5.2-9.noarch.rpm"
    pkg['fetch_name'] = "Virtualization-es-ES-5.2-9:.noarch.rpm"
    pkg['package_size'] = "1731195"
    pkg['md5sum'] = "91b0f20aeeda88ddae4959797003a173" 
    if pf.fetchRPM(pkg, dirPath="./test123"):
        print "Package fetch was successful"
    else:
        print "Error with package fetch"

