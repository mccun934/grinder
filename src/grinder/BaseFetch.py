#!/usr/bin/env python
#
# Copyright (c) 2010 Red Hat, Inc.
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
import logging
import traceback
try:
    import hashlib as md5
except:
    import md5

from RHNComm import RHNComm

LOG = logging.getLogger("BaseFetch")

class BaseFetch(object):
    STATUS_NOOP = 'noop'
    STATUS_DOWNLOADED = 'downloaded'
    STATUS_SIZE_MISSMATCH = 'size_missmatch'
    STATUS_MD5_MISSMATCH = 'md5_missmatch'
    STATUS_ERROR = 'error'
    
    def __init__(self, systemId, baseURL):
        self.authMap = None
        self.systemId = systemId
        self.baseURL = baseURL
        self.rhnComm = RHNComm(baseURL, self.systemId)

    def login(self, refresh=False):
        return self.rhnComm.login(refresh)

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

    def __storeFile(self, fileName, response, size, md5sum, dirPath, verbose=False):
        """
        Returns True if downloaded file matched expected size and md5sum.
         False is there was an error, or downloaded data didn't match expected values
        """
        # For case of kickstarts fileName may contain subdirectories
        filePath = os.path.join(dirPath, fileName)
        tempDirPath = os.path.dirname(filePath)
        if not os.path.isdir(tempDirPath):
            LOG.info("Creating directory: %s" % tempDirPath)
            try:
                os.makedirs(tempDirPath)
            except OSError, e:
                # Another thread may have created the dir since we checked,
                # if that's the case we'll see errno=17, so ignore that exception
                if e.errno != 17:
                    tb_info = traceback.format_exc()
                    LOG.debug("%s" % (tb_info))
                    LOG.critical(e)
                    raise e
        if os.path.exists(filePath) and self.verifyFile(filePath, size, md5sum):
            LOG.debug("%s exists with correct size and md5sum, no need to fetch." % (filePath))
            return BaseFetch.STATUS_NOOP

        toRead = 64 * 1024
        bytesRead = 0
        md5Hash = md5.md5()
        file = open(filePath, "wb")
        while 1:
            startTime = time.time()
            try:
                data = response.read(toRead)
            except Exception, e:
                tb_info = traceback.format_exc()
                LOG.debug("%s" % (tb_info))
                LOG.warn("Caught exception <%s> in BaseFetch::__storeFile(%s)" % 
                    (e, fileName))
                file.close()
                return BaseFetch.STATUS_ERROR
            endTime = time.time()
            if not data:
                break
            file.write(data)
            md5Hash.update(data)
            bytesRead += len(data)
            if verbose:
                LOG.debug("%s Estimated bandwidth: %s KB/sec" \
                        % (fileName, len(data)/((endTime-startTime)*1000)))

        file.close()
        calcMd5sum = md5Hash.hexdigest()
        if bytesRead != int(size):
            LOG.error("%s size mismatch, read: %s bytes, was expecting %s bytes" \
                % (fileName, bytesRead, size))
            os.remove(filePath)
            return BaseFetch.STATUS_SIZE_MISSMATCH
        elif calcMd5sum != md5sum:
            LOG.error("%s md5sum mismatch, read md5sum of: %s expected md5sum of %s" \
                %(fileName, calcMd5sum, md5sum))
            os.remove(filePath)
            return BaseFetch.STATUS_MD5_MISSMATCH
        return BaseFetch.STATUS_DOWNLOADED

    def fetch(self, fileName, fetchURL, itemSize, md5sum, savePath, retryTimes=2):
        """
        Input:
            itemInfo = dict with keys: 'file_name', 'fetch_url', 'item_size', 'md5sum'
            retryTimes = how many times to retry fetch if an error occurs
        Will return a true/false if item was fetched successfully 
        """
        try:
            authMap = self.login()
            r = urlparse.urlsplit(self.baseURL)
            if hasattr(r, 'netloc'):
                netloc = r.netloc
            else:
                #support older python
                netloc = r[1]
            conn = httplib.HTTPConnection(netloc)
            conn.request("GET", fetchURL, headers=authMap)
            LOG.info("Fetching %s bytes: %s from %s" % (itemSize, fileName, fetchURL))
            resp = conn.getresponse()
            if resp.status == 401:
                LOG.warn("Got a response of %s:%s, Will refresh authentication credentials and retry" \
                    % (resp.status, resp.reason))
                authMap = self.login(refresh=True)
                conn.request("GET", fetchURL, headers=authMap)
                resp = conn.getresponse()
            if resp.status != 200:
                conn.close()
                LOG.critical("ERROR: Response = %s fetching %s.  Our Authentication Info is : %s" \
                    % (resp.status, fetchURL, authMap))
                if retryTimes > 0:
                    retryTimes -= 1
                    LOG.warn("Retrying fetch of: %s with %s retry attempts left." % (fileName, retryTimes))
                    return self.fetch(fileName, fetchURL, itemSize, md5sum, savePath, retryTimes)
                return BaseFetch.STATUS_ERROR

            size = int(itemSize) #size is likely to be in string format after XML parsing
            status = self.__storeFile(fileName, resp, size, md5sum, dirPath=savePath)
            conn.close()
            if status in [BaseFetch.STATUS_ERROR, BaseFetch.STATUS_SIZE_MISSMATCH, 
                BaseFetch.STATUS_MD5_MISSMATCH] and retryTimes > 0:
                #
                # Incase of a network glitch or issue with RHN, retry the rpm fetch
                #
                retryTimes -= 1
                LOG.warn("Retrying fetch of: %s with %s retry attempts left." % (fileName, retryTimes))
                return self.fetch(fileName, fetchURL, itemSize, md5sum, savePath, retryTimes)
            return status
        except Exception, e:
            tb_info = traceback.format_exc()
            LOG.debug("%s" % (tb_info))
            LOG.warn("Caught exception<%s> in fetch(%s, %s)" % (e, fileName, fetchURL))
            if retryTimes > 0:
                retryTimes -= 1
                LOG.warn("Retrying fetch of: %s with %s retry attempts left." % (fileName, retryTimes))
                return self.fetch(fileName, fetchURL, itemSize, md5sum, savePath, retryTimes)
            return BaseFetch.STATUS_ERROR

if __name__ == "__main__":
    systemId = open("/etc/sysconfig/rhn/systemid").read()
    baseURL = "http://satellite.rhn.redhat.com"
    bf = BaseFetch(systemId, baseURL)
    itemInfo = {}
    itemInfo['file_name'] = "Virtualization-es-ES-5.2-9.noarch.rpm"
    fetchName = "Virtualization-es-ES-5.2-9:.noarch.rpm"
    channelLabel = "rhel-i386-server-vt-5"
    fetchURL = "/SAT/$RHN/" + channelLabel + "/getPackage/" + fetchName;
    itemSize = "1731195"
    md5sum = "91b0f20aeeda88ddae4959797003a173" 
    savePath = "./test123"
    
    status = bf.fetch(fetchName, fetchURL, itemSize, md5sum, savePath, retryTimes=2)
    print "Base fetch status is %s" % (status)
