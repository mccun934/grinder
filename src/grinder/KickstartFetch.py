#!/usr/bin/python
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

import sys
import httplib
import urlparse
import time

try:
    import hashlib as md5
except:
    import md5

from SatDumpClient import SatDumpClient
from RHNComm import RHNComm


def login(baseURL, systemId):
    rhnComm = RHNComm(baseURL, systemId)
    return rhnComm.login()

def download(baseURL, authMap, channelLabel, ksTreeLabel, ksFilePath):
    r = urlparse.urlsplit(baseURL)
    if hasattr(r, 'netloc'):
        netloc = r.netloc
    else:
        netloc = r[1]

    fetchURL = getFetchURL(channelLabel, ksTreeLabel, ksFilePath)
    conn = httplib.HTTPConnection(netloc)
    conn.request("GET", fetchURL, headers=authMap)
    resp = conn.getresponse()

    bytesRead = 0
    md5Hash = md5.md5()
    while 1:
        data = resp.read(64 * 1024)
        if not data:
            break
        md5Hash.update(data)
        bytesRead += len(data)
    calcMd5sum = md5Hash.hexdigest()
    return bytesRead, calcMd5sum


def getFetchURL(channelLabel, ksTreeLabel, ksFilePath):
    return "/SAT/$RHN/" + channelLabel + "/getKickstartFile/" + ksTreeLabel + "/" + ksFilePath;

def testKSFileDownload(baseURL, systemId, metadata):
    startTime = time.time()
    loginAuth = login(baseURL, systemId)
    for ksLabel in metadata:
        print "KS files should be written to %s" % (metadata[ksLabel]["base-path"])
        channelLabel = metadata[ksLabel]["channel"]
        for ksFile in metadata[ksLabel]["files"]:
            url = baseURL + getFetchURL(channelLabel, ksLabel, ksFile['relative-path'])
            print "Download %s" % (url)
            bytesRead, calcMd5sum = download(baseURL, loginAuth, channelLabel, ksLabel, ksFile['relative-path'])
            print "Expected file size: %s, md5sum: %s" % (ksFile['file-size'], ksFile['md5sum'])
            if bytesRead != int(ksFile['file-size']):
                print "*****ERROR*****, read %s bytes instead of %s" % (bytesRead, ksFile['file-size'])
            if calcMd5sum != ksFile['md5sum']:
                print "*****ERROR*****, read md5sum of %s instead of %s" % (calcMd5sum, ksFile['md5sum'])
    endTime = time.time()
    print "It took %s seconds to fetch the files" % (endTime - startTime)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print "Usage: %s CHANNEL-LABEL1 CHANNEL_LABEL2, etc" % (sys.argv[0])
        sys.exit(1)

    url = "http://satellite.rhn.redhat.com"
    satDump = SatDumpClient(url)
    channelLabels = sys.argv[1:]
    systemId = open("/etc/sysconfig/rhn/systemid").read()
    print "Fetch kickstart labels for '%s'" % (channelLabels)
    kickstartTrees = satDump.getKickstartLabels(systemId, channelLabels)
    print "Kickstart labels = ", kickstartTrees
    startTime = time.time()
    for channelLabel in kickstartTrees:
        ksLabels = kickstartTrees[channelLabel]
        for ksLbl in ksLabels:
            metadata = satDump.getKickstartTreeMetadata(systemId, [ksLbl])
            #print "%s Kickstart metadata for '%s' is :\n %s" % (channelLabel, ksLbl, metadata)
            testKSFileDownload(url, systemId, metadata)
    endTime = time.time()
    print "time = %s seconds" % (endTime - startTime)


