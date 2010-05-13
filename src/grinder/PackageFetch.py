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
import logging

from BaseFetch import BaseFetch
LOG = logging.getLogger("PackageFetch")


class PackageFetch(BaseFetch):
    
    def __init__(self, systemId, baseURL, channelLabel, savePath):
        BaseFetch.__init__(self, systemId, baseURL)
        self.channelLabel = channelLabel
        self.savePath = savePath
    
    def getFetchURL(self, channelLabel, fetchName):
        return "/SAT/$RHN/" + channelLabel + "/getPackage/" + fetchName;

    def fetchItem(self, itemInfo):
        fileName = itemInfo['filename']
        fetchName = itemInfo['fetch_name']
        itemSize = itemInfo['package_size']
        md5sum = itemInfo['md5sum']
        fetchURL = self.getFetchURL(self.channelLabel, fetchName)
        return self.fetch(fileName, fetchURL, itemSize, md5sum, self.savePath)


if __name__ == "__main__":
    systemId = open("/etc/sysconfig/rhn/systemid").read()
    baseURL = "http://satellite.rhn.redhat.com"
    channelLabel = "rhel-i386-server-vt-5"
    savePath = "./test123"
    pf = PackageFetch(systemId, baseURL, channelLabel, savePath)
    pkg = {}
    pkg['nevra'] = "Virtualization-es-ES-5.2-9.noarch.rpm"
    pkg['fetch_name'] = "Virtualization-es-ES-5.2-9:.noarch.rpm"
    pkg['package_size'] = "1731195"
    pkg['md5sum'] = "91b0f20aeeda88ddae4959797003a173" 
    status = pf.fetchItem(pkg)
    print "Package fetch status is %s" % (status)

