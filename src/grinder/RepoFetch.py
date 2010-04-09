#!/usr/bin/env python
#
# Copyright (c) 2010 Red Hat, Inc.
#
# Module to fetch content from yum repos
#
# Authors: Pradeep Kilambi <pkilambi@redhat.com>
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
import yum
import time
import urllib
import urlparse
import logging

from yum import config
from ParallelFetch import ParallelFetch

LOG = logging.getLogger("RepoFetch")

class RepoFetch(object):
    """
     Module to fetch content from remote yum repos
    """
    def __init__(self, repo_label, repourl, mirrorlist=None, download_dir='./'):
        self.repo_label = repo_label
        self.repourl = repourl
        self.mirrorlist = mirrorlist
        self.local_dir = download_dir
        self.repo_dir = os.path.join(self.local_dir, self.repo_label)

    def setupRepo(self):
        self.repo = yum.yumRepo.YumRepository(self.repo_label)
        self.repo.basecachedir = self.local_dir
        self.repo.cache = 0
        self.repo.metadata_expire = 0
        if self.mirrorlist:
            self.repo.mirrorlist = self.repourl
        else:
            self.repo.baseurl = [self.repourl]
        self.repo.baseurlSetup()
        self.repo.setup(False)

    def getPackageList(self, newest=False):
        self.setupRepo()

        sack = self.repo.getPackageSack()
        sack.populate(self.repo, 'metadata', None, 0)
        sack.populate(self.repo, 'filelists', None, 0)
        sack.populate(self.repo, 'otherdata', None, 0)
        if newest:
            download_list = sack.returnNewestByNameArch()
        else:
            download_list = sack.returnPackages()
        return download_list

    def fetchItem(self, downloadinfo):
        urllib.urlretrieve(downloadinfo['downloadurl'], \
                           downloadinfo['savepath'])

    def fetchAll(self):
        plist = self.getPackageList()
        total = len(plist)
        seed = 1
        for pkg in plist:
            print("Fetching [%d/%d] Packages - %s" % (seed, total, pkg))
            check = (self.validatePackage, (pkg ,1), {})
            self.repo.getPackage(pkg, checkfunc=check)
            seed += 1

    def getRepoData(self):
        self.repo.getPrimaryXML()
        self.repo.getFileListsXML()
        self.repo.getOtherXML()
        self.repo.getGroups()
        print("Fetching repo metadata for repo %s" % self.repo_label)

    def validatePackage(self, fo, pkg, fail):
        return pkg.verifyLocalPkg()

class YumRepoGrinder(object):
    """
      Driver module to initiate the repo fetching
    """
    def __init__(self, repo_label, repo_url, parallel, mirrors=None):
        self.repo_label = repo_label
        self.repo_url = repo_url
        self.mirrors = mirrors
        self.numThreads = int(parallel)

    def fetchYumRepo(self, basepath="./"):
        startTime = time.time()
        yumFetch = RepoFetch(self.repo_label, repourl=self.repo_url, \
                            mirrorlist=self.mirrors, download_dir=basepath)
        pkglist = yumFetch.getPackageList()
        LOG.info("%s packages have been marked to be fetched" % len(pkglist))
        downloadinfo = []
        for pkg in pkglist:
            info = {}
            info['downloadurl'] = urlparse.urljoin(yumFetch.repourl, \
                                                   pkg.relativepath, \
                                                   "packages")
            info['savepath'] = os.path.join(yumFetch.repo_dir, "packages", \
                                    pkg.__str__())
            downloadinfo.append(info)
        # first fetch the metadata
        yumFetch.getRepoData()
        # prepare for download
        fetchPkgs = ParallelFetch(yumFetch, self.numThreads)
        fetchPkgs.addItemList(downloadinfo)
        fetchPkgs.start()
        report = fetchPkgs.waitForFinish()
        endTime = time.time()
        LOG.info("Processed <%s> packages in [%d] seconds" % (len(pkglist), \
                  (startTime - endTime)))
        return report

if __name__ == "__main__":
    yfetch = YumRepoGrinder("centos-5", \
        "http://mirrors.kernel.org/centos/5/os/x86_64/", 20)
    yfetch.fetchYumRepo()
