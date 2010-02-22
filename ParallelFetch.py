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
import time
from threading import Thread
from Queue import Queue

from PackageFetch import PackageFetch

class ParallelFetch(object):
    def __init__(self, systemId, baseURL, channelName, numThreads=3):
        self.systemId = systemId
        self.baseURL = baseURL
        self.channelName = channelName
        self.numThreads = numThreads
        self.toSyncQ = Queue()
        self.syncCompleteQ = Queue()
        self.syncErrorQ = Queue()
        self.threads = []
        for i in range(self.numThreads):
            wt = WorkerThread(self.systemId, self.baseURL, self.channelName, self.toSyncQ, 
                    self.syncCompleteQ, self.syncErrorQ)
            self.threads.append(wt)

    def addPkg(self, pkg):
        self.toSyncQ.put(pkg)

    def addPkgList(self, pkgs):
        for p in pkgs:
            self.toSyncQ.put(p)

    def start(self):
        for t in self.threads:
            t.start()

    def waitForFinish(self):
        """
        Will wait for all worker threads to finish
        Returns (successList, errorList)
         successList is a list of all packages successfully synced
         errorList is a list of all packages which couldn't be synced
        """
        for t in self.threads:
            t.join()
        print "All threads have finished."
        successList = []
        while not self.syncCompleteQ.empty():
            p = self.syncCompleteQ.get_nowait()
            successList.append(p)
        errorList = []
        while not self.syncErrorQ.empty():
            p = self.syncErrorQ.get_nowait()
            errorList.append(p)
        print "ParallelFetch: %s package successfully fetched, %s packages had errors" % (len(successList), len(errorList))
        return (successList, errorList)


class WorkerThread(PackageFetch, Thread):

    def __init__(self, systemId, baseURL, channelName, toSyncQ, syncCompleteQ, syncErrorQ):
        Thread.__init__(self)
        PackageFetch.__init__(self, systemId, baseURL, channelName)
        self.toSyncQ = toSyncQ
        self.syncCompleteQ = syncCompleteQ
        self.syncErrorQ = syncErrorQ
        self.authMap = None
        

    def run(self):
        print "Run has started"
        while not self.toSyncQ.empty():
            print "%s packages left on Queue" % (self.toSyncQ.qsize())
            pkg = self.toSyncQ.get()
            if self.fetchRPM(pkg):
                self.syncCompleteQ.put(pkg)
            else:
                self.syncErrorQ.put(pkg)


if __name__ == "__main__":
    #For simple testing, we want the fetch function to just return True
    def simpleFetchMethod(self, x):
        if x % 3 == 0:
            time.sleep(1)
        return True
    setattr(WorkerThread, "fetchRPM", simpleFetchMethod)
    
    pf = ParallelFetch("", "", 3)
    pkgs = range(1, 20)
    pf.addPkgList(pkgs)
    pf.start()
    successList, errorList = pf.waitForFinish()
    for p in successList:
        print "Success: ", p
    for p in errorList:
        print "Error: ", p
    

