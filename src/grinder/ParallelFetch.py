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
import logging
import threading
from threading import Thread
import Queue

from PackageFetch import PackageFetch

LOG = logging.getLogger("ParallelFetch")

class ParallelFetch(object):
    def __init__(self, systemId, baseURL, channelName, numThreads=3):
        self.systemId = systemId
        self.baseURL = baseURL
        self.channelName = channelName
        self.numThreads = numThreads
        self.toSyncQ = Queue.Queue()
        self.syncCompleteQ = Queue.Queue()
        self.syncErrorQ = Queue.Queue()
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

    def stop(self):
        for t in self.threads:
            t.stop()
        
    def _running(self):
        working = 0
        for t in self.threads:
            if (t.isAlive()):
                working += 1
        return (working > 0)

    def _waitForThreads(self):
        while (self._running()):
            LOG.debug("Wait 1.  check again")
            time.sleep(0.5)
        

    def waitForFinish(self):
        """
        Will wait for all worker threads to finish
        Returns (successList, errorList)
         successList is a list of all packages successfully synced
         errorList is a list of all packages which couldn't be synced
        """
        self._waitForThreads()
            
        LOG.info("All threads have finished.")
        successList = []
        while not self.syncCompleteQ.empty():
            p = self.syncCompleteQ.get_nowait()
            successList.append(p)
        errorList = []
        while not self.syncErrorQ.empty():
            p = self.syncErrorQ.get_nowait()
            errorList.append(p)
        LOG.info("ParallelFetch: %s package successfully fetched, %s packages had errors" % (len(successList), len(errorList)))
        return (successList, errorList)


class WorkerThread(PackageFetch, Thread):

    def __init__(self, systemId, baseURL, channelName, toSyncQ, syncCompleteQ, syncErrorQ):
        Thread.__init__(self)
        PackageFetch.__init__(self, systemId, baseURL, channelName)
        self.toSyncQ = toSyncQ
        self.syncCompleteQ = syncCompleteQ
        self.syncErrorQ = syncErrorQ
        self.authMap = None
        self._stop = threading.Event()
    
    def stop(self):
        self._stop.set()

    def run(self):
        LOG.debug("Run has started")
        while not self.toSyncQ.empty() and not self._stop.isSet():
            LOG.info("%s packages left on Queue" % (self.toSyncQ.qsize()))
            try:
                pkg = self.toSyncQ.get_nowait()
                if self.fetchRPM(pkg):
                    self.syncCompleteQ.put(pkg)
                else:
                    self.syncErrorQ.put(pkg)
            except Queue.Empty:
                LOG.debug("Queue is empty, thread will end")
        LOG.debug("Thread ending")


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
    

