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
import sys
import time
import hashlib
import logging
LOG = logging.getLogger("GrinderUtils")

def getFileChecksum(hashtype, filename=None, fd=None, file=None, buffer_size=None):
    """ Compute a file's checksum
    """

    if buffer_size is None:
        buffer_size = 65536

    if filename is None and fd is None and file is None:
        raise ValueError("no file specified")
    if file:
        f = file
    elif fd is not None:
        f = os.fdopen(os.dup(fd), "r")
    else:
        f = open(filename, "r")
    # Rewind it
    f.seek(0, 0)
    m = hashlib.new(hashtype)
    while 1:
        buffer = f.read(buffer_size)
        if not buffer:
            break
        m.update(buffer)

    # cleanup time
    if file is not None:
        file.seek(0, 0)
    else:
        f.close()
    return m.hexdigest()

def verifyChecksum(filePath, hashtype, checksum):
    if getFileChecksum(hashtype, filename=filePath) == checksum:
        return True
    return False
    
def validateDownload(filePath, size, hashtype, checksum, verbose=False):
    statinfo = os.stat(filePath)
    fileName = os.path.basename(filePath)
    calchecksum = getFileChecksum(hashtype, filename=filePath)
    # validate fetched data
    if statinfo.st_size != int(size):
        LOG.error("%s size mismatch, read: %s bytes, was expecting %s bytes" \
            % (fileName, statinfo.st_size, size))
        os.remove(filePath)
        return False
    elif calchecksum != checksum:
        LOG.error("%s md5sum mismatch, read md5sum of: %s expected md5sum of %s" \
            %(fileName, calchecksum, checksum))
        os.remove(filePath)
        return False
    LOG.debug("Package [%s] is valid with checksum [%s] and size [%s]" % (fileName, checksum, size))
    return True
    
