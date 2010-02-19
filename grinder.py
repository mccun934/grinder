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
import xmlrpclib
from optparse import Option, OptionParser

def processCommandline():
    "process the commandline, setting the OPTIONS object"
    optionsTable = [
        Option('-u', '--username',            action='store',
            help=' RHN User Account'),
        Option('-p', '--password',        action='store',
            help='RHQ Passowrd'),
        Option('-v', '--verbose',         action='store_true',
            help='verbose output'),
    ]
    optionParser = OptionParser(option_list=optionsTable, usage="%prog [OPTION] [<package>]")
    global OPTIONS, files
    OPTIONS, files = optionParser.parse_args()



class RhnTransport(xmlrpclib.SafeTransport):
    __cert_file = '/usr/share/rhn/RHNS-CA-CERT'
    __key_file  = '/etc/webapp-keyring.gpg'
    
    def make_connection(self,host):
        host_with_cert = (host, 
                      { 'key_file'  :  self.__key_file,
                      'cert_file' :  self.__cert_file })
        return xmlrpclib.SafeTransport.make_connection(self,host_with_cert)     
        
    def send_host(self, connection, host):
        print "Adding extra header"
        connection.putheader("X-RHN-Satellite-XML-Dump-Version", "3.3")
        xmlrpclib.SafeTransport.send_host(self, connection, host)


class ContentSyncer ():
    def __init__(self, username, password):
        self.cert = open('/etc/sysconfig/rhn/entitlement-cert.xml', 'r').read()
        self.systemid = open('/etc/sysconfig/rhn/systemid', 'r').read()
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
        SATELLITE_URL = "http://satellite.rhn.redhat.com/SAT-DUMP"
        rhn = RhnTransport()    
        
        client = xmlrpclib.ServerProxy(SATELLITE_URL, verbose=0, transport=rhn)
        chans = ['rhel-i386-server-vt-5']
        client.dump.channel_families(self.systemid)
        #for channel in channels:
        #    print channel
        # result = (JAXBElement) dumpHandler.execute("dump.channels", params);
    
    

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

#try:
    ## NOTE: importing
    #from rhqclient import RhqCli
#except KeyboardInterrupt:
    #systemExit(0, "\nUser interrupted process.")
#except ImportError, e:
    #systemExit(2, "Unable to find Rhq client module.\n"
                  #"Error: %s" % e)

def main():
    # execute
    try:
        print "Main executed"
        processCommandline()
        username = OPTIONS.username
        password = OPTIONS.password
        cs = ContentSyncer(username, password)
        cs.activate()
        cs.sync()
        
    except KeyboardInterrupt:
        systemExit(0, "\nUser interrupted process.")

    return 0


if __name__ == '__main__':
    try:
        sys.exit(abs(main() or 0))
    except KeyboardInterrupt:
        systemExit(0, "\nUser interrupted process.")
