#!/usr/bin/env python
#
# Copyright (c) 2010 Red Hat, Inc.
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
import os
import sys
import optparse
import signal
import logging
import GrinderLog
from optparse import OptionParser
from RepoFetch import YumRepoGrinder

LOG = logging.getLogger("GrinderCLI")

class CliDriver(object):
    """ Base class for all sub-commands. """
    def __init__(self, name="cli", usage=None, shortdesc=None,
            description=None):
        self.shortdesc = shortdesc
        if shortdesc is not None and description is None:
            description = shortdesc
        self.debug = 0
        self.parser = OptionParser(usage=usage, description=description)
        self._add_common_options()
        self.name = name
        self.killcount = 0
        #GrinderLog.setup(self.debug)

    def _add_common_options(self):
        """ Add options that apply to all sub-commands. """
        self.parser.add_option("--debug", dest="debug",
                default=0, help="debug level")

    def _do_command(self):
        """ implement this in sub classes"""
        pass

    def stop(self):
        pass

    def main(self):

        (self.options, self.args) = self.parser.parse_args()
        self.args = self.args[1:]
        # do the work, catch most common errors here:
        self._do_command()

class RHNDriver(CliDriver):
    pass

class RepoDriver(CliDriver):
    parallel = 5
    def __init__(self):
        usage = "usage: %prog yum [OPTIONS]"
        shortdesc = "Fetches content from a yum repo."
        desc = "yum"
        CliDriver.__init__(self, "yum", usage, shortdesc, desc)
        GrinderLog.setup(self.debug)

        self.parser.add_option("--label", dest="label",
                          help="Repo label")
        self.parser.add_option("--url", dest="url",
                          help="Repo URL to fetch the content bits.")
        self.parser.add_option("--parallel", dest="parallel",
                          help="Thread count to fetch the bits in parallel. Defaults to 5")
        self.parser.add_option("--dir", dest="dir",
                          help="Directory path to store the fetched content. Defaults to Current working Directory")

    def _validate_options(self):
        if not self.options.label:
            print("repo label is required. Try --help.")
            sys.exit(-1)

        if not self.options.url:
            print("No Url specific to fetch content. Try --help")
            sys.exit(-1)

        if self.options.parallel:
            self.parallel = self.options.parallel

    def _do_command(self):
        """
        Executes the command.
        """
        self._validate_options()
        self.yfetch = YumRepoGrinder(self.options.label, self.options.url, \
                                self.parallel)
        if self.options.dir:
            self.yfetch.fetchYumRepo(self.options.dir)
        else:
            self.yfetch.fetchYumRepo()

    def stop(self):
        self.yfetch.stop()

# this is similar to how rho does parsing
class CLI:
    def __init__(self):

        self.cli_commands = {}
        for clazz in [ RepoDriver, RHNDriver]:
            cmd = clazz()
            # ignore the base class
            if cmd.name != "cli":
                self.cli_commands[cmd.name] = cmd 


    def _add_command(self, cmd):
        self.cli_commands[cmd.name] = cmd

    def _usage(self):
        print "\nUsage: %s [options] MODULENAME --help\n" % os.path.basename(sys
.argv[0])
        print "Supported modules:\n"

        # want the output sorted
        items = self.cli_commands.items()
        items.sort()
        for (name, cmd) in items:
            print("\t%-14s %-25s" % (name, cmd.shortdesc))
        print("")

    def _find_best_match(self, args):
        """
        Returns the subcommand class that best matches the subcommand specified
        in the argument list. For example, if you have two commands that start
        with auth, 'auth show' and 'auth'. Passing in auth show will match
        'auth show' not auth. If there is no 'auth show', it tries to find
        'auth'.

        This function ignores the arguments which begin with --
        """
        possiblecmd = []
        for arg in args[1:]:
            if not arg.startswith("-"):
                possiblecmd.append(arg)

        if not possiblecmd:
            return None

        cmd = None
        key = " ".join(possiblecmd)
        if self.cli_commands.has_key(" ".join(possiblecmd)):
            cmd = self.cli_commands[key]

        i = -1
        while cmd == None:
            key = " ".join(possiblecmd[:i])
            if key is None or key == "":
                break

            if self.cli_commands.has_key(key):
                cmd = self.cli_commands[key]
            i -= 1

        return cmd

    def main(self):
        global cmd
        if len(sys.argv) < 2 or not self._find_best_match(sys.argv):
            self._usage()
            sys.exit(0)

        cmd = self._find_best_match(sys.argv)
        if not cmd:
            self._usage()
            sys.exit(0)
        cmd.main()

def handleKeyboardInterrupt(signalNumer, frame):
    if (cmd.killcount > 0):
        LOG.error("force quitting.")
        sys.exit()
    if (cmd.killcount == 0):
        cmd.killcount = 1
        msg = "SIGINT caught, will finish currently downloading" + \
              " packages and exit. Press CTRL+C again to force quit"
        LOG.error(msg)
        cmd.stop()

signal.signal(signal.SIGINT, handleKeyboardInterrupt)

def systemExit(code, msgs=None):
    "Exit with a code and optional message(s). Saved a few lines of code."

    if msgs:
        if type(msgs) not in [type([]), type(())]:
            msgs = (msgs, )
        for msg in msgs:
            sys.stderr.write(str(msg)+'\n')
    sys.exit(code)

if __name__ == "__main__":
    try:
        sys.exit(abs(CLI().main() or 0))
    except KeyboardInterrupt:
        systemExit(0, "\nUser interrupted process.")
    except Exception, e:
        systemExit(1, e)

