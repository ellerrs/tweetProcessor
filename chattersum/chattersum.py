#!/usr/bin/env python
# -*- coding: utf-8 -*-

from cleanr import stop as cleanrStop
import getopt
import logging
from ngramr import stop as ngramrStop
import os
import signal
from streamr import stop as streamrStop
import sys
import time

def usage():
    print "usage: ./chattersum.py [option] [arg] ..."
    print "Options and arguments (and corresponding environment variables):"
    print "  available services are: ngramr, streamr, cleanr"
    print ""
    print "-a, --startAll         : start the streamr and ngramr services"
    print "-A, --stopAll          : stop the streamr and ngramr services"
    print "-h, --help             : show this"
    print "-r, --restart service  : restart the selected service. example: --restart ngramr"
    print "-s, --start service    : start the requested service. example: --start streamr"
    print "-S, --stop  service    : stop the requested service. example: --stop streamr"
    print "-v, --verbose          : crank verbosity to maximum. (this if funny if you've played Zork)"
    

def getHour(hours_ago):
    timedif     = datetime.datetime.now() - datetime.timedelta(hours=hours_ago)
    return timedif.strftime('%Y%m%d%H')

def streamrStart():
    from streamr import start
    pid = os.fork()
    if (pid == 0):          # The first child.
        os.chdir("/")
        os.setsid()
        os.umask(0)
        pid2 = os.fork()
        if (pid2 == 0):     # Second child
            start()
        else:
            sys.exit()      #First child
#    else:                   # Parent Code
#        sys.exit()          # Parent exists


def ngramrStart():
    from ngramr import start
    pid = os.fork()
    if (pid == 0):          # The first child.
        os.chdir("/")
        os.setsid()
        os.umask(0)
        pid2 = os.fork()
        if (pid2 == 0):     # Second child
            start()
        else:
            sys.exit()      #First child
 #   else:                   # Parent Code
 #       sys.exit()          # Parent exists

def cleanrStart():
    from cleanr import start
    pid = os.fork()
    if (pid == 0):
        os.chdir("/")
        os.setsid()
        os.umask(0)
        pid2 = os.fork()
        if (pid2 == 0):
            start()
        else:
            sys.exit()


def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "aAhr:s:S:v", ["startAll", "stopAll", "help", "restart=", "start=", "stop=", "verbose"])  # :v to add arg
    except getopt.GetoptError as err:
        print str(err) 
        usage()
        sys.exit(2)

    output = None
    verbose = False
    
    for o, a in opts:

        logging.info('chattersum: option %s selected' % o)
        
        if o == "-v":
            verbose = True

        elif o in ("-a", "--startAll"):
            streamrStart()
            time.sleep(2)
            ngramrStart()
            time.sleep(2)
            cleanrStart()
            sys.exit()

        elif o in ("-A", "--stopAll"):
            streamrStop()
            time.sleep(2)
            ngramrStop()
            time.sleep(2)
            cleanrStop()
        
        elif o in ("-h", "--help"):
            usage()
            sys.exit()
        
        elif o in ("-r", "--restart"):
            if a == "streamr":
                streamrStop()
                time.sleep(5)
                streamrStart()
                sys.exit()

            elif a == "ngramr":
                mgramrStop()
                time.sleep(5)
                ngramrStart()
                sys.exit()

            else:
                usage()
                sys.exit()

        elif o in ("-s", "--start"):
            if a == "streamr":
                streamrStart()
                sys.exit()

            elif a == "ngramr":
                ngramrStart()
                sys.exit()

            elif a == "cleanr":
                cleanrStart()
                sys.exit()

            else:
                usage()
                sys.exit()

        
        elif o in ("-S", "--stop"):
            if a == "streamr":
                streamrStop()
            
            elif a == 'ngramr':
                ngramrStop()

            elif a == "cleanr":
                cleanrStop()
        
        else:
            assert False, "unhandled option"

if __name__ == "__main__":

    logging.basicConfig(filename='chattersum.log', level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    main()
