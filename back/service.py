#!/usr/bin/env python
# -*- coding: utf-8 -*-

import getopt
import logging
import logging.config
import os
import signal
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
    print "-H, --health           : healthcheck services"
    print "-r, --restart service  : restart the selected service. example: --restart ngramr"
    print "-s, --start service    : start the requested service. example: --start streamr"
    print "-S, --stop  service    : stop the requested service. example: --stop streamr"
    print "-v, --verbose          : crank verbosity to maximum. (this if funny if you've played Zork)"

def healthCheck():        

#    from ngramr import health as nh
#    if(nh()):
#        logger.info("ngramr UP")
#    else:
#        logger.info("ngramr DOWN - restarting")
#        ngramrStart()

    from cleanr import health as ch
    if(ch()):
        logger.info("cleanr UP")
    else:
        logger.info("cleanr DOWN - restarting")
        cleanrStart()

    from streamr import health as sh
    if(sh()):
        logger.info("streamr UP")
    else:
        logger.info("streamr DOWN - restarting")
        streamrStart()
 
    sys.exit()


def getHour(hours_ago):
    timedif     = datetime.datetime.now() - datetime.timedelta(hours=hours_ago)
    return timedif.strftime('%Y%m%d%H')

def streamrStop():
    from streamr import stop
    stop()

def ngramrStop():
    from ngramr import stop
    stop()

def cleanrStop():
    from cleanr import stop
    stop()

def streamrStart():
    from streamr import start
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

def ngramrStart():
    from ngramr import start
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
        opts, args = getopt.getopt(sys.argv[1:], "aAhHr:s:S:viz", ["startAll", "stopAll", "help", "health", "restart=", "start=", "stop=", "verbose", "zork"])  # :v to add arg
    except getopt.GetoptError as err:
        print str(err) 
        usage()
        sys.exit(2)

    output = None
    verbose = False
    
    for o, a in opts:

        logger.info('option %s %s selected' % (o, a))
        
        if o == "-v":
            verbose = True

        elif o in ("-a", "--startAll"):
            streamrStart()
            ngramrStart()
            cleanrStart()
            sys.exit()

        elif o in ("-A", "--stopAll"):
            streamrStop()
            ngramrStop()
            cleanrStop()
        
        elif o in ("-h", "--help"):
            usage()
            sys.exit()
       
        elif o in ("-H", "--health"):
            healthCheck()
 
        elif o in ("-r", "--restart"):
            if a == "streamr":
                streamrStop()
                streamrStart()
                sys.exit()

            elif a == "ngramr":
                ngramrStop()
                ngramrStart()
                sys.exit()

            elif a == "cleanr":
                cleanrStop()
                cleanrStart()
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
    
    logging.config.fileConfig('logging.conf')
    logger = logging.getLogger('chattersum')
    main()
