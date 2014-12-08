#!/usr/bin/env python
# -*- coding: utf-8 -*-

import getopt
import logging
import os
import signal
import sys


def usage():
    print "usage: ./chattersum.py [option] [arg] ..."
    print "Options and arguments (and corresponding environment variables):"
    print ""
    print "-d, --dump  hour     : create a tar.gz archive of the raw tweets from <hour>s ago. example: --dump 1"
    print "-h, --help           : show this"
    print "-s, --start service  : start the requested service. example: --start streamr"
    print "-S, --stop  service  : stop the requested service. example: --stop streamr"
    print "-v, --verbose        : crank verbosity to maximum. (this if funny if you've played Zork)"
    



def getHour(hours_ago):
    timedif     = datetime.datetime.now() - datetime.timedelta(hours=hours_ago)
    return timedif.strftime('%Y%m%d%H')


def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "vhs:S:d:", ["verbose", "help", "start=", "stop=", "dump="])  # :v to add arg
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
        
        elif o in ("-h", "--help"):
            usage()
            sys.exit()
        
        elif o in ("-s", "--start"):

            if a == "streamr":
                from streamr import start as streamrStart
                pid = os.fork()
                if (pid == 0):          # The first child.
                    os.chdir("/")
                    os.setsid()
                    os.umask(0) 
                    pid2 = os.fork() 
                    if (pid2 == 0):     # Second child
                        streamrStart()
                    else:
                        sys.exit()      #First child
                else:                   # Parent Code
                    sys.exit()          # Parent exists
            elif a == "ngramr":
                from ngramr import start as ngramrStart
                pid = os.fork()
                if (pid == 0):          # The first child.
                    os.chdir("/")
                    os.setsid()
                    os.umask(0) 
                    pid2 = os.fork() 
                    if (pid2 == 0):     # Second child
                        ngramrStart()
                    else:
                        sys.exit()      #First child
                else:                   # Parent Code
                    sys.exit()
            else:
                usage()
                sys.exit()

        
        elif o in ("-S", "--stop"):
            if a == "streamr":
                from streamr import stop as streamrStop
                streamrStop()
            elif a == 'ngramr':
                from ngramr import stop as ngramrStop
                ngramrStop()

        elif o in ("-d", "--dump"):
            #from ngramr import buildGramsBucket
            #buildGramsBucket(getHour(a))
            print('dump')
        
        else:
            assert False, "unhandled option"

if __name__ == "__main__":

    logging.basicConfig(filename='chattersum.log', level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    main()
