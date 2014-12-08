# -*- coding: utf-8 -*-

import getopt
import logging
import os
import signal
import sys

import ngramr


def usage():
    print " -h, --help  Show this"


def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "vhsS", ["verbose", "help", "start", "stop"])  # :v to add arg
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
        
        elif o in ("-S", "--stop"):
            from streamr import stop as streamrStop
            streamrStop()
        
        else:
            assert False, "unhandled option"

if __name__ == "__main__":

    logging.basicConfig(filename='chattersum.log', level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    main()