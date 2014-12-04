# -*- coding: utf-8 -*-

import getopt
import os
import signal
import sys

import ngramr


def usage():
    print " -h, --help  Show this"


def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hsS", ["help", "start", "stop"])  # :v to add arg
    except getopt.GetoptError as err:
        print str(err) # will print something like "option -a not recognized"
        usage()
        sys.exit(2)
    output = None
    verbose = False
    for o, a in opts:
        if o == "-v":
            verbose = True
        elif o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o in ("-s", "--start"):
            from streamr import start as streamrStart
            streamrStart()
        elif o in ("-S", "--stop"):
            from streamr import stop as streamrStop
            streamrStop()
        else:
            assert False, "unhandled option"

if __name__ == "__main__":
    main()