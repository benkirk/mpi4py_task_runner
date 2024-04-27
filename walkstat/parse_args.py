#!/usr/bin/env python3

import argparse
import os



def parse_options():

    parser = argparse.ArgumentParser(description='List Defaults CLI')

    parser.add_argument('-d', '--dirs', nargs='+', help='List of Directories', required=True)

    # # Adding an optional argument with a default value
    # parser.add_argument('-u', '--user', default='there', help='The user to be greeted')
    #
    # # Adding a positional argument (action) with choices
    # #parser.add_argument('action', choices=['greet', 'farewell'], default='greet', help='The action to perform')
    #
    # parser.add_argument('--numbers', nargs='*', type=int, default=[1, 2, 3], required=False, help='List of integers (default: 1 2 3)')
    #
    # # Adding a short form (-d) and a long form (--directory) argument
    # parser.add_argument('-d', '--directory', help='Specify the directory path')
    # # Optional input file argument
    # parser.add_argument('-i', '--input', help='Input file name')


    parser.add_argument('-p', '--progress', type=int, default=5, help='Frequency to report progress (default:5 seconds, <=0 disables)')
    parser.add_argument('-s', '--summary', default=None, help='Summary spreadsheet', type=str, required=False)
    parser.add_argument('-e', '--errors', default=None, help='Error log file name (default: stderr)', type=str, required=False)
    parser.add_argument('-v', '--verbose', action='store_true', help='Print detailed information')
    parser.add_argument('--heap-size', default=500, type=int, required=False, help='queue depth for tracking large files / directories')
    parser.add_argument('--threshold-size', default='10MB', type=str, required=False, help='Only consider directories larger than this threshold when tracking mtimes/atimes (string')
    parser.add_argument('--threshold-count', default=10000, type=int, required=False, help='Only consider directories larger than this threshold when tracking mtimes/atimes (count)')

    args = parser.parse_args()

    if 0 >= args.progress:
        args.progress = float('inf')

    if '0' == args.threshold_size:
        args.threshold_size = 0
    else:
        from humanfriendly import parse_size
        args.threshold_size = parse_size(args.threshold_size)

    if args.verbose:
        print('All Options: {}'.format(args))

    for d in args.dirs:
        if not os.path.isdir(d):
            print('ERROR: no such directory: {}'.format(d))
            assert(False)

    # Access the list of directories
    print(f'Processing Directories: {args.dirs}')

    return args



if __name__ == '__main__':
    opts = parse_options()

    print('\n\n',opts)
