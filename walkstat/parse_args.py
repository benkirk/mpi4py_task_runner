#!/usr/bin/env python3

import argparse

def parse_options():

    parser = argparse.ArgumentParser(description="List Defaults CLI")

    # Adding an optional argument with a default value
    parser.add_argument("-u", "--user", default="there", help="The user to be greeted")

    # Adding a positional argument (action) with choices
    #parser.add_argument("action", choices=["greet", "farewell"], default="greet", help="The action to perform")

    parser.add_argument("--numbers", nargs="*", type=int, default=[1, 2, 3], required=False, help="List of integers (default: 1 2 3)")

    # Adding a short form (-d) and a long form (--directory) argument
    parser.add_argument("-d", "--directory", help="Specify the directory path")
    # Optional input file argument
    parser.add_argument("-i", "--input", help="Input file name")

    # Optional output file argument (defaulting to stdout)
    parser.add_argument("-o", "--output", default="stdout", help="Output file name")

    # Verbose flag (optional, no value needed)
    parser.add_argument("-v", "--verbose", action="store_true", help="Print detailed information")

    args = parser.parse_args()

    # Access the provided arguments
    if args.input:
        print(f"Input file: {args.input}")
    else:
        print("No input file specified.")

    print(f"Output file: {args.output}")

    if args.verbose:
        print("Verbose mode is enabled.")

    ## Performing actions based on user input
    #if args.action == "greet":
    #    print(f"Hello, {args.user}!")
    #elif args.action == "farewell":
    #    print(f"Goodbye, {args.user}!")



    # Access the list of integers
    print(f"Received numbers: {args.numbers}")
    return parser



if __name__ == "__main__":
    opts = parse_options()

    print('\n\n',opts)
