#!/usr/bin/env python2.7
"""
Update data file inventory: checksums and value ranges

This adds a checksum and value ranges to files that have already been added to
the database by create_file_entry.py.

This version takes a file containing filenames and works in parallel to update
each file.
"""
import argparse
import itertools
from multiprocessing import Process, Manager
import os
from ncdf_indexer import NetCDFIndexedFile
import django
django.setup()


def update_file(params):
    """
    Update an individual file.
    
    :param str filename: The complete path of the file to process
    """
    while True:
        # close existing connections so that a fresh connection is made
        django.db.connections.close_all()

        filename = params.get()

        if filename is None:
            return

        quiet = cmd_args.quiet

        f_index = NetCDFIndexedFile(filename)

        if not quiet:
            print "checksum: {}".format(filename)
        try:
            f_index.retrieve()
            f_index.checksum(checksummer=cmd_args.checksum,
                             overwrite=cmd_args.overwrite,
                             quiet=quiet)
        except Exception as err:
            print "Failed:", err

        if not quiet:
            print "variable summary:", filename
        try:
            f_index.calculate_variable_summary()
        except Exception as err:
            print "Error during calculate_variable_summary:", err


def parse_args():
    """
    Parse command-line arguments
    """
    parser = argparse.ArgumentParser(
        description="Update data file inventory: checksums and value ranges")
    parser.add_argument("filelist", type=str,
        help="File containing list of files to add to inventory database")
    parser.add_argument('-p', '--processes', help='the number of parallel '
        'processes to use (default: %(default)s)', default=8, type=int)
    parser.add_argument("--checksum", type=str,
        default="/group_workspaces/jasmin2/primavera1/tools/adler32/adler32",
        help="checksum routine to use")
    parser.add_argument("--overwrite", action="store_true",
        help="Overwrite existing checksums (USE WITH CARE)")
    parser.add_argument("--quiet", "-q", action="store_true",
        help="minimal output")

    args = parser.parse_args()

    return args


def main(args):
    """
    Main entry point
    """
    quiet = args.quiet

    raw_filenames = []

    with open(args.filelist, "r") as fh:
        for f in fh.readlines():
            raw_filenames.append(f.strip())

    if not quiet:
        print "Resolving links:"
    files = map(os.path.realpath, raw_filenames)
    for f, rf in zip(raw_filenames, files):
        if f != rf:
            if not quiet:
                print "\t%s -> %s" % (f, rf)

    jobs = []
    manager = Manager()
    params = manager.Queue()
    for i in range(args.processes):
        p = Process(target=update_file, args=(params,))
        jobs.append(p)
        p.start()

    iters = itertools.chain(files, (None,) * args.processes)
    for item in iters:
        params.put(item)

    for j in jobs:
        j.join()


if __name__ == '__main__':
    cmd_args = parse_args()

    # run the code
    main(cmd_args)
