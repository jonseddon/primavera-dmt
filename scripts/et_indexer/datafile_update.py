#!/usr/bin/env python2.7
"""
Update data file inventory: checksums and value ranges

This adds a checksum and value ranges to files that have already been added to
the database by create_file_entry.py.
"""
import argparse
import os
from ncdf_indexer import NetCDFIndexedFile

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Update data file inventory: checksums and value ranges")
    parser.add_argument("files", type=str, nargs="+",
        help="Files to update in inventory database")
    parser.add_argument("--checksum", type=str, default="/usr/bin/md5sum",
        help="checksum routine to use")
    parser.add_argument("--overwrite", action="store_true",
        help="Overwrite existing checksums (USE WITH CARE)")
    parser.add_argument("--quiet", "-q", action="store_true",
        help="minimal output")

    args = parser.parse_args()
    quiet = args.quiet

    if not quiet:
        print "Resolving links:"
    files = map(os.path.realpath, args.files)
    for f, rf in zip(args.files, files):
        if f != rf:
            if not quiet:
                print "\t%s -> %s" % (f, rf)

    for f in files:
        f_index = NetCDFIndexedFile(f)

        if not quiet:
            print "checksum:", f
        try:
            f_index.retrieve()
            f_index.checksum(checksummer=args.checksum,
                             overwrite=args.overwrite,
                             quiet=quiet)
        except Exception as err:
            print "Failed:", err

        if not quiet:
            print "variable summary:", f
        try:
            f_index.calculate_variable_summary()
        except Exception as err:
            print "Error during calculate_variable_summary:", err
