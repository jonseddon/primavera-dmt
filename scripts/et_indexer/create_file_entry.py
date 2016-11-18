import argparse
import os
from ncdf_indexer import NetCDFIndexedFile

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description="NetCDF file inventory tool for Elastic Tape Archive")
    parser.add_argument("filelist", type=str,
        help="File containing list of files to add to inventory database")
    parser.add_argument("--et_file", type=str,
        help="Generate list of files processed for use with et_put command")
    args = parser.parse_args()

    tmp = []
    with open(args.filelist, "r") as fh:
        for f in fh.readlines():
            tmp.append(f.strip())

    print "Resolving links:"
    files = map(os.path.realpath, tmp)
    for f, rf in zip(tmp, files):
        if f != rf:
            print "\t%s -> %s" % (f, rf)

    print "\nProcessing files:"

    success = []
    for f in files:
        print "\t", f
        f_index = NetCDFIndexedFile(f)
        try:
            f_index.process()
            success.append(f)
        except Exception as err:
            print "Failed:", err
            f_index.destroy()

    if success and (args.et_file is not None):
        print "Writing et_put file: %s" % args.et_file
        with open(args.et_file, "w") as fh:
            fh.write("\n".join(success)+"\n")
