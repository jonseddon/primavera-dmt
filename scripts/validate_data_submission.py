#!/usr/bin/env python2.7
"""
validate_data_submission.py

This script is run by users to validate submitted data files and to create a
data submission in the Data Management Tool.
"""
import argparse
import logging
import os
import sys

import django
django.setup()


__version__ = '0.1.0b'

DEFAULT_LOG_LEVEL = logging.WARNING
DEFAULT_LOG_FORMAT = '%(levelname)s: %(message)s'

logger = logging.getLogger(__name__)


def list_nc_files(directory):
    """
    Return a list of all the files in the submission directory structure.

    :param str directory: The root directory of the submission
    :returns: A list of absolute filepaths
    """
    nc_files = []

    dir_files = os.listdir(directory)
    for filename in dir_files:
        if os.path.isdir(filename):
            pass

    return nc_files


def create_database_submission():
    """
    Create an entry in the database for this submission
    """
    pass


def parse_args():
    """
    Parse command-line arguments
    """
    parser = argparse.ArgumentParser(description='Validatea and create a '
        'PRIMAVERA data submission')
    parser.add_argument('directory', help="the submission's top-level directory")
    parser.add_argument('--project', help='the project that data is ultimately '
        'being submitted to (default: %(default)s)', default='CMIP6')
    parser.add_argument('--log-level', help='set logging level to one of '
        'debug, info, warn (the default), or error')
    parser.add_argument('--version', action='version',
        version='%(prog)s {}'.format(__version__))
    args = parser.parse_args()

    return args


def main(args):
    """
    Main entry point
    """
    data_files = list_nc_files(args.directory)

    create_database_submission()

if __name__ == "__main__":
    cmd_args = parse_args()

    console = logging.StreamHandler(stream=sys.stdout)
    fmtr = logging.Formatter(fmt=DEFAULT_LOG_FORMAT)
    console.setFormatter(fmtr)
    logger.addHandler(console)
    if cmd_args.log_level:
        try:
            logger.setLevel(getattr(logging, cmd_args.log_level.upper()))
        except AttributeError:
            logger.setLevel(logging.WARNING)
            logger.error('log-level must be one of: debug, info, warn or error')
            sys.exit(1)
    else:
        logger.setLevel(DEFAULT_LOG_LEVEL)

    main(cmd_args)
