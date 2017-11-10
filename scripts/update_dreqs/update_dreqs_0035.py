#!/usr/bin/env python2.7
"""
update_dreqs_0035.py

This file moves for specified retrieval requests some files from their incoming
directory into the DRS directory structure.
"""
import argparse
import logging.config
import os
import shutil
import sys


import django
django.setup()

from pdata_app.models import Settings, RetrievalRequest
from pdata_app.utils.common import construct_drs_path, adler32


__version__ = '0.1.0b1'

DEFAULT_LOG_LEVEL = logging.WARNING
DEFAULT_LOG_FORMAT = '%(levelname)s: %(message)s'

logger = logging.getLogger(__name__)

INCOMING_DIR = '/group_workspaces/jasmin2/primavera4/upload'
NEW_BASE_OUTPUT_DIR = '/group_workspaces/jasmin2/primavera2/stream1'
# The top-level directory to write output data to
BASE_OUTPUT_DIR = Settings.get_solo().base_output_dir

RET_REQS = [237, 243, 244, 245, 246, 247, 248, 254]


def parse_args():
    """
    Parse command-line arguments
    """
    parser = argparse.ArgumentParser(description='Move files')
    parser.add_argument('-l', '--log-level', help='set logging level to one of '
        'debug, info, warn (the default), or error')
    parser.add_argument('--version', action='version',
        version='%(prog)s {}'.format(__version__))
    args = parser.parse_args()

    return args


def main(args):
    """
    Main entry point
    """
    for ret_req in RET_REQS:
        rr = RetrievalRequest.objects.get(id=ret_req)
        logger.debug('Starting retrieval request {}'.format(ret_req))
        for dr in rr.data_request.all():
            logger.debug('Starting data request {}'.format(dr))
            num_files_moved = 0
            for df in dr.datafile_set.all():
                if df.directory.startswith(INCOMING_DIR):
                    drs_path = construct_drs_path(df)
                    dest_dir = os.path.join(NEW_BASE_OUTPUT_DIR, drs_path)
                    if not os.path.exists(dest_dir):
                        os.makedirs(dest_dir)
                    dest_path = os.path.join(dest_dir, df.name)
                    src_path = os.path.join(df.directory, df.name)
                    # copy the file
                    shutil.copy(src_path, dest_path)
                    # check its checksum
                    checksum = adler32(dest_path)
                    if checksum != df.checksum_set.first().checksum_value:
                        msg = 'Checksum does not match for {}'.format(df.name)
                        raise ValueError(msg)
                    # construct a sym link
                    primary_path = os.path.join(BASE_OUTPUT_DIR, drs_path)
                    if not os.path.exists(primary_path):
                        os.makedirs(primary_path)
                    os.symlink(dest_path, os.path.join(primary_path, df.name))
                    # update the DB
                    df.directory = dest_dir
                    df.save()
                    num_files_moved += 1
            logger.debug('{} files moved'.format(num_files_moved))






if __name__ == "__main__":
    cmd_args = parse_args()

    # determine the log level
    if cmd_args.log_level:
        try:
            log_level = getattr(logging, cmd_args.log_level.upper())
        except AttributeError:
            logger.setLevel(logging.WARNING)
            logger.error('log-level must be one of: debug, info, warn or error')
            sys.exit(1)
    else:
        log_level = DEFAULT_LOG_LEVEL

    # configure the logger
    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'standard': {
                'format': DEFAULT_LOG_FORMAT,
            },
        },
        'handlers': {
            'default': {
                'level': log_level,
                'class': 'logging.StreamHandler',
                'formatter': 'standard'
            },
        },
        'loggers': {
            '': {
                'handlers': ['default'],
                'level': log_level,
                'propagate': True
            }
        }
    })

    # run the code
    main(cmd_args)
