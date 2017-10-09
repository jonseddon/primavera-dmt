#!/usr/bin/env python2.7
"""
update_dreqs_0027.py

Delete existing data that was set-up using incoming_to_drs.py from known
submissions apart from items that are in certain known retrieval requests.
"""
import argparse
import logging.config
import os
import shutil
import sys

import django
django.setup()

from pdata_app.models import (DataRequest, RetrievalRequest, DataSubmission,
                              DataFile, Settings)
from pdata_app.utils.common import construct_drs_path


__version__ = '0.1.0b1'

DEFAULT_LOG_LEVEL = logging.WARNING
DEFAULT_LOG_FORMAT = '%(levelname)s: %(message)s'

logger = logging.getLogger(__name__)

# The top-level directory to write output data to
BASE_OUTPUT_DIR = Settings.get_solo().base_output_dir


def copy_cerfacs_data():
    """
    Copy data from the CERFACS submission on primavera3 to primavera5
    """
    PRIM3_DIRECTORY = ('/group_workspaces/jasmin2/primavera3/upload/'
                       'CNRM-CERFACS/CNRM-CM6-1-HR/incoming/v20170623_2000')

    ret_req = RetrievalRequest.objects.get(id=67)
    data_sub = DataSubmission.objects.get(
        incoming_directory=PRIM3_DIRECTORY
    )

    all_submission_files = data_sub.datafile_set.all()

    # Move the files that we want to keep
    logger.debug('*** Keeping files: ***')
    files_to_copy = all_submission_files.filter(
        data_request__in=ret_req.data_request.all()
    )

    for df in files_to_copy:
        logger.debug('{}'.format(df.name))
        src_path = os.path.join(df.directory, df.name)
        dest_path = os.path.join(BASE_OUTPUT_DIR, construct_drs_path(df))
        # shutil.move(src_path, dest_path)
        # df.directory = dest_path
        # df.save()
        logger.debug('shutil.move {} {}'.format(src_path, dest_path))

    # Clear the directory and status on the files that aren't going to be moved
    logger.debug('*** Deleting files: ***')
    files_to_delete = all_submission_files.exclude(
        data_request__in=ret_req.data_request.all()
    )
    for df in files_to_delete:
        logger.debug('{}'.format(df.name))
        # df.directory = None
        # df.online = False
        # df.save()
        # os.remove(os.path.join(df.directory, df.name))
        logger.debug('os.remove {}'.format(os.path.join(df.directory, df.name)))


def delete_cerfacs_prim5_data():
    """
    Delete files from the submissions on primavera5/stream1 that are not in
    the listed retrieval requests
    """
    high_res_subs = ['']


def parse_args():
    """
    Parse command-line arguments
    """
    parser = argparse.ArgumentParser(description='Add additional data requests')
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
    copy_cerfacs_data()
    delete_cerfacs_prim5_data()




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
