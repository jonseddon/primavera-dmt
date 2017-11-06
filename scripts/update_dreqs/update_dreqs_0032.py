#!/usr/bin/env python2.7
"""
update_dreqs_0032.py

This file moves a retrieval request from one group workspace to another.
"""
import argparse
import logging.config
import os
import shutil
import sys


import django
django.setup()

from pdata_app.models import RetrievalRequest
from pdata_app.utils.common import construct_drs_path


__version__ = '0.1.0b1'

DEFAULT_LOG_LEVEL = logging.WARNING
DEFAULT_LOG_FORMAT = '%(levelname)s: %(message)s'

logger = logging.getLogger(__name__)

RET_REQ_ID = 28
NEW_BASE_OUTPUT_DIR = '/group_workspaces/jasmin2/primavera2/stream1'


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
    ret_req = RetrievalRequest.objects.get(id=RET_REQ_ID)

    for data_req in ret_req.data_request.all():
        logger.debug(str(data_req))
        for data_file in data_req.datafile_set.all():
            drs_path = construct_drs_path(data_file)
            dest_dir = os.path.join(NEW_BASE_OUTPUT_DIR, drs_path)
            if not os.path.exists(dest_dir):
                os.makedirs(dest_dir)
            shutil.move(os.path.join(data_file.directory, data_file.name),
                        dest_dir)
            os.symlink(os.path.join(dest_dir, data_file.name),
                       os.path.join(data_file.directory, data_file.name))
            data_file.directory = dest_dir
            data_file.save()


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
