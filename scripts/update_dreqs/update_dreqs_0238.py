#!/usr/bin/env python
"""
update_dreqs_0238.py

Replace files from EC-Earth3P highresSST-present r2i1p1f1 submissions from 1956
and 1959, which may not have been generated correctly.
"""
import argparse
import logging.config
import os
import sys

import django
django.setup()
from pdata_app.utils.common import list_files
from pdata_app.utils.replace_file import replace_files
from pdata_app.models import DataFile
from pdata_app.utils.common import delete_files

__version__ = '0.1.0b1'

DEFAULT_LOG_LEVEL = logging.WARNING
DEFAULT_LOG_FORMAT = '%(levelname)s: %(message)s'

logger = logging.getLogger(__name__)

NEW_SUBMISSION = ('/gws/nopw/j04/primavera4/upload/EC-Earth-Consortium/'
                  'EC-Earth-3/incoming/xl1a-present-fix')


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
    new_files = list_files(NEW_SUBMISSION)

    logger.debug(f'{len(new_files)} files found in the submission')

    dfs = DataFile.objects.filter(name__in=map(os.path.basename, new_files))

    logger.debug(f'{dfs.count()} files found in the DMT')

    # delete_files(affected_files, '/gws/nopw/j04/primavera5/stream1')
    # replace_files(affected_files)


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
