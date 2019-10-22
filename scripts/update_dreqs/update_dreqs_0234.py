#!/usr/bin/env python
"""
update_dreqs_0234.py

Replace files from EC-Earth3P-HR highres-future r1i1p2f1 submissions in
s2hh-future-1 and s2hh-future-2 that were affected by a varying reference time.
"""
import argparse
import logging.config
import sys

import django
django.setup()
from pdata_app.utils.replace_file import replace_files
from pdata_app.models import DataSubmission, DataIssue
from pdata_app.utils.common import delete_files

__version__ = '0.1.0b1'

DEFAULT_LOG_LEVEL = logging.WARNING
DEFAULT_LOG_FORMAT = '%(levelname)s: %(message)s'

logger = logging.getLogger(__name__)


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
    di = DataIssue.objects.get(id=23)
    for ds_num in di.data_file.values_list('data_submission',
                                           flat=True).distinct():
        ds = DataSubmission.objects.get(id=ds_num)

        affected_files = ds.datafile_set.all()

        delete_files(affected_files, '/gws/nopw/j04/primavera5/stream1')
        replace_files(affected_files)


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
