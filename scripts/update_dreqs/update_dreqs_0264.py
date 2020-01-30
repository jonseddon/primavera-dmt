#!/usr/bin/env python
"""
update_dreqs_0264.py

Add an issue to all HadGEM cltcalipso and parasolRefl files. Delete them and
their sym links from disk and remove them from the DMT.
"""
import argparse
import logging.config
import sys

import django
django.setup()
from django.contrib.auth.models import User
from pdata_app.utils.replace_file import replace_files
from pdata_app.models import DataFile, DataIssue
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
    jon = User.objects.get(username='jseddon')
    long_txt = (
        "A bug has been discovered in HadGEM3's implementation of COSP. All "
        "cltcalipso and parasolRefl variables from all versions of HadGEM3 "
        "are affected and the data should not be used. These variables have "
        "been removed from PRIMAVERA and from ESGF."
    )
    cosp_issue, _created = DataIssue.objects.get_or_create(issue=long_txt,
                                                           reporter=jon)

    affected_files = DataFile.objects.filter(
        climate_model__short_name__startswith='HadGEM3',
        variable_request__cmor_name__in=['cltcalipso', 'parasolRefl']
    )

    num_files = affected_files.count()
    logger.debug(f'{num_files} affected files found')

    cosp_issue.data_file.add(*affected_files)

    delete_files(affected_files, '/gws/nopw/j04/primavera5/stream1',
                 skip_badc=True)
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
