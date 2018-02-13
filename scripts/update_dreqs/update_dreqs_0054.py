#!/usr/bin/env python2.7
"""
update_dreqs_0054.py

This file replaces any AWI AWI-CM-1-0-LR files from version v20171119
because they had been corrupted by CDO during production.

It also removes the related issue from 189 files from a different
submission that weren't affected.
"""
import argparse
import logging.config
import sys


import django
django.setup()

from pdata_app.models import DataFile, DataIssue
from pdata_app.utils.replace_file import replace_files


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
    replaced_files = DataFile.objects.filter(
        climate_model__short_name='AWI-CM-1-0-LR',
        experiment__short_name__in=['spinup-1950', 'control-1950',
                                    'hist-1950'],
        version='v20171119',
        data_submission__incoming_directory='/group_workspaces/jasmin2/'
                                            'primavera2/upload/AWI/'
                                            'regridded360x180/core'
    )
    logger.debug('{} low res files will be replaced'.
                 format(replaced_files.count()))

    replace_files(replaced_files)

    remove_issue_files = DataFile.objects.filter(
        climate_model__short_name='AWI-CM-1-0-LR',
        experiment__short_name__in=['spinup-1950', 'control-1950',
                                    'hist-1950'],
        version='v20171119',
        data_submission__incoming_directory='/group_workspaces/jasmin2/'
                                            'primavera2/upload/AWI/'
                                            'regridded360x180/core_nogrid'
    )
    logger.debug('Issue 4 will be removed from {} files'.
                 format(remove_issue_files.count()))

    awi_issue = DataIssue.objects.get(id=4)
    awi_issue.data_file.remove(*remove_issue_files)

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
