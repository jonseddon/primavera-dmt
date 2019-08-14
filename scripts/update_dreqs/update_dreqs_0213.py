#!/usr/bin/env python
"""
update_dreqs_0213.py

Create an issue for EC-Earth3P-HR highres-future r1i1p2f1 about the reference
date changing every year.
"""
import argparse
import logging.config
import os
import sys

from cf_units import date2num, CALENDAR_GREGORIAN

import django
django.setup()
from django.contrib.auth.models import User
from pdata_app.models import DataFile, DataIssue

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


def main():
    """
    Main entry point
    """
    gijs = User.objects.get(username='gvdoord')
    hist_txt = (
        'Each atmospheric file has a reference date starting at the '
        'beginning of every year rather than at the start of the simulation. '
        'These files will be fixed in September 2019 and this issue will be '
        'removed from them when they have been fixed.'
    )
    hist_issue, _created = DataIssue.objects.get_or_create(issue=hist_txt,
                                                           reporter=gijs)

    affected_files = DataFile.objects.filter(
        data_submission__incoming_directory__contains='s2hh-future'
    ).exclude(
        data_submission__incoming_directory__contains='Oday'
    ).exclude(
        data_submission__incoming_directory__contains='Omon'
    ).exclude(
        data_submission__incoming_directory__contains='SIday'
    ).exclude(
        data_submission__incoming_directory__contains='SImon'
    )

    logger.debug('{} affected files found'.format(affected_files.count()))

    hist_issue.data_file.add(*affected_files)


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
    main()
