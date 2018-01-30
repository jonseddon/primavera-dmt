#!/usr/bin/env python2.7
"""
update_dreqs_0050.py

Identify and then remove duplicate files from EC_Earth3-HR spinup-1953. There
are two copies of some files from 1953. The checksums are different as they
generated on different days and so have different history attributes, but the
variable contents have been checked and are identical.
"""
import argparse
import logging.config
import os
import sys

from cf_units import date2num, CALENDAR_GREGORIAN

import django
django.setup()
from django.db.models import Count
from pdata_app.models import DataFile
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
    duplicate_files = (DataFile.objects.values('name')
        .filter(data_request__climate_model__short_name='EC-Earth3-HR',
                data_request__experiment__short_name='spinup-1950')
        .annotate(Count('id'))
        .order_by()
        .filter(id__count__gt=1)
    )

    duplicate_file_names = [df['name'] for df in duplicate_files]
    duplicate_file_names.sort()

    for i, dup_file in enumerate(duplicate_file_names):
        delete_file = DataFile.objects.filter(
            name=dup_file,
            incoming_directory='/group_workspaces/jasmin2/primavera2/upload/'
                               'EC-Earth-Consortium/EC-Earth-3-HR/incoming/'
                               'v20180109_knmi'
        )
        if delete_file.count() != 1:
            msg = "Didn't find exactly one file for {}".format(dup_file)
            raise ValueError(msg)
        logger.debug('{:03} {}'.format(i, delete_file.first().name))
        replace_files(delete_file)


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
