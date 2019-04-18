#!/usr/bin/env python
"""
update_dreqs_0161.py

Restore all HadGEM3-GC31 day tasmax and tasmin files.
"""
import argparse
import logging.config
import os
import sys

from cf_units import date2num, CALENDAR_GREGORIAN

import django
django.setup()
from pdata_app.utils.replace_file import restore_files
from pdata_app.models import ReplacedFile

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
    # first add in two missing checksums
    rf= ReplacedFile.objects.get(
        name='tasmin_day_HadGEM3-GC31-LL_highres-future_r1i1p1f1_gn_'
             '20390101-20391230.nc', size=20933155)
    rf.checksum_value='974051111'
    rf.checksum_type = 'ADLER32'
    rf.save()
    rf= ReplacedFile.objects.get(
        name='tasmax_day_HadGEM3-GC31-LL_highres-future_r1i1p1f1_gn_'
             '20390101-20391230.nc', size=20657477)
    rf.checksum_value='739424075'
    rf.checksum_type = 'ADLER32'
    rf.save()

    # now find the files required and restore them
    files = ReplacedFile.objects.filter(
        climate_model__short_name__startswith='HadGEM3-GC31',
        variable_request__table_name='day',
        variable_request__cmor_name__in=['tasmax', 'tasmin']
    ).exclude(
        climate_model__short_name='HadGEM3-GC31-HH',
        experiment__short_name='hist-1950',
        version='v20171213'
    ).exclude(
        climate_model__short_name='HadGEM3-GC31-HM',
        experiment__short_name='hist-1950',
        version='v20171024'
    ).exclude(
        climate_model__short_name='HadGEM3-GC31-HM',
        experiment__short_name='control-1950',
        version='v20171025'
    )
    logger.debug('{} affected files found'.format(files.count()))

    restore_files(files)


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
