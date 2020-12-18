#!/usr/bin/env python
"""
update_dreqs_0310.py

Replace the current EC-Earth3P highresSST-present r1i1p1f1 r[ls]u[ts]*
data so that the edited files can be submitted.
"""
import argparse
import logging.config
import sys

import django
django.setup()
from pdata_app.models import DataRequest, Settings  # nopep8
from pdata_app.utils.common import delete_files  # nopep8
from pdata_app.utils.replace_file import replace_files  # nopep8

__version__ = '0.1.0b1'

DEFAULT_LOG_LEVEL = logging.WARNING
DEFAULT_LOG_FORMAT = '%(levelname)s: %(message)s'

logger = logging.getLogger(__name__)

# The top-level directory to write output data to
BASE_OUTPUT_DIR = Settings.get_solo().base_output_dir


def parse_args():
    """
    Parse command-line arguments
    """
    parser = argparse.ArgumentParser(description='Add additional data requests')
    parser.add_argument('-l', '--log-level', help='set logging level to one of '
                                                  'debug, info, warn (the '
                                                  'default), or error')
    parser.add_argument('--version', action='version',
                        version='%(prog)s {}'.format(__version__))
    args = parser.parse_args()

    return args


def main():
    """
    Main entry point
    """
    fixable = DataRequest.objects.filter(
        climate_model__short_name__contains='EC-Earth3P',
        experiment__short_name='highresSST-present',
        variable_request__cmor_name__regex='r[ls]u[ts]*',
        rip_code='r1i1p1f1',
        datafile__isnull=False
    ).distinct()

    broken = DataRequest.objects.filter(
        climate_model__short_name__contains='EC-Earth3P',
        experiment__short_name='highresSST-present',
        variable_request__cmor_name__in=['rsdscs', 'rsuscs'],
        rip_code='r1i1p1f1',
        datafile__isnull=False
    ).distinct()

    dreqs = broken | fixable

    num_dreqs = dreqs.distinct().count()
    if num_dreqs != 47:
        logger.error(f'{num_dreqs} affected data requests found')
        sys.exit(1)

    for dreq in dreqs:
        delete_files(dreq.datafile_set.all(), BASE_OUTPUT_DIR, skip_badc=True)
        replace_files(dreq.datafile_set.all())


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
