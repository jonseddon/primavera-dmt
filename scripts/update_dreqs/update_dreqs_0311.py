#!/usr/bin/env python
"""
update_dreqs_0311.py

Remove the current version of the EC-Earth3P primWP5-amv-pos and
primWP5-amv-neg r[ls]us data so they can be replaced.
"""
import argparse
import logging.config
import sys

import django
django.setup()
from pdata_app.models import DataRequest  # nopep8
from pdata_app.utils.replace_file import replace_files  # nopep8

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
                                                  'debug, info, warn (the '
                                                  'default), or error')
    parser.add_argument('--version', action='version',
                        version='%(prog)s {}'.format(__version__))
    args = parser.parse_args()

    return args


def main(args):
    """
    Main entry point
    """
    dreqs = DataRequest.objects.filter(
        climate_model__short_name = 'EC-Earth3P',
        experiment__short_name__in = ['primWP5-amv-pos','primWP5-amv-neg'],
        variable_request__cmor_name__in = ['rsus', 'rlus']
    ).distinct()

    num_dreqs = dreqs.count()
    if num_dreqs != 100:
        logger.error(f'{num_dreqs} affected data requests found')
        sys.exit(1)

    for dreq in dreqs:
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
    main(cmd_args)
