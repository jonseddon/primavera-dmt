#!/usr/bin/env python2.7
"""
update_dreqs_0013.py

This script is run to create submissions for the Met Office HadGEM3-GC31-LM 
data.
"""
import argparse
import logging.config
import subprocess
import sys

from cf_units import date2num, CALENDAR_360_DAY

import django
django.setup()

from pdata_app.models import DataSubmission, DataFile
from django.contrib.auth.models import User

__version__ = '0.1.0b1'

DEFAULT_LOG_LEVEL = logging.WARNING
DEFAULT_LOG_FORMAT = '%(levelname)s: %(message)s'

logger = logging.getLogger(__name__)

PRIMAVERA_DMT_PATH = '/home/users/jseddon/primavera/LIVE-prima-dm'
JSON_PATH = '/home/users/jseddon/temp/fixed_u-ai674'


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
    jseddon = User.objects.get(username='jseddon')
    years = list(range(1950, 2015))
    for year in years:
        logger.debug('Processing year {}'.format(year))
        ds = DataSubmission.objects.get_or_create(
            status='ARRIVED',
            incoming_directory=('/scratch/jseddon/u-ao802/output/u-ai674/'
                                  '{}'.format(year)),
            directory=None,
            user=jseddon
        )

        cmd = ('{}/scripts/run_primavera validate_data_submission.py -l debug '
               '-i {}/u-ao802_u-ai674_{}.json '
               '/scratch/jseddon/u-ao802/output/u-ai674/{}'.format(
            PRIMAVERA_DMT_PATH, JSON_PATH, year, year
        ))

        try:
            subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True)
        except subprocess.CalledProcessError as exc:
            logger.error('Year {} failed: {}'.format(year, exc.output))
            raise




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
