#!/usr/bin/env python
"""
update_dreqs_0091.py

This file adds an issue to CNRM-CERFACS AMIP HR r1i1p1f2 version v20180823
files to indicate that additional years are available on request.
"""
import argparse
import logging.config
import sys


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


def main(args):
    """
    Main entry point
    """
    jon = User.objects.get(username='jseddon')
    issue_txt = (
        'Only 1950 to 1959 has currently been uploaded. Please contact Jon '
        'Seddon if you require additional years and these will be made '
        'available.'
    )
    issue, _created = DataIssue.objects.get_or_create(issue=issue_txt,
                                                           reporter=jon)


    hr_cfday = DataFile.objects.filter(
        climate_model__short_name='CNRM-CM6-1-HR',
        experiment__short_name='highresSST-present',
        variable_request__table_name='CFday',
        version='v20180823',
        rip_code='r1i1p1f2',
    ).exclude(dataissue__in=[issue])
    logger.debug('{} files without issue found'.format(
        hr_cfday.count()))
    issue.data_file.add(*hr_cfday)


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
