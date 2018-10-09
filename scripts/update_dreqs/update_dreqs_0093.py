#!/usr/bin/env python
"""
update_dreqs_0093.py

This file adds an issue to CNRM-CERFACS AMIP HR r1i1p1f2 version v20180823 or
LR r21i1p1f2 v20180718 files to indicate that there was a problem with the
the sea ice not being seen by the atmosphere.
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
        "On sea-ice grid points, the atmospheric component of CNRM-CM6 for "
        "highresSST-present only "
        "saw the ocean SST values and not the ice values as calculated by the "
        "1-D sea-ice model included in the surface scheme. The outcome is that "
        "instead of having strongly negative surface air temperature over "
        "sea-ice in winter, the values are close to the freezing point "
        "(~ -2.°C). Another obvious consequence is that the interannual "
        "variability of surface air temperature is very weak over sea-ice. "
        "Depending on your analysis, this problem may strongly (or not so "
        "much) impact the results, so please have a careful look at the "
        "behaviour of CNRM-CM6 data when using the current simulation"
        "These simulations are being re-run and new files will be availabe by "
        "the end of 2018."
        ""
    )
    issue, _created = DataIssue.objects.get_or_create(issue=issue_txt,
                                                           reporter=jon)

    hr = DataFile.objects.filter(
        climate_model__short_name='CNRM-CM6-1-HR',
        experiment__short_name='highresSST-present',
        version='v20180823',
        rip_code='r1i1p1f2',
    ).exclude(dataissue__in=[issue])
    logger.debug('{} HR files without issue found'.format(
        hr.count()))
    issue.data_file.add(*hr)

    lr = DataFile.objects.filter(
        climate_model__short_name='CNRM-CM6-1',
        experiment__short_name='highresSST-present',
        version='v20180718',
        rip_code='r21i1p1f2',
    ).exclude(dataissue__in=[issue])
    logger.debug('{} LR files without issue found'.format(
        lr.count()))
    issue.data_file.add(*lr)


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
