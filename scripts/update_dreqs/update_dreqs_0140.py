#!/usr/bin/env python
"""
update_dreqs_0140.py

This file adds an issue to CNRM-CERFACS LR coupled:

spinup-1950: r1i1p1f2, v20180906
control-1950: r2i1p1f2, v20180904
hist-1950: r2i1p1f2, v20181004

To indicate that there was a problem with the initialization.
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
    marie_pierre = User.objects.get(username='mpmoine')
    issue_txt = (
        "These simulations were initialized with WOA13 (and not EN4). They will be"
        "replaced."
    )
    issue, _created = DataIssue.objects.get_or_create(issue=issue_txt, reporter=marie_pierre)

    spinup = DataFile.objects.filter(
        climate_model__short_name='CNRM-CM6-1',
        experiment__short_name='spinup-1950',
        version='v20180906',
        rip_code='r1i1p1f2',
    )
    logger.debug('{} spinup files found'.format(spinup.count()))
    issue.data_file.add(*spinup)

    control = DataFile.objects.filter(
        climate_model__short_name='CNRM-CM6-1',
        experiment__short_name='control-1950',
        version='v20180904',
        rip_code='r2i1p1f2',
    )
    logger.debug('{} control files found'.format(control.count()))
    issue.data_file.add(*control)

    hist = DataFile.objects.filter(
        climate_model__short_name='CNRM-CM6-1',
        experiment__short_name='hist-1950',
        version='v20181004',
        rip_code='r2i1p1f2',
    )
    logger.debug('{} hist files found'.format(hist.count()))
    issue.data_file.add(*hist)


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
