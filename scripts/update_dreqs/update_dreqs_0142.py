#!/usr/bin/env python
"""
update_dreqs_0142.py

This file adds an issue to ECMWF hist and control Amon ts coupled.
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
    malcolm = User.objects.get(username='mjrobert')
    issue_txt = (
        "An unexpected difference has been found in Amon ts at the start of the hist-1950 and "
        "control-1950 experiments. It is believed that these should the be same but there is a "
        "difference of a few degrees. It isn't known if anything else is affeced. tas does not "
        "appear to be affected."
    )
    issue, _created = DataIssue.objects.get_or_create(issue=issue_txt, reporter=malcolm)

    spinup = DataFile.objects.filter(
        climate_model__short_name__in=['ECMWF-IFS-LR', 'ECMWF-IFS-HR'],
        experiment__short_name__in=['control-1950', 'hist-1950'],
        rip_code='r1i1p1f1',
    )
    logger.debug('{} spinup files found'.format(spinup.count()))
    issue.data_file.add(*spinup)


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
