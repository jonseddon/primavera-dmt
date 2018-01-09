#!/usr/bin/env python2.7
"""
update_dreqs_0048.py

This file adds an issue to any ECMWF-IFS-LR coupled files received so far
describing the coupling issue that was found with them.
"""
import argparse
import logging.config
import os
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
    issue_txt = ('A bug has been discovered in this file, where there was an '
                 'inconsistency in the coupling frequency used in the ocean '
                 'and atmosphere. The impact on the mean biases is generally '
                 'small, but could be important for some processes (e.g. '
                 'Arctic sea ice edge). ECMWF intend to repeat their '
                 'low-resolution coupled experiments with a consistent '
                 'coupling frequency of 1 hour and release a new version of '
                 'the data to JASMIN. Please contact Chris Roberts at ECMWF '
                 'if you would like further information.')
    di = DataIssue.objects.create(issue=issue_txt, reporter=jon)
    for datafile in DataFile.objects.filter(climate_model__short_name=
                                            'ECMWF-IFS-LR',
                                            experiment__short_name__in=[
                                                'spinup-1950', 'control-1950',
                                                'hist-1950']):
        di.data_file.add(datafile)
        di.save()


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
