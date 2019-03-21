#!/usr/bin/env python
"""
update_dreqs_0144.py

This file adds an issue to ECMWF experiments that may have an aliasing of the
diurnal cycle in time mean values for the DCPP data.
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
    retish = User.objects.get(username='retish')
    issue_txt = (
        "Some ECMWF variables are not true monthly or daily means and are "
        "instead calculated from instantaneous values at the frequency they "
        "are output. Experiments and members are affected differently "
        "depending on how the output frequency was specified. This feature "
        "was first spotted when a difference was found in the variable ts "
        "between the hist-1950 and control-1950 simulations."
    )
    issue, _created = DataIssue.objects.get_or_create(issue=issue_txt,
                                                      reporter=retish)

    affected_files = DataFile.objects.filter(
        institute__short_name='ECMWF',
        frequency__in=['mon', 'day'],
        experiment__short_name__in=['', ''],
        variable_request__cmor_name__in=['lai', 'snw', 'stlsi', 'mrso',
                                            'clwvi', 'clivi', 'ps', 'prw',
                                            'tsl', 'snd', 'ts', 'tsn',
                                            'uneutrals', 'vneutrals', 'tso']
    )

    logger.debug('{} affected files found'.format(affected_files.count()))
    # issue.data_file.add(*affected_files)


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
