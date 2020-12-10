#!/usr/bin/env python
"""
update_dreqs_0305.py

Add an issue to all HadGEM epfz files.
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
    long_txt = (
        "The data in the HadGEM epfz variables was incorrectly produced with "
        "units of kg s-2, but the metadata gave the units as m3 s-2 (other "
        "errors could also exist)."
    )
    epfz_issue, _created = DataIssue.objects.get_or_create(issue=long_txt,
                                                           reporter=jon)

    affected_files = DataFile.objects.filter(
        climate_model__short_name__startswith='HadGEM3',
        variable_request__cmor_name='epfz'
    )

    num_files = affected_files.count()
    logger.debug(f'{num_files} affected files found')

    epfz_issue.data_file.add(*affected_files)


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
