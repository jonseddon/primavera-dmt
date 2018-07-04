#!/usr/bin/env python2.7
"""
update_dreqs_0062.py

This file adds an issue to any CMCC-CM2-VHR4 files from version v20170927
for highresSST-present for Amon variables rsds, rsdscs, rsus, rsdt, rsut,
hfls and hfss.
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
        'Artifacts are present in these files, possibly due to a problem '
        'maybe caused by the specific partitioning of the global domain '
        '(no. of CPU) used to run the high-res AMIP model. Something did '
        'not work properly in the field reconstruction, after merging the '
        'different sub-domains. These variables may be regenerated at a later '
        'date.'
    )
    cmcc_issue = DataIssue.objects.create(issue=issue_txt, reporter=jon)
    affected_files = DataFile.objects.filter(
        climate_model__short_name='CMCC-CM2-VHR4',
        experiment__short_name='highresSST-present',
        version='v20170927',
        variable_request__table_name='Amon',
        variable_request__cmor_name__in=['rsds', 'rsdscs', 'rsus', 'rsdt',
                                         'rsut', 'hfls', 'hfss']
    )
    logger.debug('{} affected files found'.format(affected_files.count()))

    cmcc_issue.data_file.add(*affected_files)


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
