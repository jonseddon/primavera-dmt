#!/usr/bin/env python2.7
"""
update_dreqs_0052.py

This file adds an issue to any CNRM-CERFACS highresSST-present file that had
the incorrect volcanic AOD applied.
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
        'An error has been identified in the CNRM-CERFACS CNRM-CM6-1 and '
        'CNRM-CM6-1-HR models for the AMIP highresSST-present data. A nearly '
        'uniform value of volcanic AOD was applied over all of the globe. This '
        'should only have consequences for users interested in the impact of '
        'volcanoes. CNRM-CERFACS are currently rerunning these simulations and '
        'the new data will be uploaded to JASMIN as soon as possible.\n\n'
        'Ideally any results that you have should be checked against the new '
        'data before being published. CNRM-CERFACS have asked that if you '
        'publish any results from the existing data then you should cite the '
        'model as CNRM-CM6-0. This CNRM-CM6-0 data will not be published '
        'through the ESGF, nor will there be any documentation available for '
        'it.\n\n'
        'When the new data is available then all existing CNRM-CERFACS data '
        'will be removed from disk and you will need to request that the new '
        'data is restored from disk to tape. Although I will try to keep on '
        'disk the variables that have already been restored from tape. The '
        'version string in the directory path will be updated to reflect the '
        'new version of these files. We hope to have the new data uploaded to '
        'JASMIN by the end of February for the low-resolution and mid-April '
        'for the high resolution.'
    )
    cerfacs_issue = DataIssue.objects.create(issue=issue_txt, reporter=jon)
    lowres_files = DataFile.objects.filter(
        climate_model__short_name='CNRM-CM6-1',
        experiment__short_name='highresSST-present',
        version='v20170614'
    )
    logger.debug('{} low res files found'.format(lowres_files.count()))

    highres_files = DataFile.objects.filter(
        climate_model__short_name='CNRM-CM6-1-HR',
        experiment__short_name='highresSST-present',
        version='v20170622'
    )
    logger.debug('{} high res files found'.format(highres_files.count()))

    cerfacs_issue.data_file.add(*lowres_files)
    cerfacs_issue.data_file.add(*highres_files)


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
