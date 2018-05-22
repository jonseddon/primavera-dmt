#!/usr/bin/env python2.7
"""
update_dreqs_0065.py

This file adds an issue to any N512 MOHC coupled files from specified versions.
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
        'There is a problem with the freshwater surface budget - it is '
        'considerably negative (freshening). This results in a decreasing '
        'AMOC. This data is not suitable for studying the  large-scale impact '
        'of resolution on circulation. This data will be replaced but due to '
        'the resolution these simulations will take some time to run.'
    )
    mohc_issue = DataIssue.objects.create(issue=issue_txt, reporter=jon)

    hm_control = DataFile.objects.filter(
        climate_model__short_name='HadGEM3-GC31-HM',
        institute__short_name='MOHC',
        experiment__short_name='control-1950',
        version='v20171025'
    )
    logger.debug('HM control {} affected files found'.format(hm_control.count()))
    mohc_issue.data_file.add(*hm_control)

    hm_hist = DataFile.objects.filter(
        climate_model__short_name='HadGEM3-GC31-HM',
        institute__short_name='MOHC',
        experiment__short_name='hist-1950',
        version='v20171024'
    )
    logger.debug('HM hist {} affected files found'.format(hm_hist.count()))
    mohc_issue.data_file.add(*hm_hist)

    hh_hist = DataFile.objects.filter(
        climate_model__short_name='HadGEM3-GC31-HH',
        institute__short_name='MOHC',
        experiment__short_name='hist-1950',
        version='v20171213'
    )
    logger.debug('HH hist {} affected files found'.format(hh_hist.count()))
    mohc_issue.data_file.add(*hh_hist)

    hh_control = DataFile.objects.filter(
        climate_model__short_name='HadGEM3-GC31-HH',
        institute__short_name='NERC',
        experiment__short_name='control-1950',
        version='v20171220'
    )
    logger.debug('HH control {} affected files found'.format(hh_control.count()))
    mohc_issue.data_file.add(*hh_control)


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
