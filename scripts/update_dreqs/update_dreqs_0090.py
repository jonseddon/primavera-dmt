#!/usr/bin/env python
"""
update_dreqs_0090.py

This file adds an issue to any HadGEM3-GC31-HH coupled files from specified
versions to indicate that the institution is wrong.
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
    cont_txt = (
        'The institution in metadata of this file is incorrect. The '
        'institution is reported as NERC when it should be Met Office. This '
        'will be corrected when the file is submitted to the ESGF. Please '
        'take care to correctly cite this file.'
    )
    cont_issue, _created = DataIssue.objects.get_or_create(issue=cont_txt,
                                                           reporter=jon)

    jon = User.objects.get(username='jseddon')
    hist_txt = (
        'The institution in metadata of this file is incorrect. The '
        'institution is reported as Met Office when it should be NERC. This '
        'will be corrected when the file is submitted to the ESGF. Please '
        'take care to correctly cite this file.'
    )
    hist_issue, _created = DataIssue.objects.get_or_create(issue=hist_txt,
                                                           reporter=jon)

    hh_control = DataFile.objects.filter(
        climate_model__short_name='HadGEM3-GC31-HH',
        institute__short_name='NERC',
        experiment__short_name='control-1950',
        version='v20180927'
    ).exclude(dataissue__in=[cont_issue])
    logger.debug('HH control {} files without issue found'.format(
        hh_control.count()))
    cont_issue.data_file.add(*hh_control)

    hh_hist = DataFile.objects.filter(
        climate_model__short_name='HadGEM3-GC31-HH',
        institute__short_name='MOHC',
        experiment__short_name='hist-1950',
        version='v20180927'
    ).exclude(dataissue__in=[hist_issue])
    logger.debug('HH hist {} files without issue found'.format(
        hh_hist.count()))
    hist_issue.data_file.add(*hh_hist)


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
