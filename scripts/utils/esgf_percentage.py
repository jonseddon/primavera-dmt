#!/usr/bin/env python
"""
esgf_percentage.py

Calculate the volume of data submitted to the ESGF.
"""
from __future__ import unicode_literals, division, absolute_import

import argparse
import logging.config
import sys

import django
from django.db.models import Sum
from django.template.defaultfilters import filesizeformat
django.setup()
from pdata_app.models import DataFile, ESGFDataset


__version__ = '0.1.0b1'

DEFAULT_LOG_LEVEL = logging.WARNING
DEFAULT_LOG_FORMAT = '%(levelname)s: %(message)s'

logger = logging.getLogger(__name__)


def parse_args():
    """
    Parse command-line arguments
    """
    parser = argparse.ArgumentParser(description='Calculate the volume of data '
                                                 'submitted to the ESGF.')
    parser.add_argument('-l', '--log-level', help='set logging level to one of '
                                                  'debug, info, warn (the '
                                                  'default), or error')
    parser.add_argument('--version', action='version',
                        version='%(prog)s {}'.format(__version__))
    args = parser.parse_args()

    return args


def main(args):
    """Run the script"""
    esgf_volume = 0
    for esgf in ESGFDataset.objects.all():
        dataset_volume = (esgf.data_request.datafile_set.distinct().
                          aggregate(Sum('size'))['size__sum'])
        if dataset_volume:
            esgf_volume += dataset_volume

    total_volume = (DataFile.objects.all().distinct().aggregate(Sum('size'))
                    ['size__sum'])

    pretty_esgf = filesizeformat(esgf_volume).replace('\xa0', ' ')
    pretty_total = filesizeformat(total_volume).replace('\xa0', ' ')
    print(f'Volume published to ESGF {pretty_esgf}')
    print(f'Total Volume {pretty_total}')
    print(f'Percentage published to ESGF {esgf_volume / total_volume:.0%}')


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
