#!/usr/bin/env python
"""
update_dreqs_0215.py

Create data requests for HadGEM3-GC31 highres-future LL, MM, HM and HH members.
"""
from __future__ import (unicode_literals, division, absolute_import,
                        print_function)
import argparse
from itertools import chain
import logging.config
import sys

import django
django.setup()

from pdata_app.models import ClimateModel, DataRequest, Institute


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
    # LL
    i1_dreqs = DataRequest.objects.filter(
        climate_model__short_name='HadGEM3-GC31-LL',
        experiment__short_name='highres-future',
        rip_code='r1i1p1f1'
    )

    logger.debug('{} r1i1p1f1 LL data requests found'.format(i1_dreqs.count()))

    for dreq in i1_dreqs:
        for i in chain(range(2, 5), (9,)):
            dreq.id = None
            dreq.rip_code = f'r1i{i}p1f1'
            dreq.save()

    # MM
    i1_dreqs = DataRequest.objects.filter(
        climate_model__short_name='HadGEM3-GC31-MM',
        experiment__short_name='highres-future',
        rip_code='r1i1p1f1'
    )

    logger.debug('{} r1i1p1f1 MM data requests found'.format(i1_dreqs.count()))

    for dreq in i1_dreqs:
        for i in range(2, 6):
            dreq.id = None
            dreq.rip_code = f'r1i{i}p1f1'
            dreq.save()

    # HM
    i1_dreqs = DataRequest.objects.filter(
        climate_model__short_name='HadGEM3-GC31-HM',
        experiment__short_name='highres-future',
        rip_code='r1i1p1f1'
    )

    logger.debug('{} r1i1p1f1 HM data requests found'.format(i1_dreqs.count()))

    nerc = Institute.objects.get(short_name='NERC')
    mohc = Institute.objects.get(short_name='MOHC')

    for dreq in i1_dreqs:
        for i in range(2, 4):
            dreq.id = None
            dreq.rip_code = f'r1i{i}p1f1'
            if i == 2:
                dreq.institute = nerc
            else:
                dreq.institute = mohc
            dreq.save()

    # HH
    i1_dreqs = DataRequest.objects.filter(
        climate_model__short_name='HadGEM3-GC31-HM',
        experiment__short_name='highres-future',
        rip_code='r1i1p1f1'
    )

    logger.debug('{} r1i1p1f1 HH data requests found'.format(i1_dreqs.count()))

    hh = ClimateModel.objects.get(short_name='HadGEM3-GC31-HH')

    for dreq in i1_dreqs:
        dreq.id = None
        dreq.climate_model = hh
        dreq.save()


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
