#!/usr/bin/env python
"""
update_dreqs_0125.py

Create a custom retrieval request for Yohan Ruprich-Robert at BSC.
"""
from __future__ import unicode_literals, division, absolute_import
import argparse
import datetime
import logging.config
import sys

import django

django.setup()

from django.contrib.auth.models import User
from pdata_app.models import RetrievalRequest, DataRequest

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
    yohan = User.objects.get(username='yohan')
    rr = RetrievalRequest.objects.create(requester=yohan, start_year=1950,
                                         end_year=1950)
    time_zone = datetime.timezone(datetime.timedelta())
    rr.date_created = datetime.datetime(2000, 1, 1, 0, 0, tzinfo=time_zone)
    rr.save()

    common = {
        'experiment__short_name': 'highresSST-present',
    }
    stream_1 = [
        {'climate_model__short_name__in': ['CMCC-CM2-HR4', 'CMCC-CM2-VHR4'],
         'rip_code': 'r1i1p1f1'},
        {'climate_model__short_name__in': ['CNRM-CM6-1', 'CNRM-CM6-1-HR'],
         'rip_code__in': ['r21i1p1f2', 'r1i1p1f2']},
        {'climate_model__short_name__in': ['EC-Earth3', 'EC-Earth3-HR'],
         'rip_code': 'r1i1p1f1'},
        {'climate_model__short_name__in': ['ECMWF-IFS-LR', 'ECMWF-IFS-HR'],
         'rip_code': 'r1i1p1f1'},
        {'climate_model__short_name__in': ['HadGEM3-GC31-LM',
                                           'HadGEM3-GC31-MM',
                                           'HadGEM3-GC31-HM'],
         'rip_code': 'r1i1p1f1'},
        {'climate_model__short_name__in': ['MPIESM-1-2-HR', 'MPIESM-1-2-XR'],
         'rip_code': 'r1i1p1f1'}
    ]
    for stream in stream_1:
        drs = DataRequest.objects.filter(datafile__isnull=False,
                                         variable_request__frequency='mon',
                                         **common, **stream).distinct()
        rr.data_request.add(*drs)


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
