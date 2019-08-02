#!/usr/bin/env python
"""
update_dreqs_0207.py

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
    yohan = User.objects.get(username='yruprich')
    rr = RetrievalRequest.objects.create(requester=yohan, start_year=1950,
                                         end_year=2050)
    time_zone = datetime.timezone(datetime.timedelta())
    rr.date_created = datetime.datetime(2000, 1, 1, 0, 0, tzinfo=time_zone)
    rr.save()

    drs = DataRequest.objects.filter(
        climate_model__short_name='EC-Earth3P-HR',
        experiment__short_name__in=['primWP5-amv-neg', 'primWP5-amv-pos'],
        variable_request__cmor_name__in=['thetao', 'uo', 'vo', 'mlotst',
                                         'evspsbl', 'wap']
    )
    rr.data_request.add(*drs)

    logger.debug('Request id {} created'.format(rr.id))


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
