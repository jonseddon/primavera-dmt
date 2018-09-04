#!/usr/bin/env python2.7
"""
update_dreqs_0066.py

This file adds an variant_label to all existing data_requests.
"""
import argparse
import logging.config
import sys


import django
django.setup()
from django.contrib.auth.models import User

from pdata_app.models import DataRequest


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
    awi_variant_label = 'r1i1p1f002'
    awi_reqs = DataRequest.objects.filter(institute__short_name='AWI')
    for data_req in awi_reqs:
        if data_req.datafile_set.count() > 0:
            request_rip_codes = set(data_req.datafile_set.values_list('rip_code'))
            if len(request_rip_codes) != 1:
                logger.error('More than 1 variant_label for {}'.
                             format(data_req))
                sys.exit(1)
            elif list(request_rip_codes)[0] != (awi_variant_label, ):
                logger.error('AWI variant_label does not equal {}: {}'.
                             format(awi_variant_label, data_req))
                sys.exit(1)
            else:
                data_req.rip_code = awi_variant_label
                data_req.save()
        else:
            data_req.rip_code = awi_variant_label
            data_req.save()


    other_variant_label = 'r1i1p1f1'
    other_reqs = DataRequest.objects.exclude(institute__short_name='AWI')
    for data_req in other_reqs:
        if data_req.datafile_set.count() > 0:
            request_rip_codes = set(data_req.datafile_set.values_list('rip_code'))
            if len(request_rip_codes) != 1:
                logger.error('More than 1 variant_label for {}'.
                             format(data_req))
                sys.exit(1)
            elif list(request_rip_codes)[0] != (other_variant_label, ):
                logger.error('AWI variant_label does not equal {}: {}'.
                             format(other_variant_label, data_req))
                sys.exit(1)
            else:
                data_req.rip_code = other_variant_label
                data_req.save()
        else:
            data_req.rip_code = other_variant_label
            data_req.save()


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
