#!/usr/bin/env python
"""
update_dreqs_0101.py

This file adds all existing ECMWF data requests to the following:
ECMWF-IFS-MR hist-1950 r1i1p1f1
ECMWF-IFS-MR control-1950 r1i1p1f1
ECMWF-IFS-HR hist-1950 r2i1p1f1
ECMWF-IFS-HR hist-1950 r3i1p1f1
ECMWF-IFS-HR hist-1950 r4i1p1f1
"""
import argparse
import logging.config
import os
import sys

from cf_units import date2num, CALENDAR_GREGORIAN

import django
django.setup()
from pdata_app.models import ClimateModel, DataRequest, Experiment
from pdata_app.utils.common import delete_drs_dir

__version__ = '0.1.0b1'

DEFAULT_LOG_LEVEL = logging.WARNING
DEFAULT_LOG_FORMAT = '%(levelname)s: %(message)s'

logger = logging.getLogger(__name__)


def delete_files(query_set):
    """
    Delete any files online from the specified queryset
    """
    directories_found = []
    for df in query_set:
        if df.online:
            try:
                os.remove(os.path.join(df.directory, df.name))
            except OSError as exc:
                logger.error(str(exc))
                sys.exit(1)
            else:
                if df.directory not in directories_found:
                    directories_found.append(df.directory)
            df.online = False
            df.directory = None
            df.save()

    for directory in directories_found:
        if not os.listdir(directory):
            delete_drs_dir(directory)
    logger.debug('{} directories removed'.format(len(directories_found)))


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
    ecmwf_mr = ClimateModel.objects.get(short_name='ECMWF-IFS-MR')

    # MR control-1950 r1i1p1f1
    data_reqs = DataRequest.objects.filter(
        climate_model__short_name='ECMWF-IFS-LR',
        experiment__short_name='control-1950',
        rip_code='r1i1p1f1'
    )
    num_created = 0
    for data_req in data_reqs:
        data_req.id = None
        data_req.climate_model=ecmwf_mr
        data_req.save()
        num_created += 1
    logger.debug('{} MR control-1950 r1i1p1f1 data requests created.'.
                 format(num_created))

    # MR hist-1950 r1i1p1f1
    data_reqs = DataRequest.objects.filter(
        climate_model__short_name='ECMWF-IFS-LR',
        experiment__short_name='hist-1950',
        rip_code='r1i1p1f1'
    )
    num_created = 0
    for data_req in data_reqs:
        data_req.id = None
        data_req.climate_model=ecmwf_mr
        data_req.save()
        num_created += 1
    logger.debug('{} MR hist-1950 r1i1p1f1 data requests created.'.
                 format(num_created))

    # HR hist-1950 r2i1p1f1
    data_reqs = DataRequest.objects.filter(
        climate_model__short_name='ECMWF-IFS-HR',
        experiment__short_name='hist-1950',
        rip_code='r1i1p1f1'
    )
    num_created = 0
    for data_req in data_reqs:
        data_req.id = None
        data_req.rip_code='r2i1p1f1'
        data_req.save()
        num_created += 1
    logger.debug('{} HR hist-1950 r2i1p1f1 data requests created.'.
                 format(num_created))

    # HR hist-1950 r3i1p1f1
    data_reqs = DataRequest.objects.filter(
        climate_model__short_name='ECMWF-IFS-HR',
        experiment__short_name='hist-1950',
        rip_code='r1i1p1f1'
    )
    num_created = 0
    for data_req in data_reqs:
        data_req.id = None
        data_req.rip_code='r3i1p1f1'
        data_req.save()
        num_created += 1
    logger.debug('{} HR hist-1950 r3i1p1f1 data requests created.'.
                 format(num_created))

    # HR hist-1950 r4i1p1f1
    data_reqs = DataRequest.objects.filter(
        climate_model__short_name='ECMWF-IFS-HR',
        experiment__short_name='hist-1950',
        rip_code='r1i1p1f1'
    )
    num_created = 0
    for data_req in data_reqs:
        data_req.id = None
        data_req.rip_code='r4i1p1f1'
        data_req.save()
        num_created += 1
    logger.debug('{} HR hist-1950 r4i1p1f1 data requests created.'.
                 format(num_created))


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
