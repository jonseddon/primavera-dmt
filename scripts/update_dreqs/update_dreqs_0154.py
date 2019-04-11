#!/usr/bin/env python
"""
update_dreqs_0154.py

This file removes all CERFACS files that were initialized with WOA13 from:

CNRM-CM6-1 spinup-1950 r1i1p1f2 v20180906
CNRM-CM6-1 control-1950 r2i1p1f2 v20180904
CNRM-CM6-1 hist-1950 r2i1p1f2 v20181004
"""
import argparse
import logging.config
import os
import sys

from cf_units import date2num, CALENDAR_GREGORIAN

import django
django.setup()
from pdata_app.utils.replace_file import replace_files
from pdata_app.models import DataFile
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
    for df in query_set.filter(online=True):
        try:
            os.remove(os.path.join(df.directory, df.name))
        except OSError as exc:
            logger.error(str(exc))
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
    spinup = DataFile.objects.filter(
        institute__short_name='CNRM-CERFACS',
        climate_model__short_name='CNRM-CM6-1',
        experiment__short_name='spinup-1950',
        version='v20180906',
        rip_code='r1i1p1f2'
    )
    logger.debug('{} affected spinup files found'.format(spinup.count()))
    delete_files(spinup)
    replace_files(spinup)

    control = DataFile.objects.filter(
        institute__short_name='CNRM-CERFACS',
        climate_model__short_name='CNRM-CM6-1',
        experiment__short_name='control-1950',
        version='v20180904',
        rip_code='r2i1p1f2'
    )
    logger.debug('{} affected control files found'.format(control.count()))
    delete_files(control)
    replace_files(control)

    hist = DataFile.objects.filter(
        institute__short_name='CNRM-CERFACS',
        climate_model__short_name='CNRM-CM6-1',
        experiment__short_name='hist-1950',
        version='v20181004',
        rip_code='r2i1p1f2'
    )
    logger.debug('{} affected hist files found'.format(hist.count()))
    delete_files(hist)
    replace_files(hist)


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
