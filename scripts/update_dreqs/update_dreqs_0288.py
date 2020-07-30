#!/usr/bin/env python
"""
update_dreqs_0288.py

Copy the CMMC-CM2-VHR4 control-1950 data from the CEDA archives back to the
PRIMAVERA group workspaces.

Th table should be specified on the command line and the tables in the archive
are:

6hrPlev
6hrPlevPt
Amon
day
LImon
Lmon
Oday
Omon
SIday
SImon
"""
import argparse
import logging.config
import os
import shutil
import sys

import django
django.setup()
from pdata_app.models import DataFile  # nopep8
from pdata_app.utils.common import construct_drs_path  # nopep8

__version__ = '0.1.0b1'

DEFAULT_LOG_LEVEL = logging.WARNING
DEFAULT_LOG_FORMAT = '%(levelname)s: %(message)s'

logger = logging.getLogger(__name__)

BASE_GWS = '/gws/nopw/j04/primavera4/stream1'
ARCHIVE_BASE = '/badc/cmip6'


def parse_args():
    """
    Parse command-line arguments
    """
    parser = argparse.ArgumentParser(description='Add additional data requests')
    parser.add_argument('table_name', help='table to copy')
    parser.add_argument('-l', '--log-level', help='set logging level to one of '
                                                  'debug, info, warn (the '
                                                  'default), or error')
    parser.add_argument('--version', action='version',
                        version='%(prog)s {}'.format(__version__))
    args = parser.parse_args()

    return args


def main(args):
    """
    Main entry point
    """
    affected_files = DataFile.objects.filter(
        climate_model__short_name='CMCC-CM2-VHR4',
        experiment__short_name='control-1950',
        variable_request__table_name=args.table_name
    ).distinct().order_by(
        'variable_request__table_name', 'variable_request__cmor_name'
    )

    num_files = affected_files.count()
    logger.debug(f'{num_files} affected files found')

    for df in affected_files:
        if not df.directory.startswith(ARCHIVE_BASE):
            logger.error(f'{df.name} not in {ARCHIVE_BASE}')
            continue
        new_dir = os.path.join(BASE_GWS, construct_drs_path(df))
        new_path = os.path.join(new_dir, df.name)
        old_path = os.path.join(df.directory, df.name)
        # shutil.copy(old_path, new_path)
        # df.directory = new_dir
        # df.save()


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
