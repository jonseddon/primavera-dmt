#!/usr/bin/env python2.7
"""
update_dreqs_0033.py

This file creates some links to files that were extracted but not linked to.
"""
import argparse
import logging.config
import os
import shutil
import sys


import django
django.setup()

from pdata_app.models import DataRequest, Settings
from pdata_app.utils.common import construct_drs_path


__version__ = '0.1.0b1'

DEFAULT_LOG_LEVEL = logging.WARNING
DEFAULT_LOG_FORMAT = '%(levelname)s: %(message)s'

logger = logging.getLogger(__name__)

NEW_BASE_OUTPUT_DIR = '/group_workspaces/jasmin2/primavera2/stream1'
# The top-level directory to write output data to
BASE_OUTPUT_DIR = Settings.get_solo().base_output_dir


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
    data_req = DataRequest.objects.get(
        climate_model__short_name='EC-Earth3',
        experiment__short_name='highresSST-present',
        variable_request__table_name='day',
        variable_request__cmor_name='va'
    )

    links_created = 0
    for data_file in data_req.datafile_set.all():
        drs_path = construct_drs_path(data_file)
        stream1_dir = os.path.join(BASE_OUTPUT_DIR, drs_path)
        stream1_file = os.path.join(stream1_dir, data_file.name)
        dest_file = os.path.join(NEW_BASE_OUTPUT_DIR, drs_path, data_file.name)
        if not os.path.exists(stream1_file):
            os.symlink(dest_file, stream1_file)
            links_created += 1

    logger.debug('{} links created'.format(links_created))




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
