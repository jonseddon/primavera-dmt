#!/usr/bin/env python
"""
update_dreqs_0276.py

Check all netCDF files below the specified directory. If their sizes don't
match the values in the database then delete the file, syn link and mark the
file as offline.
"""
import argparse
import logging.config
import os

import django
django.setup()

from pdata_app.models import DataFile, Settings  # nopep8
from pdata_app.utils.common import (ilist_files, construct_drs_path,
                                    is_same_gws)  # nopep8

# The top-level directory where output data is stored
BASE_OUTPUT_DIR = Settings.get_solo().base_output_dir

__version__ = '0.1.0b1'

DEFAULT_LOG_FORMAT = '%(levelname)s: %(message)s'

logger = logging.getLogger(__name__)


def parse_args():
    """
    Parse command-line arguments
    """
    parser = argparse.ArgumentParser(description='Check file sizes')
    parser.add_argument('directory', help="the top-level directory to check")
    parser.add_argument('-l', '--log-level',
                        help='set logging level (default: %(default)s)',
                        choices=['debug', 'info', 'warning', 'error'],
                        default='info')
    parser.add_argument('--version', action='version',
                        version='%(prog)s {}'.format(__version__))
    args = parser.parse_args()

    return args


def main(args):
    """
    Main entry point
    """
    logger.debug('Starting file structure scan.')

    for nc_file in ilist_files(args.directory):
        nc_file_name = os.path.basename(nc_file)
        db_files = DataFile.objects.filter(name=nc_file_name)

        if db_files.count() == 0:
            logger.error('File not found in database: {}'.format(nc_file))
        elif db_files.count() > 1:
            logger.error('{} entries found in database for file: {}'.
                         format(db_files.count(), nc_file))
        else:
            db_file = db_files.first()

        act_size = os.path.getsize(nc_file)
        if act_size != db_file.size:
            logger.info('File %s has size %d', db_file.name, act_size)
            db_file.online = False
            db_file.directory = None
            db_file.save()

            os.remove(nc_file)
            if not is_same_gws(nc_file, BASE_OUTPUT_DIR):
                sym_link_path = os.path.join(BASE_OUTPUT_DIR,
                                             construct_drs_path(db_file),
                                             db_file.name)
                try:
                    os.remove(sym_link_path)
                except OSError:
                    logger.error('Unable to delete sym link %s', sym_link_path)


if __name__ == "__main__":
    cmd_args = parse_args()

    # determine the log level
    log_level = getattr(logging, cmd_args.log_level.upper())

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
