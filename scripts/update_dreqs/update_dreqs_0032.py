#!/usr/bin/env python2.7
"""
update_dreqs_0032.py

This file moves a retrieval request from one group workspace to another.
"""
import argparse
import datetime
import logging.config
import os
import shutil
import sys

import cf_units

import django
django.setup()

from pdata_app.models import RetrievalRequest, DataFile
from pdata_app.utils.common import construct_drs_path


__version__ = '0.1.0b1'

DEFAULT_LOG_LEVEL = logging.WARNING
DEFAULT_LOG_FORMAT = '%(levelname)s: %(message)s'

logger = logging.getLogger(__name__)

NEW_BASE_OUTPUT_DIR = '/group_workspaces/jasmin2/primavera2/stream1'


def parse_args():
    """
    Parse command-line arguments
    """
    parser = argparse.ArgumentParser(description='Move retrieval to a '
                                                 'different directory.')
    parser.add_argument('-r', '--retrieval-id', help='the id of the retrieval '
        'to move', type=int)
    parser.add_argument('-d', '--directory', help='the new top-level directory '
        ' for the DRS structure (default: %(default)s)',
        default=NEW_BASE_OUTPUT_DIR)
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
    ret_req = RetrievalRequest.objects.get(id=args.retrieval_id)

    all_files = DataFile.objects.filter(data_request__in=
                                        ret_req.data_request.all())
    time_units = all_files[0].time_units
    calendar = all_files[0].calendar
    start_float = cf_units.date2num(
        datetime.datetime(ret_req.start_year, 1, 1), time_units, calendar
    )
    end_float = cf_units.date2num(
        datetime.datetime(ret_req.end_year + 1, 1, 1), time_units, calendar
    )
    data_files = all_files.filter(start_time__gte=start_float,
                                  end_time__lt=end_float)

    num_files = 0
    for data_file in data_files:
        drs_path = construct_drs_path(data_file)
        dest_dir = os.path.join(NEW_BASE_OUTPUT_DIR, drs_path)
        if dest_dir == data_file.directory:
            logger.warning('Skipping file as already in destination directory '
                           '{}'.format(data_file.name))
            continue
        if not os.path.exists(dest_dir):
            os.makedirs(dest_dir)
        shutil.move(os.path.join(data_file.directory, data_file.name),
                    dest_dir)
        os.symlink(os.path.join(dest_dir, data_file.name),
                   os.path.join(data_file.directory, data_file.name))
        data_file.directory = dest_dir
        data_file.save()
        num_files += 1

    logger.debug('Moved {} files'.format(num_files))


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
