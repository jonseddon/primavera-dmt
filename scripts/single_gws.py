#!/usr/bin/env python
"""
single_gws.py

This script is used to identify the directories that a data request is spread
over. The script can also be used to move all of the files into a single
directory.
"""
from __future__ import print_function
import argparse
import logging.config
import os
import shutil
import sys

import django
django.setup()
from django.template.defaultfilters import filesizeformat

from pdata_app.models import DataRequest, Settings
from pdata_app.utils.common import (adler32, construct_drs_path,
                                    delete_drs_dir, is_same_gws,
                                    directories_spanned)

__version__ = '0.1.0b'

DEFAULT_LOG_LEVEL = logging.WARNING
DEFAULT_LOG_FORMAT = '%(levelname)s: %(message)s'

logger = logging.getLogger(__name__)

# The common prefix of all of our GWSs
COMMON_GWS_NAME = '/gws/nopw/j04/primavera'
# The top-level directory to write output data to
BASE_OUTPUT_DIR = Settings.get_solo().base_output_dir


def move_dirs(data_req, new_gws):
    """
    Move the files

    :param pdata_app.models.DataRequest data_req: the data request to move
    :param int new_gws: the number of the gws to move to
    """
    single_dir = '{}{}'.format(COMMON_GWS_NAME, new_gws)
    existing_dirs = data_req.directories()
    # ignore data that is offline
    if None in existing_dirs:
        existing_dirs.remove(None)
    use_single_dir = False
    for exist_dir in existing_dirs:
        if exist_dir.startswith(single_dir):
            use_single_dir = True
            break
    if not use_single_dir:
        # As a quick sanity check, generate an error if there is no
        # data already in the requested output directory
        logger.error('The new output directory is {} but no data from '
                     'this variable is currently in this directory.'.
                     format(single_dir))
        sys.exit(1)

    for exist_dir in existing_dirs:
        if exist_dir.startswith(single_dir):
            continue
        files_to_move = data_req.datafile_set.filter(directory=exist_dir)
        logger.debug('Moving {} files from {}'.format(
            files_to_move.count(), exist_dir))
        for file_to_move in files_to_move:
            # Move the file
            src = os.path.join(exist_dir, file_to_move.name)
            dest_path = os.path.join(single_dir, 'stream1',
                                     construct_drs_path(file_to_move))
            if not os.path.exists(dest_path):
                os.makedirs(dest_path)
            dest = os.path.join(dest_path, file_to_move.name)
            # remove existing link if about to write over it
            if dest.startswith(BASE_OUTPUT_DIR):
                if os.path.exists(dest):
                    if os.path.islink(dest):
                        os.remove(dest)
            # Move the file
            shutil.move(src, dest)
            # Update the file's location in the DB
            file_to_move.directory = dest_path
            file_to_move.save()
            # Check that it was safely copied
            actual_checksum = adler32(dest)
            db_checksum = file_to_move.checksum_set.first().checksum_value
            if not actual_checksum == db_checksum:
                logger.error('For {}\ndatabase checksum: {}\n'
                             'actual checksum: {}'.
                             format(dest, db_checksum, actual_checksum))
                sys.exit(1)
            # Update the symlink
            if not is_same_gws(dest_path, BASE_OUTPUT_DIR):
                primary_path_dir = os.path.join(
                    BASE_OUTPUT_DIR,
                    construct_drs_path(file_to_move))
                primary_path = os.path.join(primary_path_dir,
                                            file_to_move.name)
                if os.path.lexists(primary_path):
                    if not os.path.islink(primary_path):
                        logger.error("{} exists and isn't a symbolic "
                                     "link.".format(primary_path))
                        sys.exit(1)
                    else:
                        # it is a link so remove it
                        os.remove(primary_path)
                if not os.path.exists(primary_path_dir):
                    os.makedirs(primary_path_dir)
                os.symlink(dest, primary_path)

        delete_drs_dir(exist_dir)


def parse_args():
    """
    Parse command-line arguments
    """
    parser = argparse.ArgumentParser(
        description='Move files to a single group workspace.'
    )
    parser.add_argument('request_id', help="the data request's id")
    parser.add_argument('-m', '--move', help="move the data request to this "
                                             "GWS number", type=int)
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

    Data request IDs are in the form:

    <climate-model>_<experiment>_<variant-label>_<table>_<variable>

    e.g.:

    HadGEM3-GC31-LM_highresSST-present_r1i1p1f1_Amon_psl
    """
    (source_id, experiment_id, variant_label, table_id, variable_id) = (
        args.request_id.split('_')
    )
    try:
        data_req = DataRequest.objects.get(
            climate_model__short_name=source_id,
            experiment__short_name=experiment_id,
            rip_code=variant_label,
            variable_request__table_name=table_id,
            variable_request__cmor_name=variable_id
        )
    except django.core.exceptions.ObjectDoesNotExist:
        logger.error('No data requests found for {}'.format(args.request_id))
        sys.exit(1)
    except django.core.exceptions.MultipleObjectsReturned:
        logger.error('Multiple data requests found for {}'.
                     format(args.request_id))
        sys.exit(1)

    if not args.move:
        # print the number of files and their total size
        for exist_dir in directories_spanned(data_req):
            print('{} {} {}'.format(
                exist_dir['dir_name'],
                exist_dir['num_files'],
                filesizeformat(exist_dir['dir_size']).replace('\xa0', ' ')
            ))
    else:
        # move the data
        move_dirs(data_req, args.move)


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
