#!/usr/bin/env python
"""
drs_out_name.py

This script is used to update the directory structure so that the variable in
the structure is out_name rather than cmor_name. All files must be online and
in the same directory.
"""
from __future__ import print_function
import argparse
import logging.config
import os
import shutil
import sys

import django
django.setup()

from pdata_app.models import DataRequest, Settings
from pdata_app.utils.common import construct_drs_path, get_gws, is_same_gws

__version__ = '0.1.0b'

DEFAULT_LOG_LEVEL = logging.WARNING
DEFAULT_LOG_FORMAT = '%(levelname)s: %(message)s'

logger = logging.getLogger(__name__)

# The top-level directory to write output data to
BASE_OUTPUT_DIR = Settings.get_solo().base_output_dir


def construct_old_drs_path(data_file):
    """
    Make the CMIP6 DRS directory path for the specified file but using
    out_name.

    :param pdata_app.models.DataFile data_file: the file
    :returns: A string containing the DRS directory structure
    """
    return os.path.join(
        data_file.project.short_name,
        data_file.activity_id.short_name,
        data_file.institute.short_name,
        data_file.climate_model.short_name,
        data_file.experiment.short_name,
        data_file.rip_code,
        data_file.variable_request.table_name,
        data_file.variable_request.cmor_name,
        data_file.grid,
        data_file.version
    )


def parse_args():
    """
    Parse command-line arguments
    """
    parser = argparse.ArgumentParser(
        description='Change DRS directory to out_name'
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

    # Some quick checks that all files are online, in the same directory and
    # that we need to make a change.
    if data_req.online_status() != 'online':
        logger.error('Not all files are online')
        sys.exit(1)
    directories = data_req.directories()
    if None in directories:
        logger.error('None in directories')
        sys.exit(1)
    if len(directories) != 1:
        logger.error('Length of directories is not 1')
        sys.exit(1)
    first_file = data_req.datafile_set.first()
    var_req = first_file.variable_request
    if var_req.out_name is None:
        logger.error('out_name is None. Nothing needs to be done.')
        sys.exit(1)
    if data_req.esgfdataset_set.count():
        esgf = data_req.esgfdataset_set.order_by('-version').first()
        if esgf.status in ['SUBMITTED', 'AT_CEDA', 'PUBLISHED', 'REJECTED']:
            logger.error('Already submitted to CREPP')
            sys.exit(1)

    # Construct the new directory names
    directory = directories[0]
    existing_dir = construct_old_drs_path(first_file)
    if not directory.endswith(existing_dir):
        logger.error(f'Directory does not end with {existing_dir}. '
                     f'It is {directory}')
        sys.exit(1)
    new_drs_dir = os.path.join(get_gws(directory),
                               construct_drs_path(first_file))
    if not os.path.exists(new_drs_dir):
        os.makedirs(new_drs_dir)

    # Set-up for sym links if required
    do_sym_links = False
    if not is_same_gws(BASE_OUTPUT_DIR, directory):
        do_sym_links = True
        sym_link_dir = os.path.join(BASE_OUTPUT_DIR,
                                    construct_old_drs_path(first_file))
        new_sym_link_dir = os.path.join(BASE_OUTPUT_DIR,
                                        construct_drs_path(first_file))
        if not os.path.exists(new_sym_link_dir):
            os.makedirs(new_sym_link_dir)

    logger.debug('All checks complete. Starting to move files.')

    # Move the files
    for data_file in data_req.datafile_set.order_by('name'):
        new_path = os.path.join(new_drs_dir, data_file.name)
        shutil.move(
            os.path.join(data_file.directory, data_file.name),
            new_path
        )
        data_file.directory = new_drs_dir
        data_file.save()

        if do_sym_links:
            old_link_path = os.path.join(sym_link_dir, data_file.name)
            if os.path.lexists(old_link_path):
                if os.path.islink(old_link_path):
                    os.remove(old_link_path)
                else:
                    logger.warning(f"{old_link_path} exists but isn't a link")
            os.symlink(
                new_path,
                os.path.join(new_sym_link_dir, data_file.name)
            )

    # Delete empty directories
    if not os.listdir(directory):
        os.rmdir(directory)
    if do_sym_links:
        if not os.listdir(sym_link_dir):
            os.rmdir(sym_link_dir)


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
