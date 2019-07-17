#!/usr/bin/env python
"""
update_dreqs_0203.py

Correct the version in some MPI files. Move the files and update the sym links
if online.
"""
import argparse
import logging.config
import os
import sys

from cf_units import date2num, CALENDAR_GREGORIAN

import django
django.setup()
from pdata_app.models import DataFile
from pdata_app.utils.common import (construct_drs_path, delete_drs_dir,
                                    get_gws, is_same_gws)

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
    dfs = DataFile.objects.filter(
        climate_model__short_name='MPI-ESM1-2-XR',
        experiment__short_name='highres-future',
        version='v20190617'
    )

    prim_gws = '/gws/nopw/j04/primavera5/stream1'

    old_dirs = []

    for df in dfs:
        old_drs_path = construct_drs_path(df)
        df.version = 'v20190517'
        df.save()
        if df.online:
            # file itself
            gws = get_gws(df.directory)
            old_dir = df.directory
            new_dir = os.path.join(gws, construct_drs_path(df))
            if not os.path.exists(new_dir):
                os.makedirs(new_dir)
            os.rename(os.path.join(df.directory, df.name),
                      os.path.join(new_dir, df.name))
            df.directory = new_dir
            df.save()
            if old_dir not in old_dirs:
                old_dirs.append(old_dir)

            # sym link
            if not is_same_gws(df.directory, prim_gws):
                old_sym_dir = os.path.join(prim_gws, old_drs_path)
                old_sym = os.path.join(old_sym_dir, df.name)
                if os.path.exists(old_sym):
                    if os.path.islink(old_sym):
                        os.remove(old_sym)
                    else:
                        logger.warning(f'Not symlink as expected: {old_sym}')
                new_sym_dir = os.path.join(prim_gws, construct_drs_path(df))
                if not os.path.exists(new_sym_dir):
                    os.makedirs(new_sym_dir)
                os.symlink(os.path.join(new_dir, df.name),
                           os.path.join(new_sym_dir, df.name))
                if old_sym_dir not in old_dirs:
                    old_dirs.append(old_sym_dir)

    logger.debug(f'Removing {len(old_dirs)} old dirs')
    for old_dir in old_dirs:
        delete_drs_dir(old_dir)


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
