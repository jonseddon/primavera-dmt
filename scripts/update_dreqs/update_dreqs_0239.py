#!/usr/bin/env python
"""
update_dreqs_0239.py

Some retrievals were accidentally restored to primavera5/ rather than
primavera5/stream1/. This moves the files (removing the sym links if required)
and then updates the DMT.
"""
import argparse
import logging.config
import os
import sys

import django
django.setup()
from pdata_app.utils.common import list_files, construct_drs_path
from pdata_app.models import DataFile, Settings


__version__ = '0.1.0b1'

DEFAULT_LOG_LEVEL = logging.WARNING
DEFAULT_LOG_FORMAT = '%(levelname)s: %(message)s'

logger = logging.getLogger(__name__)

BAD_DIR = '/gws/nopw/j04/primavera5/CMIP6/'

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
    bad_files = list_files(BAD_DIR)

    logger.debug(f'{len(bad_files)} files found')

    for bf in bad_files:
        df = DataFile.objects.get(name=os.path.basename(bf))
        new_dir = os.path.join(BASE_OUTPUT_DIR, construct_drs_path(df))
        new_path = os.path.join(new_dir, df.name)
        if not os.path.exists(new_dir):
            os.makedirs(new_dir)
        if os.path.exists(new_path):
            if os.path.islink(new_path):
                os.remove(new_path)
            else:
                logger.error(f'{new_path} is not a link')
                continue
        os.rename(bf, new_path)
        df.directory = new_dir
        df.save()


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
