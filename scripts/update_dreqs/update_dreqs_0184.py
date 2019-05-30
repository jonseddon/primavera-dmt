#!/usr/bin/env python
"""
update_dreqs_0184.py

Loop through the files that have been published to the ESGF and which are now
available in the ESGF. For each file check that its status in the DMT is
online, that its path is correct and that it is correctly symbolically linked
from the base output directory.
"""
from __future__ import (unicode_literals, division, absolute_import,
                        print_function)
import argparse
import logging.config
import os
import sys

import django
django.setup()

from pdata_app.models import ESGFDataset, Settings
from pdata_app.utils.common import construct_drs_path


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
    parser.add_argument('request_id', help='to request id to update')
    parser.add_argument('--version', action='version',
        version='%(prog)s {}'.format(__version__))
    args = parser.parse_args()

    return args


def main(args):
    """
    Main entry point
    """
    base_output_dir = Settings.get_solo().base_output_dir
    ceda_base = '/badc/cmip6/data'

    for ed in ESGFDataset.objects.filter(status='PUBLISHED'):
        for df in ed.data_request.datafile_set.order_by('name'):
            ceda_dir = os.path.join(ceda_base, construct_drs_path(df))
            ceda_path = os.path.join(ceda_dir, df.name)
            if df.directory:
                logger.error('Directory given {}'.
                    format(os.path.join(df.directory, df.name)))
            if os.path.exists(ceda_path):
                df.online = True
                df.directory = ceda_dir
                df.save()
            else:
                logger.error('Not in archive {}'.format(ceda_path))
                continue
            base_dir = os.path.join(base_output_dir, construct_drs_path(df))
            base_path = os.path.join(base_dir, df.name)
            if os.path.exists(base_path):
                os.remove(base_path)
                logger.error('Deleted {}'.format(base_path))
            if not os.path.exists(base_dir):
                os.makedirs(base_dir)
            os.symlink(ceda_path, base_path)


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
