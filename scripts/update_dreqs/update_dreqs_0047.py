#!/usr/bin/env python2.7
"""
update_dreqs_0047.py

This file updates the version number of the added agessc files to match the
rest of the data fro that model and experiment.
"""
import argparse
import logging.config
import os
import sys


import django
django.setup()

from pdata_app.models import DataFile
from pdata_app.utils.common import adler32


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
    versions = {
        'agessc_Omon_HadGEM3-GC31-LL_hist-1950_r1i1p1f1_gn_196401-196412.nc':
            'v20170921',
        'agessc_Omon_HadGEM3-GC31-HM_hist-1950_r1i1p1f1_gn_196001-196012.nc':
            'v20171024',
        'agessc_Omon_HadGEM3-GC31-LL_control-1950_r1i1p1f1_gn_197101-197112.nc':
            'v20170927',
        'agessc_Omon_HadGEM3-GC31-MM_control-1950_r1i1p1f1_gn_199101-199112.nc':
            'v20171010',
        'agessc_Omon_HadGEM3-GC31-HM_control-1950_r1i1p1f1_gn_196001-196012.nc':
            'v20171025'
    }

    for filename in versions:
        df = DataFile.objects.get(name=filename)
        if not df:
            raise ValueError('Cannot find in the database {}'.format(filename))

        logger.debug('{} Changing from {} to {}'.format(filename, df.version,
                                                        versions[filename]))
        df.version = versions[filename]
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
