#!/usr/bin/env python
"""
high_freq_file_details.py

Output to the specified file the name, size, checksum, version and tape URI of
sub-daily files.
"""
from __future__ import unicode_literals, division, absolute_import

import argparse
import logging.config
import os
import sys

import django
django.setup()
from pdata_app.models import DataFile


__version__ = '0.1.0b1'

DEFAULT_LOG_LEVEL = logging.WARNING
DEFAULT_LOG_FORMAT = '%(levelname)s: %(message)s'

logger = logging.getLogger(__name__)


def parse_args():
    """
    Parse command-line arguments
    """
    parser = argparse.ArgumentParser(description='Write to file the details of '
                                                 'the high-frequency files.')
    parser.add_argument('filepath', help='the file path to write to')
    parser.add_argument('-l', '--log-level', help='set logging level to one of '
                                                  'debug, info, warn (the '
                                                  'default), or error')
    parser.add_argument('--version', action='version',
                        version='%(prog)s {}'.format(__version__))
    args = parser.parse_args()

    return args


def main(args):
    """Run the script"""
    dfs = DataFile.objects.filter(
        variable_request__frequency__in=['6hr', '3hr', '1hr']
    ).order_by(
        'data_request__institute__short_name',
        'data_request__climate_model__short_name',
        'data_request__experiment__short_name',
        'rip_code',
        'variable_request__table_name',
        'variable_request__cmor_name',
        'name'
    )

    with open(args.filepath, 'w') as fh:
        for df in dfs:
            file_path = (os.path.join(df.directory, df.name)
                         if df.online else df.name)
            checksum = df.checksum_set.first()
            if checksum:
                checksum_text = (f'{checksum.checksum_type}:'
                                 f'{checksum.checksum_value}')
            else:
                logger.warning(f'No checksum found for file {df.name}')
            fh.write(f'{file_path} {df.size} {checksum_text} {df.version} '
                     f'{df.tape_url}\n')


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
