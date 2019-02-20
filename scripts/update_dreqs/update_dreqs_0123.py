#!/usr/bin/env python
"""
update_dreqs_0123.py

This file runs on mass-cli1 and for HadGEM3-GC31-HM control-1950 r1i1p1f1 Amon
in 1967 it reads the correct checksum for a file from MASS and applies this to
the database.
"""
from __future__ import (unicode_literals, division, absolute_import,
                        print_function)
import argparse
import logging.config
import sys
import subprocess
import xml.etree.ElementTree as ET

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
    data_files = DataFile.objects.filter(
        climate_model__short_name='HadGEM3-GC31-HM',
        experiment__short_name='control-1950',
        rip_code='r1i1p1f1',
        variable_request__table_name='Amon',
        name__contains='196701-196712'
    )

    logger.debug('Found {} files'.format(data_files.count()))

    for df in data_files:
        cmd  = 'moo ls -mx {}/{}'.format(df.tape_url, df.name)
        co = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE)
        root = ET.fromstring(co.stdout)
        actual_cs = root.findall('./node/checksum/value')[0].text
        curr_cs_obj = df.checksum_set.first()
        if curr_cs_obj.checksum_value == actual_cs:
            logger.debug('Skipping {} as checksums already match'.
                         format(df.name))
        else:
            curr_cs_obj.checksum_value = actual_cs
            curr_cs_obj.save()
            logger.debug('Updated {}'.format(df.name))

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
