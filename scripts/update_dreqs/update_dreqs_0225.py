#!/usr/bin/env python
"""
update_dreqs_0225.py

This file removes all files for the EC-Earth highres-future r1i1p1f1 variables:

EC-Earth3P Amon hfss, hfls
EC-Earth3P-HR day hfss, hfls
EC-Earth3P-HR 3hr hfss, hfls
"""
import argparse
import logging.config
import sys

import django
django.setup()
from pdata_app.utils.replace_file import replace_files
from pdata_app.models import DataFile, Settings
from pdata_app.utils.common import delete_files

__version__ = '0.1.0b1'

DEFAULT_LOG_LEVEL = logging.WARNING
DEFAULT_LOG_FORMAT = '%(levelname)s: %(message)s'

logger = logging.getLogger(__name__)

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
    lr = DataFile.objects.filter(
        climate_model__short_name='EC-Earth3P',
        experiment__short_name='highres-future',
        rip_code='r1i1p1f1',
        variable_request__table_name='Amon',
        variable_request__cmor_name__in=['hfls', 'hfss']
    )
    if lr.count() != 840:
        raise ValueError('Not 420 LR files found')

    hr_3hr = DataFile.objects.filter(
        climate_model__short_name='EC-Earth3P-HR',
        experiment__short_name='highres-future',
        rip_code='r1i1p1f1',
        variable_request__table_name='3hr',
        variable_request__cmor_name__in=['hfls', 'hfss']
    )
    if hr_3hr.count() != 864:
        raise ValueError('Not 420 HR 3hr files found')

    hr_day = DataFile.objects.filter(
        climate_model__short_name='EC-Earth3P-HR',
        experiment__short_name='highres-future',
        rip_code='r1i1p1f1',
        variable_request__table_name='day',
        variable_request__cmor_name__in=['hfls', 'hfss']
    )
    if hr_day.count() != 864:
        raise ValueError('Not 420 HR day files found')

    affected_files = lr | hr_3hr | hr_day

    logger.debug('{} affected files found'.format(affected_files.count()))

    delete_files(affected_files, BASE_OUTPUT_DIR)
    replace_files(affected_files)


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
