#!/usr/bin/env python
"""
update_dreqs_0146.py

Remove all HadGEM3-GC31 tasmax, tasmin, epfluxdiv files.
"""
import argparse
import logging.config
import os
import sys

from cf_units import date2num, CALENDAR_GREGORIAN

import django
django.setup()
from pdata_app.utils.replace_file import replace_files
from pdata_app.models import DataFile, ReplacedFile
from pdata_app.utils.common import delete_drs_dir

__version__ = '0.1.0b1'

DEFAULT_LOG_LEVEL = logging.WARNING
DEFAULT_LOG_FORMAT = '%(levelname)s: %(message)s'

logger = logging.getLogger(__name__)


def delete_files(query_set):
    """
    Delete any files online from the specified queryset
    """
    directories_found = []
    for df in query_set.filter(online=True):
        try:
            os.remove(os.path.join(df.directory, df.name))
        except OSError as exc:
            logger.error(str(exc))
        else:
            if df.directory not in directories_found:
                directories_found.append(df.directory)
        df.online = False
        df.directory = None
        df.save()

    for directory in directories_found:
        if not os.listdir(directory):
            delete_drs_dir(directory)
    logger.debug('{} directories removed'.format(len(directories_found)))


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
    tas_files = DataFile.objects.filter(
        climate_model__short_name__startswith='HadGEM3-GC31',
        variable_request__table_name='Amon',
        variable_request__cmor_name__in=['tasmax', 'tasmin']
    )
    epfluxdiv_files = DataFile.objects.filter(
        climate_model__short_name__startswith='HadGEM3-GC31',
        variable_request__cmor_name='epfluxdiv'
    )
    files = tas_files | epfluxdiv_files
    logger.debug('{} affected files found'.format(files.count()))

    delete_files(files)

    # some files have already been replaced and must have their
    # incoming_directory updated to maintain uniqueness.
    for df in files:
        rfs = ReplacedFile.objects.filter(
            name=df.name,
            incoming_directory=df.incoming_directory
        )
        if rfs.count() == 0:
            continue
        for rf in rfs:
            rf.incoming_directory = rf.incoming_directory + '_01'
            rf.save()

    replace_files(files)



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
