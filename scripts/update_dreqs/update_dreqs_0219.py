#!/usr/bin/env python
"""
update_dreqs_0219.py

Delete from disk and its symbolic links the MPI AMIP data that is about to be
retracted.
"""
import argparse
import logging.config
import os
import sys

import django
django.setup()
from pdata_app.models import DataRequest, Settings
from pdata_app.utils.common import (construct_drs_path, delete_drs_dir,
                                    delete_files)

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
    dreqs = DataRequest.objects.filter(
        institute__short_name='MPI-M',
        experiment__short_name='highresSST-present'
    )

    logger.debug(f'Found {dreqs.count()} datasets')

    for dreq in dreqs:
        if dreq.esgfdataset_set.all():
            # ESGF dataset's been created...
            esgf = dreq.esgfdataset_set.first()
            if esgf.status == 'PUBLISHED':
                # ... and published so the data's in the CEDA archive
                # and symlinked from the PRIMAVERA data structure
                # All sym links will be in one directory
                set_dir = os.path.join(
                    BASE_OUTPUT_DIR,
                    construct_drs_path(dreq.datafile_set.first())
                )
                for df in dreq.datafile_set.all():
                    file_path = os.path.join(set_dir, df.name)
                    if not os.path.islink(file_path):
                        logger.warning(f'Expected a sym link {file_path}')
                        continue
                    try:
                        os.remove(file_path)
                    except OSError as exc:
                        logger.error(str(exc))
                    df.online = False
                    df.directory = None
                    df.save()
                delete_drs_dir(set_dir)
                logger.debug(f'Removed files for ESGFDataset {esgf}')
                esgf.status = 'CREATED'
                esgf.save()
                continue
        # The data's not been published so delete the files and their sym links
        delete_files(dreq.datafile_set.all(), BASE_OUTPUT_DIR)
        logger.debug(f'Removed files for DataRequest {dreq}')
        dreq.datafile_set.update(directory=None, online=False)


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
