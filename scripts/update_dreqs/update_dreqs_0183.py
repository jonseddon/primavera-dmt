#!/usr/bin/env python
"""
update_dreqs_0183.py

ESGF AttributeUpdate
Called from a Rose suite to update the checksums in MPI AMIP files that have
already been updated with the first version that didn't preserve checksums.
"""
from __future__ import (unicode_literals, division, absolute_import,
                        print_function)
import argparse
import logging.config
import os
import sys

import django
django.setup()

from pdata_app.models import Checksum, DataRequest, TapeChecksum
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
    parser.add_argument('request_id', help='to request id to update')
    parser.add_argument('--version', action='version',
        version='%(prog)s {}'.format(__version__))
    args = parser.parse_args()

    return args


def main(args):
    """
    Main entry point
    """
    model, expt, var_lab, table, var = args.request_id.split('_')

    if model == 'MPIESM-1-2-HR':
        new_model = 'MPI-ESM1-2-HR'
    elif model == 'MPIESM-1-2-XR':
        new_model = 'MPI-ESM1-2-XR'
    else:
        raise ValueError('Unknown source_id {}'.format(model))

    dreq = DataRequest.objects.get(
        climate_model__short_name=new_model,
        experiment__short_name=expt,
        rip_code=var_lab,
        variable_request__table_name=table,
        variable_request__cmor_name=var
    )
    logger.debug('DataRequest is {}'.format(dreq))

    for data_file in dreq.datafile_set.order_by('name'):
        logger.debug('Processing {}'.format(data_file.name))
        file_path = os.path.join(data_file.directory, data_file.name)

        cs = data_file.checksum_set.first()
        if not cs:
            logger.error('No checksum for {}'.format(data_file.name))
        else:
            TapeChecksum.objects.create(
                data_file=data_file,
                checksum_value=cs.checksum_value,
                checksum_type=cs.checksum_type
            )
            # Remove the original checksum now that the tape checksum's
            # been created
            cs.delete()
        Checksum.objects.create(
            data_file=data_file,
            checksum_type='ADLER32',
            checksum_value=adler32(file_path)
        )
        # Update the file's size
        data_file.tape_size = data_file.size
        data_file.size = os.path.getsize(file_path)
        # Save all of the changes
        data_file.save()


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
