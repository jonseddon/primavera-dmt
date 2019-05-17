#!/usr/bin/env python
"""
update_dreqs_0181.py

ESGF AttributeUpdate
Called from a Rose suite to update the source_id in MPI AMIP files.
"""
from __future__ import (unicode_literals, division, absolute_import,
                        print_function)
import argparse
import logging.config
import sys

import django
django.setup()

from pdata_app.models import ClimateModel, DataRequest
from pdata_app.utils.attribute_update import SourceIdUpdate


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

    dreq = DataRequest.objects.get(
        climate_model__short_name=model,
        experiment__short_name=expt,
        rip_code=var_lab,
        variable_request__table_name=table,
        variable_request__cmor_name=var
    )
    logger.debug('DataRequest is {}'.format(dreq))

    for data_file in dreq.datafile_set.order_by('name'):
        logger.debug('Processing {}'.format(data_file.name))

        if data_file.climate_model.short_name == 'MPIESM-1-2-HR':
            new_source_id = 'MPI-ESM1-2-HR'
        elif data_file.climate_model.short_name == 'MPIESM-1-2-XR':
            new_source_id = 'MPI-ESM1-2-XR'
        else:
            raise ValueError('Unknown source_id {}'.
                             format(data_file.climate_model.short_name))

        # new_dreq, created = DataRequest.objects.get_or_create(
        #     project=dreq.project,
        #     institute=dreq.institute,
        #     climate_model=ClimateModel.objects.get(short_name=
        #                                            new_source_id),
        #     experiment=dreq.experiment,
        #     variable_request=dreq.variable_request,
        #     rip_code=dreq.rip_code,
        #     request_start_time=dreq.request_start_time,
        #     request_end_time=dreq.request_end_time,
        #     time_units=dreq.time_units,
        #     calendar=dreq.calendar
        # )
        # if created:
        #     logger.debug('Created {}'.format(new_dreq))
        #
        # updater = SourceIdUpdate(data_file, new_source_id)
        # updater.update()


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
