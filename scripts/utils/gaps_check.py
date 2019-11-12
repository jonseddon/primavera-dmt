#!/usr/bin/env python
"""
gaps_check.py

Identify gaps in the files for a specified experiment
"""
import argparse
import logging.config
import sys

import cf_units

import django
django.setup()
from pdata_app.models import DataRequest

__version__ = '0.1.0b1'

DEFAULT_LOG_LEVEL = logging.WARNING
DEFAULT_LOG_FORMAT = '%(levelname)s: %(message)s'

logger = logging.getLogger(__name__)


def identify_gaps_in_files(data_req, file_handle):
    """
    Check the files in the data request to see if there
    appears to be any files missing from this dataset.
    """
    one_day = 1.
    data_files = data_req.datafile_set.order_by('name')
    gap_found = False
    msg = ''
    for index, data_file in enumerate(data_files):
        if index == 0:
            continue
        start_time = cf_units.num2date(data_file.start_time,
                                       data_file.time_units,
                                       data_file.calendar)
        previous_end_time = cf_units.num2date(data_files[index - 1].end_time,
                                              data_files[index - 1].time_units,
                                              data_files[index - 1].calendar)
        difference = start_time - previous_end_time
        if difference.days > one_day:
            gap_found = True
            msg = f'{difference.days} day gap prior to {data_file.name}'
        else:
            # End of gap so report if there was a gap
            if gap_found:
                file_handle.write(msg + '\n')
                gap_found = False

    if gap_found:
        # Gap extends to the end of files so report last message
        file_handle.write(msg + '\n')


def parse_args():
    """
    Parse command-line arguments
    """
    parser = argparse.ArgumentParser(description='Add additional data requests')
    parser.add_argument('-l', '--log-level', help='set logging level to one of '
        'debug, info, warn (the default), or error')
    parser.add_argument('--version', action='version',
        version='%(prog)s {}'.format(__version__))
    parser.add_argument(
        'experiment_id',
        help="the experiment's id in the form "
             "source-id_experiment-id_variant-id"
    )
    args = parser.parse_args()

    return args


def main(args):
    """
    Main entry point
    """
    source_id, experiment_id, variant_label = args.experiment_id.split('_')

    data_reqs = DataRequest.objects.filter(
        climate_model__short_name=source_id,
        experiment__short_name=experiment_id,
        rip_code=variant_label
    ).order_by(
        'variable_request__table_name',
        'variable_request__cmor_name'
    )

    if not data_reqs.count():
        logger.error(f'No data requests found for {args.experiment_id}')
        sys.exit(1)

    file_list_name = f'{source_id}_{experiment_id}_{variant_label}.txt'
    logger.debug(f'Writing {file_list_name}')
    with open(file_list_name, 'w') as fh:
        for data_req in data_reqs:
            for df in data_req.datafile_set.order_by('name'):
                fh.write(df.name + '\n')

    gaps_name = f'{source_id}_{experiment_id}_{variant_label}_gaps.txt'
    logger.debug(f'Writing {gaps_name}')
    with open(gaps_name, 'w') as fh:
        for data_req in data_reqs:
            identify_gaps_in_files(data_req, fh)


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
