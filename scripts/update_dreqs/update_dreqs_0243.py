#!/usr/bin/env python
"""
update_dreqs_0243.py

Determine which MPI coupled variables need the external_variables global
attribute setting.
"""
import argparse
import json
import logging.config
import os
import pprint
import sys

import django
django.setup()
from pdata_app.models import DataRequest, VariableRequest

__version__ = '0.1.0b1'

DEFAULT_LOG_LEVEL = logging.WARNING
DEFAULT_LOG_FORMAT = '%(levelname)s: %(message)s'

logger = logging.getLogger(__name__)

TABLES_DIR = '/home/h04/jseddon/primavera/cmip6-cmor-tables/Tables'


def process_cell_measures(var_req, cell_measures, output_dict):
    """
    Add the table and variable name to the appropriate cell measures entry.
    """
    if not cell_measures:
        # If blank then don't do anything.
        return
    # correct for typos in the data request
    if cell_measures == 'area: areacello OR areacella':
        cell_measures = 'area: areacella'
    if cell_measures in output_dict:
        if var_req.table_name in output_dict[cell_measures]:
            if (var_req.cmor_name not in
                    output_dict[cell_measures][var_req.table_name]):
                (output_dict[cell_measures][var_req.table_name].
                 append(var_req.cmor_name))
        else:
            output_dict[cell_measures][var_req.table_name] = [var_req.cmor_name,]
    else:
        output_dict[cell_measures] = {var_req.table_name:[var_req.cmor_name,]}


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
    drs = DataRequest.objects.filter(
        institute__short_name='MPI-M',
        experiment__short_name__in=['control-1950', 'hist-1950',
                                    'spinup-1950'],
        datafile__isnull=False
    ).distinct()

    tables = (drs.values_list('variable_request__table_name', flat=True).
              distinct().order_by('variable_request__table_name'))

    output_dict = {}

    for tab_name in tables:
        if tab_name.startswith('Prim'):
            for dr in (drs.filter(variable_request__table_name=tab_name).
                    order_by('variable_request__cmor_name')):
                cell_measures = (dr.variable_request.cell_measures)
                process_cell_measures(dr.variable_request, cell_measures,
                                      output_dict)
        else:
            json_file = os.path.join(TABLES_DIR, f'CMIP6_{tab_name}.json')
            with open(json_file) as fh:
                mip_table = json.load(fh)
            for dr in (drs.filter(variable_request__table_name=tab_name).
                    order_by('variable_request__cmor_name')):
                try:
                    cell_measures = (mip_table['variable_entry']
                        [dr.variable_request.cmor_name]['cell_measures'])
                except KeyError:
                    continue
                process_cell_measures(dr.variable_request, cell_measures,
                                      output_dict)

    print(pprint.pformat(output_dict))


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
