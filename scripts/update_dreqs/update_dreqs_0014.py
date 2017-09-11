#!/usr/bin/env python2.7
"""
update_dreqs_0014.py

This script is run to fix the broken JSON files for u-ao80_u-ai674_*.json for
HadGEM3-GC31-LL highresSST-present data. It loads each JSON file and replaces
the null values with the correct dictionary objects. 
"""
import argparse
import json
import logging.config
import os
import sys

from cf_units import date2num, CALENDAR_360_DAY

import django
django.setup()

from pdata_app.utils.common import ilist_files
from pdata_app.utils.dbapi import match_one
from pdata_app.models import VariableRequest

__version__ = '0.1.0b1'

DEFAULT_LOG_LEVEL = logging.WARNING
DEFAULT_LOG_FORMAT = '%(levelname)s: %(message)s'

logger = logging.getLogger(__name__)

INPUT_JSON_DIR = '/home/h04/jseddon/primavera/validation/u-ao802/u-ai674'
OUTPUT_JSON_DIR = '/home/h04/jseddon/primavera/validation/u-ao802/fixed_u-ai674'

def _get_cmor_name(var_name, table_name):
    """
    Check the variable request to find the actual CMOR name from a variable
    name.
    
    :param str var_name: The variable name from the first component of the
        file name.
    :param str table_name: The table name. 
    :return: The CMOR name
    :rtype: str
    """
    var_match = match_one(VariableRequest, cmor_name=var_name,
                          table_name=table_name)
    if var_match:
        return var_name
    else:
        # if cmor_name doesn't match then it may be a variable where out_name
        # is different to cmor_name so check these
        var_matches = VariableRequest.objects.filter(
            var_name=var_name, table_name=table_name)
        if var_matches.count() == 1:
            return var_matches[0].cmor_name
        elif var_matches.count() == 0:
            msg = ('No variable request found for variable {} {}.'.
                   format(table_name, var_name))
            logger.error(msg)
        else:
            if table_name == 'E3hrPt':
                if var_name == 'ua':
                    return 'ua7h'
                elif var_name == 'va':
                    return 'va7h'
                else:
                    msg = 'Please hard code a mapping for {} {}'.format(
                        table_name, var_name)
                    logger.error(msg)
            else:
                msg = 'Please hard code a mapping for {} {}'.format(
                    table_name, var_name)
                logger.error(msg)


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
    for json_file in ilist_files(INPUT_JSON_DIR, '.json'):
        with open(json_file, 'r') as fh:
            metadata = json.load(fh)

        for nc_file in metadata:
            nc_file['activity_id'] = {
                "__module__": "pdata_app.models",
                "__kwargs__": {
                    "short_name": "HighResMIP"
                },
                "__class__": "ActivityId"
            }

            nc_file['experiment'] = {
                "__module__": "pdata_app.models",
                "__kwargs__": {
                    "short_name": "highresSST-present"
                },
                "__class__": "Experiment"
            }

            nc_file['climate_model'] = {
                "__module__": "pdata_app.models",
                "__kwargs__": {
                    "short_name": "HadGEM3-GC31-LM"
                },
                "__class__": "ClimateModel"
            }

            nc_file['institute'] = {
                "__module__": "pdata_app.models",
                "__kwargs__": {
                    "short_name": "MOHC"
                },
                "__class__": "Institute"
            }

            filename = nc_file['basename']
            var_name, table_name = filename.split('_')[0:2]

            if table_name.startswith('Prim'):
                nc_file['project'] = {
                    "__module__": "pdata_app.models",
                    "__kwargs__": {
                        "short_name": "PRIMAVERA"
                    },
                    "__class__": "Project"
                }
            else:
                nc_file['project'] = {
                    "__module__": "pdata_app.models",
                    "__kwargs__": {
                        "short_name": "CMIP6"
                    },
                    "__class__": "Project"
                }

            cmor_name = _get_cmor_name(var_name, table_name)

            nc_file['variable'] = {
                "__module__": "pdata_app.models",
                "__kwargs__": {
                    "cmor_name": cmor_name,
                    "table_name": table_name
                },
                "__class__": "VariableRequest"
            }

            nc_file['data_request'] = {
                "__module__": "pdata_app.models",
                "__kwargs__": {
                    "variable_request__cmor_name": cmor_name,
                    "variable_request__table_name": table_name,
                    "climate_model__short_name": "HadGEM3-GC31-LM",
                    "experiment__short_name": "highresSST-present",
                    "institute__short_name": "MOHC"
                },
                "__class__": "DataRequest"
            }

        with open(os.path.join(OUTPUT_JSON_DIR,
                               os.path.basename(json_file)), 'w') as fh:
            json.dump(metadata, fh, indent=4)


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
