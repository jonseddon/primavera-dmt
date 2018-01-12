#!/usr/bin/env python2.7
"""
update_dreqs_0049.py

This script removes flux files from the spinup-1950 EC-Earth3-HR from v20171024
to v20171116 for variables that have had replacement files produced for them.

'rlus_3hr',
'rsus_3hr',
'rsuscs_3hr',
'rlut_E3hr',
'rlutcs_E3hr',
'rsut_E3hr',
'rlus_day',
'rlut_day',
'rsus_day',
'rlutcs_CFday',
'rsuscs_CFday',
'rsut_CFday',
'rsutcs_CFday',
'rlus_Amon',
'rlut_Amon',
'rlutcs_Amon',
'rsus_Amon',
'rsuscs_Amon',
'rsut_Amon',
'rsutcs_Amon'
"""
import argparse
import logging.config
import os
import sys

from cf_units import date2num, CALENDAR_GREGORIAN

import django
django.setup()
from pdata_app.utils.replace_file import replace_file
from pdata_app.models import DataFile, ReplacedFile
from pdata_app.utils.common import delete_drs_dir

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
    var_tables = [
        'rlus_3hr',
        'rsus_3hr',
        'rsuscs_3hr',
        'rlut_E3hr',
        'rlutcs_E3hr',
        'rsut_E3hr',
        'rlus_day',
        'rlut_day',
        'rsus_day',
        'rlutcs_CFday',
        'rsuscs_CFday',
        'rsut_CFday',
        'rsutcs_CFday',
        'rlus_Amon',
        'rlut_Amon',
        'rlutcs_Amon',
        'rsus_Amon',
        'rsuscs_Amon',
        'rsut_Amon',
        'rsutcs_Amon'
    ]

    submissions = [
        u'/group_workspaces/jasmin2/primavera4/upload/EC-Earth-Consortium/EC-Earth-3-HR/incoming/v20171110',
        u'/group_workspaces/jasmin2/primavera4/upload/EC-Earth-Consortium/EC-Earth-3-HR/incoming/v20171111',
        u'/group_workspaces/jasmin2/primavera4/upload/EC-Earth-Consortium/EC-Earth-3-HR/incoming/v20171112',
        u'/group_workspaces/jasmin2/primavera4/upload/EC-Earth-Consortium/EC-Earth-3-HR/incoming/v20171116',
        u'/group_workspaces/jasmin2/primavera4/upload/EC-Earth-Consortium/EC-Earth-3-HR/incoming/v20171024',
        u'/group_workspaces/jasmin2/primavera4/upload/EC-Earth-Consortium/EC-Earth-3-HR/incoming/v20171027',
        u'/group_workspaces/jasmin2/primavera4/upload/EC-Earth-Consortium/EC-Earth-3-HR/incoming/v20171101',
        u'/group_workspaces/jasmin2/primavera4/upload/EC-Earth-Consortium/EC-Earth-3-HR/incoming/v20171114',
        u'/group_workspaces/jasmin2/primavera4/upload/EC-Earth-Consortium/EC-Earth-3-HR/incoming/v20171115',
        u'/group_workspaces/jasmin2/primavera4/upload/EC-Earth-Consortium/EC-Earth-3-HR/incoming/v20171113'
    ]

    models = ['EC-Earth3-HR']
    experiment = 'spinup-1950'

    for var_table in var_tables:
        var, __, table = var_table.partition('_')
        for model in models:
            query_set = DataFile.objects.filter(
                data_request__climate_model__short_name=model,
                data_request__experiment__short_name=experiment,
                variable_request__table_name=table,
                variable_request__cmor_name=var,
                data_submission__incoming_directory__in=submissions
            )
            logger.debug('{} {} {} {}'.format(model, table, var,
                                              query_set.count()))

            directories_found = []
            for df in query_set:
                if df.online:
                    try:
                        os.remove(os.path.join(df.directory, df.name))
                    except OSError as exc:
                        logger.error(str(exc))
                        sys.exit(1)
                    else:
                        if df.directory not in directories_found:
                            directories_found.append(df.directory)
                    df.online = False
                    df.directory = None
                    df.save()

            for directory in directories_found:
                if not os.listdir(directory):
                    delete_drs_dir(directory)

            replace_file(query_set)


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
