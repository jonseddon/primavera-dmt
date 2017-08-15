#!/usr/bin/env python2.7
"""
update_dreqs_0017.py

This script is run to change the version string in all files from the specified
EC-Earth submissions to one value.
"""
import argparse
import logging.config
import os
import sys

from cf_units import date2num, CALENDAR_GREGORIAN

import django
django.setup()

from pdata_app.models import DataSubmission

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
    ECEARTH_NEW_VERSION = 'v20170811'
    CERFACS_NEW_VERSION = 'v20170622'

    wrong_versions = [
        u'/group_workspaces/jasmin2/primavera4/upload/EC-Earth-Consortium/EC-Earth-3-HR/incoming/v20170607',
        u'/group_workspaces/jasmin2/primavera4/upload/EC-Earth-Consortium/EC-Earth-3-HR/incoming/v20170608',
        u'/group_workspaces/jasmin2/primavera4/upload/EC-Earth-Consortium/EC-Earth-3-HR/incoming/v20170609',
        u'/group_workspaces/jasmin2/primavera4/upload/EC-Earth-Consortium/EC-Earth-3-HR/incoming/v20170610',
        u'/group_workspaces/jasmin2/primavera4/upload/EC-Earth-Consortium/EC-Earth-3-HR/incoming/v20170611',
        u'/group_workspaces/jasmin2/primavera4/upload/EC-Earth-Consortium/EC-Earth-3-HR/incoming/v20170612',
        u'/group_workspaces/jasmin2/primavera4/upload/EC-Earth-Consortium/EC-Earth-3-HR/incoming/v20170616',
        u'/group_workspaces/jasmin2/primavera4/upload/EC-Earth-Consortium/EC-Earth-3-HR/incoming/v20170619',
        u'/group_workspaces/jasmin2/primavera4/upload/EC-Earth-Consortium/EC-Earth-3-HR/incoming/v20170621',
        u'/group_workspaces/jasmin2/primavera4/upload/EC-Earth-Consortium/EC-Earth-3-HR/incoming/v20170622',
        u'/group_workspaces/jasmin2/primavera4/upload/EC-Earth-Consortium/EC-Earth-3-HR/incoming/v20170626',
        u'/group_workspaces/jasmin2/primavera4/upload/EC-Earth-Consortium/EC-Earth-3-HR/incoming/v20170628'
    ]

    for incom_dir in wrong_versions:
        logger.debug(incom_dir)
        data_sub = DataSubmission.objects.get(incoming_directory=incom_dir)
        for data_file in data_sub.datafile_set.all():
            data_file.version = ECEARTH_NEW_VERSION
            data_file.save()

    wrong_versions = [
        u'/group_workspaces/jasmin2/primavera4/upload/CNRM-CERFACS/CNRM-CM6-1-HR/incoming/v20170518_1950',
        u'/group_workspaces/jasmin2/primavera4/upload/CNRM-CERFACS/CNRM-CM6-1-HR/incoming/v20170518_1960',
        u'/group_workspaces/jasmin2/primavera4/upload/CNRM-CERFACS/CNRM-CM6-1-HR/incoming/v20170518_1970'
    ]

    for incom_dir in wrong_versions:
        logger.debug(incom_dir)
        data_sub = DataSubmission.objects.get(incoming_directory=incom_dir)
        for data_file in data_sub.datafile_set.all():
            old_dir = data_file.directory
            old_path = os.path.join(old_dir, data_file.name)
            new_dir = os.path.join(os.path.dirname(data_file.directory),
                                    CERFACS_NEW_VERSION)
            new_path = os.path.join(new_dir, data_file.name)

            if not os.path.exists(new_dir):
                os.makedirs(new_dir)

            try:
                os.rename(old_path, new_path)
            except OSError:
                logger.error(old_path)
                logger.error(new_path)
                raise


            if not os.listdir(old_dir):
                # directory's empty so delete it
                os.rmdir(old_dir)

            data_file.version = CERFACS_NEW_VERSION
            data_file.directory = new_dir
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
