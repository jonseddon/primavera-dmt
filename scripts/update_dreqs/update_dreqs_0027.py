#!/usr/bin/env python2.7
"""
update_dreqs_0027.py

Delete existing data that was set-up using incoming_to_drs.py from known
submissions.
"""
import argparse
import logging.config
import os
import sys

import django
django.setup()

from pdata_app.models import DataSubmission


__version__ = '0.1.0b1'

DEFAULT_LOG_LEVEL = logging.WARNING
DEFAULT_LOG_FORMAT = '%(levelname)s: %(message)s'

logger = logging.getLogger(__name__)


def delete_submission_files(incoming_directory, description):
    """
    Delete all files from the data submission specified by its
    incoming_directory.

    :param str incoming_directory: The incoming_directory of the submission to
        delete all files from.
    :param str description: A description of the submission to include in the
        debug messages.
    """
    data_sub = DataSubmission.objects.get(
        incoming_directory=incoming_directory
    )

    all_submission_files = data_sub.datafile_set.all()

    for df in all_submission_files:
        df.directory = None
        df.online = False
        df.save()
        try:
            os.remove(os.path.join(df.directory, df.name))
        except OSError:
            logger.error('Unable to delete from {} {}'.format(
                description,
                os.path.join(df.directory, df.name
            )))
        else:
            logger.debug('{} os.remove {}'.format(
                description,
                os.path.join(df.directory, df.name))
            )


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

    submissions_to_delete = [
        {'directory': '/group_workspaces/jasmin2/primavera4/upload/'
                      'CNRM-CERFACS/CNRM-CM6-1/incoming',
         'description': 'CERFACS LR'},
        {'directory': '/group_workspaces/jasmin2/primavera3/upload/'
                      'CNRM-CERFACS/CNRM-CM6-1-HR/incoming/v20170623_2000',
         'description': 'CERFACS 2000'},
        {'directory': '/group_workspaces/jasmin2/primavera4/upload/'
                      'CNRM-CERFACS/CNRM-CM6-1-HR/incoming/v20170518_1970',
         'description': 'CERFACS 1970'},
        {'directory': '/group_workspaces/jasmin2/primavera4/upload/'
                      'CNRM-CERFACS/CNRM-CM6-1-HR/incoming/v20170518_1960',
         'description': 'CERFACS 1960'},
        {'directory': '/group_workspaces/jasmin2/primavera4/upload/'
                      'CNRM-CERFACS/CNRM-CM6-1-HR/incoming/v20170518_1950',
         'description': 'CERFACS 1950'},
        {'directory': '/group_workspaces/jasmin2/primavera4/upload/CMCC/'
                      '20170703',
         'description': 'CMCC 20170703'},
        {'directory': '/group_workspaces/jasmin2/primavera4/upload/CMCC/'
                      '20170708',
         'description': 'CMCC 20170708'},
        {'directory': '/group_workspaces/jasmin2/primavera4/upload/CMCC/'
                      '20170725',
         'description': 'CMCC 20170725'},
        {'directory': '/group_workspaces/jasmin2/primavera4/upload/CMCC/'
                      'CMCC-VHR4/20170927',
         'description': 'CMCC VHR'},
    ]

    for submission in submissions_to_delete:
        delete_submission_files(submission['directory'],
                                submission['description'])


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
