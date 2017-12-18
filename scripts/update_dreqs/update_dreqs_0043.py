#!/usr/bin/env python2.7
"""
update_dreqs_0043.py

This script updates the version string on the specified data submissions. These
are files that have replaced earlier files. The default string for that
experiment was used, when a more recent version string should have been used.

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
    new_version = 'v20171218'

    data_submissions = [
        '/group_workspaces/jasmin2/primavera4/upload/EC-Earth-Consortium/'
        'EC-Earth-3/incoming/amip-fix-v20171102',
        '/group_workspaces/jasmin2/primavera4/upload/EC-Earth-Consortium/'
        'EC-Earth-3/incoming/amip-fix-v20171103',
        '/group_workspaces/jasmin2/primavera4/upload/EC-Earth-Consortium/'
        'EC-Earth-3/incoming/amip-fix-v20171104',
        '/group_workspaces/jasmin2/primavera4/upload/EC-Earth-Consortium/'
        'EC-Earth-3-HR/incoming/amip-fix-v20171107',
        '/group_workspaces/jasmin2/primavera4/upload/EC-Earth-Consortium/'
        'EC-Earth-3-HR/incoming/amip-fix-v20171109',
        '/group_workspaces/jasmin2/primavera4/upload/EC-Earth-Consortium/'
        'EC-Earth-3-HR/incoming/amip-fix-v20171110',
        '/group_workspaces/jasmin2/primavera4/upload/EC-Earth-Consortium/'
        'EC-Earth-3-HR/incoming/amip-fix-v20171111',
        '/group_workspaces/jasmin2/primavera4/upload/EC-Earth-Consortium/'
        'EC-Earth-3-HR/incoming/amip-fix-v20171119',
    ]

    num_data_subs = 0

    for data_sub_dir in data_submissions:
        data_sub = DataSubmission.objects.get(incoming_directory=data_sub_dir)
        logger.debug('{}: {} files'.format(os.path.basename(data_sub_dir),
                                           data_sub.datafile_set.count()))
        num_data_subs += 1

        for data_file in data_sub.datafile_set.all():
            data_file.version = new_version
            data_file.save()

    logger.debug('{} data submissions updated'.format(num_data_subs))


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
