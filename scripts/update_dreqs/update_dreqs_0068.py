#!/usr/bin/env python2.7
"""
update_dreqs_0068.py

This file adds a variant_label to every entry in a JSON file output by
validate_data_submission.py. The new variant_label and the file to
modify are specified on the command line.
"""
import argparse
import json
import logging.config
import shutil
import sys

import django
django.setup()


__version__ = '0.1.0b1'

DEFAULT_LOG_LEVEL = logging.WARNING
DEFAULT_LOG_FORMAT = '%(levelname)s: %(message)s'

logger = logging.getLogger(__name__)


def parse_args():
    """
    Parse command-line arguments
    """
    parser = argparse.ArgumentParser(description='Add variant_label to JSON '
                                                 'file.')
    parser.add_argument('variant_label', help='the variant_label to add')
    parser.add_argument('json_file', help='the path of the JSON file to modify')
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
    with open(args.json_file) as fh:
        json_contents = json.load(fh)

    for datafile in json_contents:
        datafile['data_request']['__kwargs__']['rip_code'] = args.variant_label

    shutil.move(args.json_file, args.json_file + '.orig')

    with open(args.json_file, 'w') as fh:
        json.dump(json_contents, fh, indent=4)


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
