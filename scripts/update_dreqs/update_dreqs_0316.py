#!/usr/bin/env python
"""
update_dreqs_0316.py

Update further_info_url in files that require it in PRIMAVERA specific
EC-Earth3P-HR control-1950 r1i1p2f1.
"""
import argparse
import glob
import logging.config
import os
import subprocess
import sys

__version__ = '0.1.0b'

DEFAULT_LOG_LEVEL = logging.WARNING
DEFAULT_LOG_FORMAT = '%(levelname)s: %(message)s'

logger = logging.getLogger(__name__)


def parse_args():
    """
    Parse command-line arguments
    """
    parser = argparse.ArgumentParser(
        description='Make a copy of the data requests in a JSON file for '
                    'loading into the ESGF data fixing software'
    )
    parser.add_argument('fix_dir', help="the path of the directory to fix")
    parser.add_argument('-l', '--log-level', help='set logging level to one of '
                                                  'debug, info, warn (the '
                                                  'default), or error')
    parser.add_argument('--version', action='version',
                        version='%(prog)s {}'.format(__version__))
    args = parser.parse_args()

    return args


def main(args):
    """
    Main entry point
    """
    data_files = glob.glob(os.path.join(args.fix_dir, '*'))

    prepare_script = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        '..',
        'run_prepare.sh'
    )

    for df in data_files:
        cmd = [prepare_script, df]
        cmd_out = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
        if cmd_out.returncode != 0:
            print(f'Need to fix {df}')


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
