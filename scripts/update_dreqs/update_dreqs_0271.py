#!/usr/bin/env python
"""
update_dreqs_0271.py

From the Cylc db identify jobs that failed due to running out of time. For
these, retrigger once the data's been restored.
"""
from __future__ import (unicode_literals, division, absolute_import,
                        print_function)
import argparse
import logging.config
import sqlite3
import subprocess
import sys


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
    parser.add_argument('-c', '--create', help='Create the retrieval request '
                                               'rather than just displaying '
                                               'the data volums',
                        action='store_true')
    args = parser.parse_args()

    return args


def main(args):
    """
    Main entry point

    Example task_name:

    crepp_submission_HadGEM3-GC31-LL_hist-1950_r1i7p1f1_Omon_so
    """
    conn = sqlite3.connect('/home/users/jseddon/cylc-run/db_u-bs020.db')
    c = conn.cursor()
    task_names = [task_name[0] for task_name in
                  c.execute('SELECT "name" FROM "task_jobs" WHERE '
                            '"name" LIKE "crepp_submission_%" AND '
                            '"run_signal" IS "SIGUSR2";')]

    for task_name in task_names:
        logger.debug(task_name)
        if args.create:
            cmd = f'cylc trigger u-bs020 {task_name}'
            try:
                subprocess.run(cmd, shell=True, check=True,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
            except subprocess.CalledProcessError as exc:
                logger.error(f'{cmd} failed with\n{str(exc)}')


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
