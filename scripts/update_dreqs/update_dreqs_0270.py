#!/usr/bin/env python
"""
update_dreqs_0270.py

From the Cylc db identify jobs that failed due to running out of time. For
these, delete the data from disk and create a retrieval request to restore it.
"""
from __future__ import (unicode_literals, division, absolute_import,
                        print_function)
import argparse
import datetime
import logging.config
import sqlite3
import sys

import django
django.setup()

from django.contrib.auth.models import User
from pdata_app.models import DataRequest, RetrievalRequest
from pdata_app.utils.common import delete_files


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
    if args.create:
        start_year = 1948
        end_year = 2051
        jon = User.objects.get(username='jseddon')
        rr = RetrievalRequest.objects.create(requester=jon, start_year=start_year,
                                             end_year=end_year)
        time_zone = datetime.timezone(datetime.timedelta())
        rr.date_created = datetime.datetime(2000, 1, 1, 0, 0, tzinfo=time_zone)
        rr.save()

    conn = sqlite3.connect('/home/users/jseddon/cylc-run/db_u-bs020.db')
    c = conn.cursor()
    for task_name in c.execute('SELECT "name" FROM "task_jobs" WHERE '
                               '"name" LIKE "crepp_submission_%" AND '
                               '"run_signal" IS "SIGUSR2";'):
        model, expt, var_label, table, var_name = task_name[0].split('_')[2:]
        dreq = DataRequest.objects.get(
            climate_model__short_name=model,
            experiment__short_name=expt,
            rip_code=var_label,
            variable_request__table_name=table,
            variable_request__cmor_name=var_name
        )
        logger.debug(f'{task_name[0]} '
                     f'{dreq.datafile_set.filter(online=True).count()}')
        if args.create:
            try:
                delete_files(dreq.datafile_set.all(),
                             '/gws/nopw/j04/primavera5/stream1')
            except Exception as exc:
                logger.error(str(exc))
            rr.data_request.add(dreq)


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
