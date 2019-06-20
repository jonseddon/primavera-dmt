#!/usr/bin/env python
"""
update_dreqs_0187.py

Create an issue for the data below and then remove the affected files.

EC-Earth3P-HR hist-1950 r1i1p1f1 v20180710 2013 to 2014

"""
import argparse
import logging.config
import os
import sys

from cf_units import date2num, CALENDAR_GREGORIAN

import django
django.setup()
from django.contrib.auth.models import User
from pdata_app.utils.replace_file import replace_files
from pdata_app.models import DataFile, DataIssue
from pdata_app.utils.common import delete_drs_dir

__version__ = '0.1.0b1'

DEFAULT_LOG_LEVEL = logging.WARNING
DEFAULT_LOG_FORMAT = '%(levelname)s: %(message)s'

logger = logging.getLogger(__name__)


def delete_files(query_set):
    """
    Delete any files online from the specified queryset
    """
    directories_found = []
    for df in query_set.filter(online=True):
        try:
            os.remove(os.path.join(df.directory, df.name))
        except OSError as exc:
            logger.error(str(exc))
        else:
            if df.directory not in directories_found:
                directories_found.append(df.directory)
        df.online = False
        df.directory = None
        df.save()

    for directory in directories_found:
        if not os.listdir(directory):
            delete_drs_dir(directory)
    logger.debug('{} directories removed'.format(len(directories_found)))


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
    lp = User.objects.get(username='lpcaron')
    hist_txt = (
        'An issue has been identified with the way the GHGs were ingested by '
        'EC-Earth for years 2013 and 2014. For those two years, the GHG '
        'concentrations were too low, which led to a sudden drop in surface '
        'thermal radiation and in a noticeable decrease in global surface '
        'temperature. '
    )
    hist_issue, _created = DataIssue.objects.get_or_create(issue=hist_txt,
                                                           reporter=lp)

    affected_files = DataFile.objects.filter(
        climate_model__short_name='EC-Earth3P-HR',
        experiment__short_name='hist-1950',
        rip_code='r1i1p1f1',
        version='v20180710',
        # cf_units.date2num(datetime.datetime(2013, 1 , 1, 0, 0),
        # 'days since 1950-01-01', 'gregorian') = 23011.0
        start_time__gte=23011.0
    )

    logger.debug('{} affected files found'.format(affected_files.count()))

    hist_issue.data_file.add(*affected_files)

    delete_files(affected_files)
    replace_files(affected_files)


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
