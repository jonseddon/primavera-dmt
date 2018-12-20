#!/usr/bin/env python
"""
update_dreqs_0114.py

This file adds an issue to EC-Earth r1i1p1f1 to indicate their purpose and
relationship to r1i1p2f1 data.
"""
import argparse
import logging.config
import sys


import django
django.setup()
from django.contrib.auth.models import User

from pdata_app.models import DataFile, DataIssue


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
    gijs = User.objects.get(username='gvdoord')
    issue_txt = (
        "The r1i1p2f1 ocean model has different parameters controlling the "
        "turbulent kinetic energy scheme; in particular the parametrization "
        "of Langmuir cells was modified (rn_lc = 0.15 (r1i1p1f1), 0.2 "
        "(r1i1p2f1)) and the penetration of TKE below the mixed layer has "
        "been disabled (nn_etau = 1 (r1i1p1f1), 0 (r1i1p2f1)). Furthermore "
        "the thermal conductivity of the snow on sea ice has been changed "
        "(rn_cdsn = 0.31 W m-1 K-1 (r1i1p1f1), 0.4 W m-1 K-1 (r1i1p2f1)). As "
        "a result, we have obtained more realistic Atlantic meridional "
        "overturning circulation and sea ice extent."
    )
    issue, _created = DataIssue.objects.get_or_create(issue=issue_txt,
                                                           reporter=gijs)

    r1i1p1f1 = DataFile.objects.filter(
        climate_model__short_name='EC-Earth3P-HR',
        experiment__short_name__in=['control-1950', 'hist-1950'],
        version='v20180710',
        rip_code='r1i1p1f1',
    ).distinct()
    logger.debug('{} r1i1p1f1 coupled files found'.format(
        r1i1p1f1.count()))
    issue.data_file.add(*r1i1p1f1)

    spinup = DataFile.objects.filter(
        climate_model__short_name='EC-Earth3-HR',
        experiment__short_name='spinup-1950',
        version='v20171119',
        rip_code='r1i1p1f1',
    ).distinct()
    logger.debug('{} r1i1p1f1 spinup files found'.format(
        spinup.count()))
    issue.data_file.add(*spinup)



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
