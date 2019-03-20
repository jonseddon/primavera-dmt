#!/usr/bin/env python
"""
update_dreqs_0142.py

This file adds an issue to ECMWF experiments that may have an aliasing of the
diurnal cycle in time mean values.
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
    retish = User.objects.get(username='retish')
    issue_txt = (
        "Some ECMWF variables are not true monthly or daily means and are "
        "instead calculated from instantaneous values at the frequency they "
        "are output. Experiments and members are affected differently "
        "depending on how the output frequency was specified. This feature "
        "was first spotted when a difference was found in the variable ts "
        "between the hist-1950 and control-1950 simulations."
    )
    issue, _created = DataIssue.objects.get_or_create(issue=issue_txt,
                                                      reporter=retish)

    common = {
        'institute__short_name': 'ECMWF',
        'frequency__in': ['mon', 'day'],
        'variable_request__cmor_name__in': ['lai', 'snw', 'stlsi', 'mrso',
                                            'clwvi', 'clivi', 'ps', 'prw',
                                            'tsl', 'snd', 'ts', 'tsn',
                                            'uneutrals', 'vneutrals', 'tso']
    }

    affected_sims = [
        'ECMWF-IFS-LR_hist-1950_r2i1p1f1',
        'ECMWF-IFS-LR_hist-1950_r3i1p1f1',
        'ECMWF-IFS-LR_hist-1950_r4i1p1f1',
        'ECMWF-IFS-LR_hist-1950_r5i1p1f1',
        'ECMWF-IFS-LR_hist-1950_r6i1p1f1',
        'ECMWF-IFS-LR_control-1950_r1i1p1f1',
        'ECMWF-IFS-LR_highresSST-present_r3i1p1f1',
        'ECMWF-IFS-LR_highresSST-present_r4i1p1f1',
        'ECMWF-IFS-LR_highresSST-present_r5i1p1f1',
        'ECMWF-IFS-LR_highresSST-present_r6i1p1f1',
        'ECMWF-IFS-MR_hist-1950_r1i1p1f1',
        'ECMWF-IFS-MR_control-1950_r1i1p1f1',
        'ECMWF-IFS-HR_hist-1950_r3i1p1f1',
        'ECMWF-IFS-HR_hist-1950_r4i1p1f1',
        'ECMWF-IFS-HR_control-1950_r1i1p1f1',
        'ECMWF-IFS-HR_highresSST-present_r3i1p1f1',
        'ECMWF-IFS-HR_highresSST-present_r4i1p1f1'
    ]

    for sim in affected_sims:
        model, expt, var_lab = sim.split('_')
        affected_files = DataFile.objects.filter(
            climate_model__short_name=model,
            experiment__short_name=expt,
            rip_code=var_lab,
            **common
        ).distinct()
        logger.debug('{} affected files found for {}'.
            format(affected_files.count(), sim))
        issue.data_file.add(*affected_files)


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
