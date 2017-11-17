#!/usr/bin/env python2.7
"""
update_dreqs_0038.py

This script calculates the storage required for the online storage of requested
monthly variables.
"""
import argparse
import datetime
import logging.config
import re
import sys

import django
django.setup()
from django.db.models import Sum
from django.template.defaultfilters import filesizeformat
from pdata_app.models import DataRequest

__version__ = '0.1.0b1'

DEFAULT_LOG_LEVEL = logging.WARNING
DEFAULT_LOG_FORMAT = '%(levelname)s: %(message)s'

logger = logging.getLogger(__name__)


def parse_args():
    """
    Parse command-line arguments
    """
    parser = argparse.ArgumentParser(description='Calculate volumes')
    parser.add_argument('-l', '--log-level', help='set logging level to one '
        'of debug, info, warn (the default), or error')
    parser.add_argument('--version', action='version',
        version='%(prog)s {}'.format(__version__))
    args = parser.parse_args()

    return args


def main(args):
    """
    Main entry point
    """
    variables = [
        ('psl', 'Amon'),
        ('tas', 'Amon'),
        ('pr', 'Amon'),
        ('prc', 'Amon'),
        ('tos', 'Omon'),
        ('siconc', 'SImon'),
        ('mlotst', 'Omon'),
        ('hfds', 'Omon'),
        ('tauuo', 'Omon'),
        ('tauvo', 'Omon'),
        ('evspsbl', 'Amon'),
        ('rsds', 'Amon'),
        ('rlds', 'Amon'),
        ('rlus', 'Amon'),
        ('rlut', 'Amon'),
        ('rsdt', 'Amon'),
        ('rsut', 'Amon'),
        ('rsus', 'Amon'),
        ('hfss', 'Amon'),
        ('hfls', 'Amon'),
        ('mrso', 'Lmon'),
        ('clt', 'Amon'),
        ('prw', 'Amon'),
        ('sivol', 'SImon'),
        ('prsn', 'Amon'),
        ('zg500', 'Amon'),
        ('msftmyz', 'Omon'),
        ('mrros', 'Lmon'),
        ('mrro', 'Lmon'),
    ]

    variables.sort(key=lambda pair: pair[0])

    total_size = 0
    model_sizes = {}
    variable_sizes = {}
    for cmor, table in variables:
        data_reqs = DataRequest.objects.filter(
            datafile__isnull=False,
            variable_request__cmor_name=cmor,
            variable_request__table_name=table
        ).distinct()

        if data_reqs.count() == 0:
            logger.debug('No files for {}_{}'.format(table, cmor))
            variable_sizes[cmor] = 0
            continue

        var_size = data_reqs.aggregate(Sum('datafile__size'))[
            'datafile__size__sum']
        total_size += var_size
        variable_sizes[cmor] = var_size

        var_models = data_reqs.values('climate_model__short_name').distinct()

        for model in var_models:
            model_name = model['climate_model__short_name']
            model_size = (data_reqs.
                          filter(climate_model__short_name=model_name).
                          aggregate(Sum('datafile__size'))
                          ['datafile__size__sum'])
            if model_name in model_sizes:
                model_sizes[model_name] += model_size
            else:
                model_sizes[model_name] = model_size

    var_size_check = sum([variable_sizes[vs] for vs in variable_sizes])
    if not total_size == var_size_check:
        logger.error('total size {} var sizes {}'.format(total_size,
                                                         var_size_check))

    model_size_check = sum([model_sizes[ms] for ms in model_sizes])
    if not total_size == model_size_check:
        logger.error('total size {} model sizes {}'.format(total_size,
                                                           model_size_check))
    with open('var_sizes.txt', 'w') as fh:
        fh.write('Report generated at {}\n\n'.format(
            datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:00')))
        fh.write('Total volume requested: {}\n\n'.format(
            _filesizeformat(total_size)))
        fh.write('Volume per variable:\n\n')
        for cmor, table in variables:
            fh.write('{:<10} {}\n'.format(
                cmor, _filesizeformat(variable_sizes[cmor])
            ))
        fh.write('\n')
        fh.write('Volume per model:\n\n')
        model_names = [model_name for model_name in model_sizes]
        model_names.sort()
        for model_name in model_names:
            fh.write('{:<20} {}\n'.format(
                model_name, _filesizeformat(model_sizes[model_name])
            ))
        fh.write('\n')


def _filesizeformat(file_str):
    """
    Remove the unicode characters from the output of the filesizeformat()
    function.

    :param file_str:
    :returns: A string representation of a filesizeformat() string
    """
    cmpts = re.match(r'(\d+\.?\d*)\S(\w+)', filesizeformat(file_str))

    return '{} {}'.format(cmpts.group(1), cmpts.group(2))


if __name__ == "__main__":
    cmd_args = parse_args()

    # determine the log level
    if cmd_args.log_level:
        try:
            log_level = getattr(logging, cmd_args.log_level.upper())
        except AttributeError:
            logger.setLevel(logging.WARNING)
            logger.error('log-level must be one of: debug, info, warn or '
                         'error')
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
