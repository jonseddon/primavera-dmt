#!/usr/bin/env python
"""
esgf_timeseries.py

Calculate the volume of Stream 1 and Stream 2 data submitted to the ESGF. Save
the results in a file and generate a plot to show the progress.
"""
from __future__ import unicode_literals, division, absolute_import

import argparse
import logging.config
import sys

import django
from django.db.models import Sum
from django.template.defaultfilters import filesizeformat
django.setup()
from pdata_app.models import DataFile, ESGFDataset


__version__ = '0.1.0b1'

DEFAULT_LOG_LEVEL = logging.WARNING
DEFAULT_LOG_FORMAT = '%(levelname)s: %(message)s'

logger = logging.getLogger(__name__)


def get_volume_published():
    """
    Calculate the total volume in bytes of files in the ESGF publication
    process.

    :return: volume in bytes
    """
    esgf_volume = 0
    for esgf in ESGFDataset.objects.all():
        dataset_volume = (esgf.data_request.datafile_set.distinct().
                          aggregate(Sum('size'))['size__sum'])
        if dataset_volume:
            esgf_volume += dataset_volume

    return esgf_volume


def get_total_volume():
    """
    Calculate the total volume in bytes of files that need to be published.

    :return: volume in bytes.
    """
    amip_expts = ['highresSST-present', 'highresSST-future']
    coupled_expts = ['spinup-1950', 'hist-1950', 'control-1950',
                     'highres-future']
    stream1_2_expts = amip_expts + coupled_expts

    # MOHC stream 2 is members r1i2p2f1 to r1i15p1f1
    mohc_stream2_members = [f'r1i{init_index}p1f1'
                            for init_index in range(2,16)]

    stream1_2 = DataFile.objects.filter(
        experiment__short_name__in=stream1_2_expts
    ).exclude(
        # Exclude MOHC Stream 2
        institute__short_name__in=['MOHC', 'NERC'],
        rip_code__in=mohc_stream2_members,
    ).exclude(
        # Exclude EC-Earth coupled r1i1p1f1
        institute__short_name='EC-Earth-Consortium',
        experiment__short_name__in=coupled_expts,
        rip_code='r1i1p1f1'
    ).distinct()

    mohc_stream2_members = DataFile.objects.filter(
        institute__short_name__in=['MOHC', 'NERC'],
        experiment__short_name__in=stream1_2_expts,
        rip_code__in = mohc_stream2_members
    ).distinct()

    mohc_stream2_low_freq = mohc_stream2_members.filter(
        variable_request__frequency__in=['mon', 'day']
    ).exclude(
        variable_request__table_name='CFday'
    ).distinct()

    mohc_stream2_cfday = mohc_stream2_members.filter(
        variable_request__table_name='CFday',
        variable_request__cmor_name='ps'
    ).distinct()

    mohc_stream2_6hr = mohc_stream2_members.filter(
        variable_request__table_name='Prim6hr',
        variable_request__cmor_name='wsgmax'
    ).distinct()

    mohc_stream2_3hr = mohc_stream2_members.filter(
        variable_request__table_name__in=['3hr', 'E3hr', 'E3hrPt', 'Prim3hr',
                                          'Prim3hrPt'],
        variable_request__cmor_name__in=['rsdsdiff', 'rsds', 'tas', 'uas',
                                         'vas', 'ua50m', 'va50m', 'ua100m',
                                         'va100m', 'ua850', 'va850', 'sfcWind',
                                         'sfcWindmax', 'pr', 'psl', 'zg7h']
    ).distinct()

    publishable_files = (stream1_2 | mohc_stream2_low_freq |
                         mohc_stream2_cfday | mohc_stream2_6hr |
                         mohc_stream2_3hr)

    return publishable_files.aggregate(Sum('size'))['size__sum']


def parse_args():
    """
    Parse command-line arguments
    """
    parser = argparse.ArgumentParser(description='Plot the volume of data '
                                                 'submitted to the ESGF.')
    parser.add_argument('time-series-file', help='The full path to the comma '
                                                 'separated file to save the '
                                                 'time series data in.')
    parser.add_argument('output-image', help='The full path to the image that '
                                             'will be generated.')
    parser.add_argument('-l', '--log-level', help='set logging level to one of '
                                                  'debug, info, warn (the '
                                                  'default), or error')
    parser.add_argument('--version', action='version',
                        version='%(prog)s {}'.format(__version__))
    args = parser.parse_args()

    return args


def main(args):
    """Run the script"""
    esgf_volume = get_volume_published()
    total_volume = get_total_volume()

    pretty_esgf = filesizeformat(esgf_volume).replace('\xa0', ' ')
    pretty_total = filesizeformat(total_volume).replace('\xa0', ' ')
    print(f'Volume published to ESGF {pretty_esgf}')
    print(f'Total Volume {pretty_total}')
    print(f'Percentage published to ESGF {esgf_volume / total_volume:.0%}')


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
