#!/usr/bin/env python
"""
members_table.py

Generate a table showing all of the ensemble members that have been uploaded.
"""
from __future__ import unicode_literals, division, absolute_import

import argparse
import logging.config
import sys

import django
django.setup()
from pdata_app.models import DataRequest


__version__ = '0.1.0b1'

DEFAULT_LOG_LEVEL = logging.WARNING
DEFAULT_LOG_FORMAT = '%(levelname)s: %(message)s'

logger = logging.getLogger(__name__)


def parse_args():
    """
    Parse command-line arguments
    """
    parser = argparse.ArgumentParser(description='Write to file the members '
                                                 'uploaded for each model.')
    parser.add_argument('filepath', help='the file path to write to')
    parser.add_argument('-l', '--log-level', help='set logging level to one of '
                                                  'debug, info, warn (the '
                                                  'default), or error')
    parser.add_argument('--version', action='version',
                        version='%(prog)s {}'.format(__version__))
    args = parser.parse_args()

    return args


def main(args):
    """Run the script"""
    model_order = [
        'AWI-CM-1-1-LR',
        'AWI-CM-1-1-HR',
        'CMCC-CM2-HR4',
        'CMCC-CM2-VHR4',
        'CNRM-CM6-1',
        'CNRM-CM6-1-HR',
        ['EC-Earth3', 'EC-Earth3P'],
        ['EC-Earth3-HR', 'EC-Earth3P-HR'],
        'ECMWF-IFS-LR',
        'ECMWF-IFS-MR',
        'ECMWF-IFS-HR',
        'HadGEM3-GC31-LL',
        'HadGEM3-GC31-LM',
        'HadGEM3-GC31-MM',
        'HadGEM3-GC31-MH',
        'HadGEM3-GC31-HM',
        'HadGEM3-GC31-HH',
        ['MPIESM-1-2-HR', 'MPI-ESM1-2-HR'],
        ['MPIESM-1-2-XR', 'MPI-ESM1-2-XR']
    ]

    experiment_order = [
        'highresSST-present',
        'highresSST-future',
        'spinup-1950',
        'hist-1950',
        'control-1950',
        'highres-future'
    ]

    html_table = '<html><head>'
    html_table += ('<link rel="stylesheet" href="https://unpkg.com/purecss@'
                   '1.0.1/build/pure-min.css" integrity="sha384-oAOxQR6DkCo'
                   'MliIh8yFnu25d7Eq/PHS21PClpwjOTeU2jRSq11vu66rf90/cZr47" '
                   'crossorigin="anonymous">')
    html_table += '</head><body>'
    html_table += ('<table class="pure-table pure-table-bordered" '
                   'style="margin: 10px">')
    html_table += '<thead>'
    html_table += '<tr><th>&nbsp;</th>'
    for expt in experiment_order:
        html_table += f'<th>{expt}</th>'
    html_table += '</tr>'
    html_table += '</thead><tbody>'
    for model in model_order:
        if isinstance(model, str):
            html_table += f'<tr><td><b>{model}</b></td>'
        else:
            html_table += f'<tr><td><b>{model[1]}</b></td>'
        for expt in experiment_order:
            if isinstance(model, str):
                reqs = DataRequest.objects.filter(
                    climate_model__short_name=model,
                    experiment__short_name=expt,
                    datafile__isnull=False,
                )
            else:
                reqs = DataRequest.objects.filter(
                    climate_model__short_name__in=model,
                    experiment__short_name=expt,
                    datafile__isnull=False,
                )
            uploaded = list(reqs.values_list('rip_code', flat=True).distinct())
            uploaded.sort()
            html_table += f'<td>{", ".join(uploaded)}</td>'
        html_table += '</tr>'
    html_table += '</tbody>'
    html_table += '</table>'
    html_table += '</body></html>'

    with open(args.filepath, 'w') as fh:
        fh.write(html_table)


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
