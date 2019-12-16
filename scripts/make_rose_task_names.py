#!/usr/bin/env python
"""
make_rose_task_names.py

This script is used to generate a JSON list of the task names that
should be run by the rose suite that performs submissions to the CREPP
system.

All task names are added to the specified JSON file, e.g. `filename.json`, but
an additional file called `filename_name.json` is created containing the task
names that have been added to the JSON file. Any existing `filename_new.json`
file is renamed to `filename_new.json.YYmmddHHMM`.
"""
import argparse
import datetime
import json
import logging.config
import os
import sys

import django
django.setup()

from pdata_app.models import DataRequest

__version__ = '0.1.0b'

DEFAULT_LOG_LEVEL = logging.WARNING
DEFAULT_LOG_FORMAT = '%(levelname)s: %(message)s'

logger = logging.getLogger(__name__)


def parse_args():
    """
    Parse command-line arguments
    """
    parser = argparse.ArgumentParser(
        description='Generate a JSON list of the Rose task names that should '
                    'be submitted to CREPP.'
    )
    parser.add_argument('json_file', help="the path to the JSON file to "
                                          "generate")
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

    Task names in the output JSON file are in the form:

    <climate-model>_<experiment>_<variant-label>_<table>_<variable>

    e.g.:

    HadGEM3-GC31-LM_highresSST-present_r1i1p1f1_Amon_psl
    """
    existing_tasks = []
    if os.path.exists(args.json_file):
        with open(args.json_file) as fh:
            existing_tasks = json.load(fh)

        logger.debug('{} existing tasks loaded from file'.
                     format(len(existing_tasks)))

    hadgem3_gc31_amip_amon = DataRequest.objects.filter(
        climate_model__short_name__startswith='HadGEM3-GC31',
        experiment__short_name='highresSST-present',
        rip_code='r1i1p1f1',
        variable_request__table_name='Amon',
        datafile__isnull=False
    ).distinct()

    ecmwf_amip_all = DataRequest.objects.filter(
        institute__short_name='ECMWF',
        experiment__short_name='highresSST-present',
        rip_code='r1i1p1f1',
        datafile__isnull=False
    ).exclude(
        variable_request__table_name__startswith='Prim'
    ).distinct()

    cmcc_amip_all = DataRequest.objects.filter(
        institute__short_name='CMCC',
        experiment__short_name='highresSST-present',
        rip_code='r1i1p1f1',
        datafile__isnull=False
    ).exclude(
        variable_request__table_name__startswith='Prim'
    ).distinct()

    hadgem3_gc31_amip_lm = DataRequest.objects.filter(
        climate_model__short_name='HadGEM3-GC31-LM',
        experiment__short_name='highresSST-present',
        rip_code='r1i1p1f1',
        datafile__isnull=False
    ).exclude(
        variable_request__table_name__startswith='Prim'
    ).distinct()

    hadgem3_gc31_amip_mm = DataRequest.objects.filter(
        climate_model__short_name='HadGEM3-GC31-MM',
        experiment__short_name='highresSST-present',
        rip_code='r1i1p1f1',
        datafile__isnull=False
    ).exclude(
        variable_request__table_name__startswith='Prim'
    ).distinct()

    hadgem3_gc31_amip_hm = DataRequest.objects.filter(
        climate_model__short_name='HadGEM3-GC31-HM',
        experiment__short_name='highresSST-present',
        rip_code='r1i1p1f1',
        variable_request__frequency__in=['mon', 'day', '6hr', '3hr', '1hr'],
        datafile__isnull=False
    ).exclude(
        variable_request__table_name__startswith='Prim'
    ).distinct()

    mpi_amip_all = DataRequest.objects.filter(
        institute__short_name='MPI-M',
        climate_model__short_name__in=['MPI-ESM1-2-HR', 'MPI-ESM1-2-XR'],
        experiment__short_name='highresSST-present',
        datafile__isnull=False
    ).exclude(
        variable_request__table_name__startswith='Prim'
    ).distinct()

    cerfacs_amip = DataRequest.objects.filter(
        climate_model__short_name='CNRM-CM6-1',
        experiment__short_name='highresSST-present',
        rip_code='r1i1p1f2',
    ).exclude(
        variable_request__table_name__startswith='Prim'
    ).filter(
        datafile__isnull=False
    ).distinct()

    cerfacs_hr_amip = DataRequest.objects.filter(
        climate_model__short_name='CNRM-CM6-1-HR',
        experiment__short_name='highresSST-present',
        rip_code='r1i1p1f2',
        variable_request__frequency__in=['mon', 'day', '6hr', '3hr'],
        datafile__isnull=False
    ).exclude(
        variable_request__table_name__startswith='Prim'
    ).distinct()

    mohc_ll_coup = DataRequest.objects.filter(
        climate_model__short_name='HadGEM3-GC31-LL',
        experiment__short_name__in=['hist-1950', 'control-1950'],
        rip_code='r1i1p1f1',
        variable_request__table_name__in=[
            '3hr', '6hrPlev', '6hrPlevPt', 'AERday', 'AERmon', 'Amon', 
            'CF3hr', 'CFday', 'CFmon', 'E1hr', 'E3hr', 'E3hrPt', 'Eday', 
            'EdayZ', 'Emon', 'EmonZ', 'Esubhr', 'LImon', 'Lmon', 'day'
        ],
        datafile__isnull=False
    ).distinct()

    mohc_mm_coup = DataRequest.objects.filter(
        climate_model__short_name='HadGEM3-GC31-MM',
        experiment__short_name__in=['hist-1950', 'control-1950'],
        rip_code='r1i1p1f1',
        variable_request__table_name__in=[
            '3hr', '6hrPlev', '6hrPlevPt', 'AERday', 'AERmon', 'Amon',
            'CF3hr', 'CFday', 'CFmon', 'E1hr', 'E3hr', 'E3hrPt', 'Eday',
            'EdayZ', 'Emon', 'EmonZ', 'Esubhr', 'LImon', 'Lmon', 'day'
        ],
        datafile__isnull=False
    ).distinct()

    mohc_hm_hist = DataRequest.objects.filter(
        climate_model__short_name='HadGEM3-GC31-HM',
        experiment__short_name='hist-1950',
        rip_code='r1i1p1f1',
        variable_request__table_name__in=[
            '3hr', '6hrPlev', '6hrPlevPt', 'AERday', 'AERmon', 'Amon',
            'CF3hr', 'CFday', 'CFmon', 'E1hr', 'E3hr', 'E3hrPt', 'Eday',
            'EdayZ', 'Emon', 'EmonZ', 'Esubhr', 'LImon', 'Lmon', 'day'
        ],
        # variable_request__frequency__in=['mon', 'day', '6hr', '3hr'],
        datafile__isnull=False
    ).distinct()

    mohc_hm_cont = DataRequest.objects.filter(
        climate_model__short_name='HadGEM3-GC31-HM',
        experiment__short_name='control-1950',
        rip_code='r1i1p1f1',
        variable_request__table_name__in=[
            '3hr', '6hrPlev', '6hrPlevPt', 'AERday', 'AERmon', 'Amon',
            'CF3hr', 'CFday', 'CFmon', 'E1hr', 'E3hr', 'E3hrPt', 'Eday',
            'EdayZ', 'Emon', 'EmonZ', 'Esubhr', 'LImon', 'Lmon', 'day'
        ],
        datafile__isnull=False
    ).distinct()

    ecmwf_lr_coupled = DataRequest.objects.filter(
        climate_model__short_name='ECMWF-IFS-LR',
        experiment__short_name__in=['hist-1950', 'control-1950', 'spinup-1950'],
        rip_code='r1i1p1f1',
        datafile__isnull=False
    ).exclude(
        variable_request__table_name__startswith='Prim'
    ).distinct()

    ecmwf_hr_coupled = DataRequest.objects.filter(
        climate_model__short_name='ECMWF-IFS-HR',
        experiment__short_name__in=['hist-1950', 'control-1950', 'spinup-1950'],
        rip_code='r1i1p1f1',
        datafile__isnull=False
    ).exclude(
        variable_request__table_name__startswith='Prim'
    ).distinct()

    cerfacs_lr_coupled = DataRequest.objects.filter(
        climate_model__short_name='CNRM-CM6-1',
        experiment__short_name__in=['hist-1950', 'control-1950'],
        rip_code='r1i1p1f2',
        datafile__isnull=False
    ).exclude(
        variable_request__table_name__startswith='Prim'
    ).distinct()

    cerfacs_hr_coupled = DataRequest.objects.filter(
        climate_model__short_name='CNRM-CM6-1-HR',
        experiment__short_name='hist-1950',
        rip_code='r1i1p1f2',
        datafile__isnull=False
    ).exclude(
        variable_request__table_name__startswith='Prim'
    ).distinct()

    mohc_amip_future = DataRequest.objects.filter(
        climate_model__short_name__in=['HadGEM3-GC31-LM', 'HadGEM3-GC31-MM', 
                                       'HadGEM3-GC31-HM'],
        experiment__short_name='highresSST-future',
        rip_code='r1i1p1f1',
        datafile__isnull=False
    ).exclude(
        variable_request__table_name__startswith='Prim'        
    ).distinct()

    mohc_future = DataRequest.objects.filter(
        climate_model__short_name__in=['HadGEM3-GC31-LL', 'HadGEM3-GC31-MM'],
        experiment__short_name='highres-future',
        rip_code='r1i1p1f1',
        datafile__isnull=False
    ).exclude(
        variable_request__table_name__startswith='Prim'
    ).distinct()

    mohc_future_hm = DataRequest.objects.filter(
        climate_model__short_name='HadGEM3-GC31-HM',
        experiment__short_name='highres-future',
        rip_code='r1i1p1f1',
        variable_request__table_name__in=[
            '3hr', '6hrPlev', '6hrPlevPt', 'AERday', 'AERmon', 'Amon',
            'CF3hr', 'CFday', 'CFmon', 'E1hr', 'E3hr', 'E3hrPt', 'Eday',
            'EdayZ', 'Emon', 'EmonZ', 'Esubhr', 'LImon', 'Lmon', 'day'
        ],
        datafile__isnull=False
    ).distinct()

    cerfacs_future = DataRequest.objects.filter(
        climate_model__short_name__in=['CNRM-CM6-1', 'CNRM-CM6-1-HR'],
        experiment__short_name__in=['highresSST-future', 'highres-future'],
        rip_code='r1i1p1f2',
        datafile__isnull=False
    ).exclude(
        variable_request__table_name__startswith='Prim'
    ).distinct()

    cmcc_coupled = DataRequest.objects.filter(
        climate_model__short_name='CMCC-CM2-HR4',
        experiment__short_name__in=['control-1950', 'hist-1950'],
        rip_code='r1i1p1f1',
        datafile__isnull=False
    ).exclude(
        variable_request__table_name__startswith='Prim'
    ).distinct()

    cmcc_vhr_coupled = DataRequest.objects.filter(
        climate_model__short_name='CMCC-CM2-VHR4',
        experiment__short_name='control-1950',
        rip_code='r1i1p1f1',
        variable_request__frequency__in=['mon', 'day', '6hr'],
        datafile__isnull=False
    ).exclude(
        variable_request__table_name__startswith='Prim'
    ).distinct()

    cmcc_vhr_hist = DataRequest.objects.filter(
        climate_model__short_name='CMCC-CM2-VHR4',
        experiment__short_name='hist-1950',
        rip_code='r1i1p1f1',
        datafile__isnull=False
    ).exclude(
        variable_request__table_name__startswith='Prim'
    ).distinct()

    mpi_coupled = DataRequest.objects.filter(
        climate_model__short_name__in=['MPI-ESM1-2-HR', 'MPI-ESM1-2-XR'],
        experiment__short_name__in=['hist-1950', 'control-1950'],
        datafile__isnull=False
    ).exclude(
        variable_request__table_name__startswith='Prim'
    ).distinct()

    mohc_spinup = DataRequest.objects.filter(
        climate_model__short_name__in=['HadGEM3-GC31-LL', 'HadGEM3-GC31-MM', 'HadGEM3-GC31-MH'],
        experiment__short_name='spinup-1950',
        rip_code='r1i1p1f1',
        variable_request__table_name__in=[
            '3hr', '6hrPlev', '6hrPlevPt', 'AERday', 'AERmon', 'Amon',
            'CF3hr', 'CFday', 'CFmon', 'E1hr', 'E3hr', 'E3hrPt', 'Eday',
            'EdayZ', 'Emon', 'EmonZ', 'Esubhr', 'LImon', 'Lmon', 'day'
        ],
        datafile__isnull=False
    ).distinct()

    # task querysets can be ORed together with |

    all_tasks = (hadgem3_gc31_amip_amon | ecmwf_amip_all | cmcc_amip_all | hadgem3_gc31_amip_lm |
                 mpi_amip_all | hadgem3_gc31_amip_mm | hadgem3_gc31_amip_hm | cerfacs_amip | 
                 cerfacs_hr_amip | mohc_ll_coup | mohc_mm_coup | mohc_hm_hist | mohc_hm_cont |
                 ecmwf_lr_coupled | ecmwf_hr_coupled | cerfacs_lr_coupled | cerfacs_hr_coupled |
                 mohc_amip_future | cerfacs_future | mohc_future | mohc_future_hm | cmcc_coupled |
                 cmcc_vhr_coupled | cmcc_vhr_hist | mpi_coupled | mohc_spinup)

    task_name_list = [
        '{}_{}_{}_{}_{}'.format(dr.climate_model.short_name,
                                dr.experiment.short_name,
                                dr.rip_code,
                                dr.variable_request.table_name,
                                dr.variable_request.cmor_name)
        for dr in all_tasks
    ]
    logger.debug('{} tasks in total'.format(len(all_tasks)))

    with open(args.json_file, 'w') as fh:
        json.dump(task_name_list, fh, indent=4)

    if existing_tasks:
        new_tasks_list = list(set(task_name_list) - set(existing_tasks))

        new_tasks_file = args.json_file.replace('.json', '_new.json')
        if os.path.exists(new_tasks_file):
            suffix = datetime.datetime.utcnow().strftime('%Y%m%d%H%M')
            os.rename(new_tasks_file, new_tasks_file + '.' + suffix)
        with open(new_tasks_file, 'w') as fh:
            json.dump(new_tasks_list, fh, indent=4)
        logger.debug('{} new tasks'.format(len(new_tasks_list)))


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
