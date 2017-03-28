#!/usr/bin/env python2.7
"""
prepopulate_db.py

This script is run by the data manager to populate the database with the
expected climate models, experiments, etc.
"""
import django
django.setup()

from pdata_app.models import (Project, ClimateModel, Experiment, Institute,
                              ActivityId)
from pdata_app.utils.dbapi import get_or_create


def main():
    """
    Populate the database
    """
    # Projects
    proj = get_or_create(Project, short_name='CMIP6',
        full_name='Coupled Model Intercomparison Project Phase 6')
    proj = get_or_create(Project, short_name='PRIMAVERA',
        full_name='PRIMAVERA High Resolution Modelling Project')

    # Activty ID
    act_id = get_or_create(ActivityId, short_name='HighResMIP',
                           full_name='High Resolution Model Intercomparison '
                                     'Project')

    # Experiments General
    expt = get_or_create(Experiment, short_name='historical',
        full_name='historical')
    expt = get_or_create(Experiment, short_name='present_day',
        full_name='present_day')
    expt = get_or_create(Experiment, short_name='rcp26',
        full_name='Representative Concentration Pathway 2.6 Wm - 2')

    # Experiments HighResMIP
    experiments = {
        'control-1950': "coupled control with fixed 1950's forcing (HighResMIP "
                        "equivalent of pre-industrial control)",
        'highres-future': "coupled future 2015-2050 using a scenario as close "
                          "to CMIP5 RCP8.5 as possible within CMIP6",
        'hist-1950': "coupled historical 1950-2014",
        'highresSST-present': "forced atmosphere experiment for 1950-2014",
        'highresSST-future': "forced atmosphere experiment for 2015-2050 using "
                             "SST/sea-ice derived from CMIP5 RCP8.5 simulations "
                             "and a scenario as close to RCP8.5 as possible "
                             "within CMIP6",
        'highresSST-LAI': "common LAI dataset within the highresSST-present "
                          "experiment",
        'highresSST-smoothed': "smoothed SST version of highresSST-present",
        'highresSST-p4K': "uniform 4K warming of highresSST-present SST",
        'highresSST-4co2': "highresSST-present SST with 4xCO2 concentrations"
    }
    for expt in experiments:
        _ex = get_or_create(Experiment, short_name=expt,
            full_name=experiments[expt])

    # Institutes
    institutes = {
        'AWI': 'AWI',
        'CNRM-CERFACS': 'Centre National de Recherches Meteorologiques, Meteo-France, '
                'Toulouse, France) and CERFACS (Centre Europeen de Recherches '
                'et de Formation Avancee en Calcul Scientifique, Toulouse, '
                'France',
        'CMCC': 'CMCC',
        'KNMI': 'KNMI',
        'SHMI': 'SHMI',
        'BSC': 'BSC',
        'CNR': 'CNR',
        'MPI-M': 'Max Planck Institute for Meteorology',
        'MOHC': 'Met Office Hadley Centre, Fitzroy Road, Exeter, Devon, '
                'EX1 3PB, UK.',
        'ECMWF': 'European Centre for Medium-Range Weather Forecasts'
    }
    for inst in institutes:
        _inst = get_or_create(Institute, short_name=inst,
            full_name=institutes[inst])

    # Models
    models = {
        'AWI-CM': 'AWI-CM:',
        'CNRM': 'CNRM:',
        'CMCC-ESM': 'CMCC-ESM:',
        'EC-Earth': 'EC-Earth:',
        'MPI-ESM': 'MPI-ESM:',
        'HadGEM3': 'HadGEM3:',
        'HadGEM3-GC31-HM': 'HadGEM3-GC31-HM',
        'HadGEM3-GC31-HH': 'HadGEM3-GC31-HH',
        'IFS': 'IFS'
    }
    for model in models:
        _mdl = get_or_create(ClimateModel, short_name=model,
            full_name=models[model])

    # TODO update this for CMIP6, current value is for testing with CMIP5 data
    clim_model = get_or_create(ClimateModel, short_name='HadGEM2-ES',
        full_name='HadGEM2 Earth System')
    clim_model = get_or_create(ClimateModel, short_name='HadGEM3-GC2',
        full_name='HadGEM3 GC2')


if __name__ == '__main__':
    main()
