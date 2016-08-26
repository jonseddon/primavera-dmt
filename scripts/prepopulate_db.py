#!/usr/bin/env python2.7
"""
prepopulate_db.py

This script is run by the data manager to populate the database with the
expected climate models, experiments, etc.
"""
import django
django.setup()

from pdata_app.models import Project, ClimateModel, Experiment
from pdata_app.utils.dbapi import get_or_create


def main():
    """
    Populate the database
    """
    proj = get_or_create(Project, short_name='CMIP6',
        full_name='6th Coupled Model Intercomparison Project')

    expt = get_or_create(Experiment, short_name='historical',
        full_name='historical')

    # TODO update this for CMIP6, current value is for testing with CMIP5 data
    clim_model = get_or_create(ClimateModel, short_name='HadGEM2-ES',
        full_name='HadGEM2 Earth System')


if __name__ == '__main__':
    main()
