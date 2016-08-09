#!/usr/bin/env python2.7
"""
make_dummy_data.py

Uses test_dataset.py to create some dummy files with a .nc extension. A dummy
data submission is created, which contains these files. Dummy data requests
that would be completely and partially fulfilled by these files are also created.

The reset_db.sh script can be used by the user to clear the database before
running this script if desired.
"""
import os
import datetime
import pytz

from django.utils.timezone import make_aware

import test.test_datasets as datasets
from pdata_app.utils.dbapi import get_or_create
from pdata_app.models import (DataSubmission, DataFile, ClimateModel,
    Experiment, Project, Variable, ESGFDataset, CEDADataset, DataRequest,
    Institute)
from vocabs import STATUS_VALUES, FREQUENCY_VALUES


def make_data_request():
    """
    Create a DataRequest that matches files in the later submission
    """
    # Make the variable zos for which all data is available
    institute = get_or_create(Institute, short_name='MOHC', full_name='Met Office Hadley Centre')
    climate_model = get_or_create(ClimateModel, short_name='HadGEM2-ES', full_name='Really good model')
    experiment = get_or_create(Experiment, short_name='rcp45', full_name='Really good experiment')
    variable = get_or_create(Variable, var_id='zos', units='1',
        long_name='Really good variable')

    data_req = get_or_create(DataRequest, institute=institute,
        climate_model=climate_model, experiment=experiment,
        variable=variable, frequency=FREQUENCY_VALUES['day'],
        start_time=datetime.datetime(1991, 1, 1, 0, 0, 0, 0, pytz.utc),
        end_time=datetime.datetime(1993, 12, 30, 0, 0, 0, 0, pytz.utc))

    # Make the variable rsds for which one year is missing
    institute = get_or_create(Institute, short_name='IPSL', full_name='Institut Pierre Simon Laplace')
    climate_model = get_or_create(ClimateModel, short_name='IPSL-CM5A-LR', full_name='Really good model')
    experiment = get_or_create(Experiment, short_name='abrupt4xCO2', full_name='Really good experiment')
    variable = get_or_create(Variable, var_id='rsds', units='1',
        long_name='Really good variable')

    data_req = get_or_create(DataRequest, institute=institute,
        climate_model=climate_model, experiment=experiment,
        variable=variable, frequency=FREQUENCY_VALUES['day'],
        start_time=datetime.datetime(1991, 1, 1, 0, 0, 0, 0, pytz.utc),
        end_time=datetime.datetime(1994, 12, 30, 0, 0, 0, 0, pytz.utc))


def make_data_submission():
    """
    Create a Data Submission and add files to it
    """
    test_dsub = datasets.test_data_submission
    test_dsub.create_test_files()

    dsub = get_or_create(DataSubmission, status=STATUS_VALUES.ARRIVED,
               incoming_directory=test_dsub.INCOMING_DIR,
               directory=test_dsub.INCOMING_DIR)

    for dfile_name in test_dsub.files:
        path = os.path.join(test_dsub.INCOMING_DIR, dfile_name)
        metadata = _extract_file_metadata(path)

        proj = get_or_create(Project, short_name="CMIP6", full_name="6th Coupled Model Intercomparison Project")
        var = get_or_create(Variable, var_id=metadata["var_id"], long_name="Really good variable", units="1")
        climate_model = get_or_create(ClimateModel, short_name=metadata["climate_model"], full_name="Really good model")
        experiment = get_or_create(Experiment, short_name=metadata["experiment"], full_name="Really good experiment")

        _dfile = DataFile.objects.create(name=dfile_name, incoming_directory=test_dsub.INCOMING_DIR,
            directory=test_dsub.INCOMING_DIR, size=os.path.getsize(path),
            project=proj, climate_model=climate_model,
            experiment=experiment, variable=var, frequency=metadata["frequency"],
            rip_code=metadata["ensemble"],
            start_time=make_aware(metadata["start_time"], timezone=pytz.utc, is_dst=False),
            end_time=make_aware(metadata["end_time"], timezone=pytz.utc, is_dst=False),
            data_submission=dsub, online=True)

    ceda_ds = get_or_create(CEDADataset, catalogue_url='http://www.metoffice.gov.uk',
        directory=test_dsub.INCOMING_DIR)

    esgf_ds = get_or_create(ESGFDataset, drs_id='ab.cd.ef.gh', version='v19160519',
        directory=test_dsub.INCOMING_DIR, data_submission=dsub, ceda_dataset=ceda_ds)

    for df in dsub.get_data_files():
        df.esgf_dataset = esgf_ds
        df.ceda_dataset = ceda_ds
        df.ceda_download_url = 'http://browse.ceda.ac.uk/browse/badc/cmip5/' + df.name
        df.ceda_opendap_url = 'http://dap.ceda.ac.uk/data/badc/cmip5/some/dir/' + df.name
        df.esgf_opendap_url = 'http://esgf.ceda.ac.uk/data/badc/cmip5/some/dir/' + df.name
        df.esgf_download_url = 'http://esgf.ceda.ac.uk/browse/badc/cmip5/' + df.name
        df.save()

    dsub.status = STATUS_VALUES.ARCHIVED
    dsub.save()


def main():
    make_data_request()
    make_data_submission()


def _extract_file_metadata(file_path):
    """
    Extracts metadata from file name and returns dictionary.
    """
    # e.g. tasmax_day_IPSL-CM5A-LR_amip4K_r1i1p1_18590101-18591230.nc
    keys = ("var_id", "frequency", "climate_model", "experiment", "ensemble", "time_range")

    items = os.path.splitext(os.path.basename(file_path))[0].split("_")
    data = {}

    for i in range(len(items)):
        key = keys[i]
        value = items[i]

        if key == "time_range":
            start_time, end_time = value.split("-")
            data["start_time"] = datetime.datetime.strptime(start_time, "%Y%m%d")
            data["end_time"] = datetime.datetime.strptime(end_time, "%Y%m%d")
        else:
            data[key] = value

    return data


if __name__ == "__main__":
    main()
