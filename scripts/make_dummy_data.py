#!/usr/bin/env python2.7
"""
make_dummy_data.py

Uses test_dataset.py to create some dummy files with a .nc extension. A dummy
data submission is created, which contains these files.

The reset_db.sh script can be used by the user to clear the database before
running this script if desired.
"""
import os
import datetime

import test.test_datasets as datasets
from pdata_app.utils.dbapi import get_or_create
from pdata_app.models import (DataSubmission, DataFile, ClimateModel,
    Experiment, Project, Variable)
from vocabs import STATUS_VALUES


def main():
    # Create a Data Submission and add files to it
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

        dfile = DataFile.objects.create(name=dfile_name, incoming_directory=test_dsub.INCOMING_DIR,
                                        directory=test_dsub.INCOMING_DIR, size=os.path.getsize(path),
                                        project=proj, climate_model=climate_model,
                                        experiment=experiment, variable=var, frequency=metadata["frequency"],
                                        start_time=metadata["start_time"], end_time=metadata["end_time"],
                                        data_submission=dsub, online=True)


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
