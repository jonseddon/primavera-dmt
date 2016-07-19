"""
Unit tests for pdata_app.models
"""
import os
import datetime

from django.test import TestCase
from django.test import runner

from pdata_app import models
from vocabs import STATUS_VALUES
from pdata_app.utils.dbapi import get_or_create
from test.test_datasets import test_data_submission


def _extract_file_metadata(file_path):
    "Extracts metadata from file name and returns dictionary."
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


class TestDataSubmission(TestCase):
    def setUp(self):
        example_files = test_data_submission
        self.dsub = get_or_create(models.DataSubmission, status=STATUS_VALUES.ARRIVED,
            incoming_directory=test_data_submission.INCOMING_DIR,
            directory=test_data_submission.INCOMING_DIR)

        for dfile_name in example_files.files:
            metadata = _extract_file_metadata(os.path.join(
                test_data_submission.INCOMING_DIR, dfile_name))
            self.proj = get_or_create(models.Project, short_name="CMIP6", full_name="6th "
                "Coupled Model Intercomparison Project")
            climate_model = get_or_create(models.ClimateModel,
                short_name=metadata["climate_model"], full_name="Really good model")
            experiment = get_or_create(models.Experiment, short_name="experiment",
                full_name="Really good experiment")
            var = get_or_create(models.Variable, var_id=metadata["var_id"],
                long_name="Really good variable", units="1")

            dfile = models.DataFile.objects.create(name=dfile_name,
                incoming_directory=example_files.INCOMING_DIR,
                directory=example_files.INCOMING_DIR, size=1, project=self.proj,
                climate_model=climate_model, experiment=experiment,
                variable=var, frequency=metadata["frequency"], online=True,
                data_submission=self.dsub)

    def test_project(self):
        self.assertEqual(self.dsub.project(), [self.proj])