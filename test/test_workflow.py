"""
Use unit tests to represent example workflows in the data management tool.

The workflows that will occur are documented in:
https://docs.google.com/document/d/1qnIg2pHqF1I1tuP_iCVzb6yL_bXZoheBBUQ9RuGWPlQ
"""
import os
import datetime
import unittest
import logging
import inspect
import pytz

from django.test import TestCase
from django.utils.timezone import make_aware

import test.test_datasets as datasets
from config import config
from config import get_dir_from_scheme

from vocabs.vocabs import FREQUENCY_VALUES, STATUS_VALUES
from pdata_app.utils.dbapi import get_or_create
from pdata_app.models import (ClimateModel, Institute, Experiment, Project,
    Variable, DataSubmission, DataFile, DataRequest, ESGFDataset, CEDADataset,
    DataIssue, Checksum, Settings)
from pdata_app.models import __all__ as all_class_names

classes = [eval(cls_name) for cls_name in all_class_names]


# Utility functions for test workflow
def _empty_test_data_dirs():
    dir_types = ("incoming", "archive", "esgf")

    for dir_type in dir_types:
        dr = get_dir_from_scheme("CMIP6-MOHC", "%s_dir" % dir_type)
        for fname in os.listdir(dr):
            os.remove(os.path.join(dr, fname))


def _create_test_data_dirs():
    dir_types = ("incoming", "archive", "esgf")

    for dir_type in dir_types:
        dr = get_dir_from_scheme("CMIP6-MOHC", "%s_dir" % dir_type)
        if not os.path.isdir(dr): os.makedirs(dr)


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


class PdataBaseTest(TestCase):

    @classmethod
    def tlog(self, msg, log_level="info"):
        meth_name = inspect.stack()[1][0].f_code.co_name
        self.log.log(getattr(logging, log_level.upper()), "%s: %s" % (meth_name, msg))

    @classmethod
    def setUpClass(self):
        self.log = logging.getLogger(self.__class__.__name__)
        self.log.setLevel(getattr(logging, config["log_level"]))
        self.tlog("Setting up...", "INFO")
        _create_test_data_dirs()

    @classmethod
    def tearDownClass(self):
        self.log.info("Removing all content after running tests.")
        # Empty data dirs
        _empty_test_data_dirs()


class TestWorkflows(PdataBaseTest):
    def setUp(self):
        # Set up global settings
        self.settings = Settings.objects.create(is_paused=False)

    def tearDown(self):
        pass

    def test_01_data_request(self):
        institute = get_or_create(Institute, short_name='u', full_name='University')
        climate_model = get_or_create(ClimateModel, short_name='my_model', full_name='Really big model')
        experiment = get_or_create(Experiment, short_name='my_expt', full_name='Really detailed experiment')
        variable = get_or_create(Variable, var_id='var1', units='1')

        data_req = get_or_create(DataRequest, institute=institute,
            climate_model=climate_model, experiment=experiment,
            variable=variable, frequency=FREQUENCY_VALUES['ann'],
            start_time=datetime.datetime(1900, 1, 1, 0, 0, 0, 0, pytz.utc),
            end_time=datetime.datetime(2000, 1, 1, 0, 0, 0, 0, pytz.utc))

        # Make some assertions
        data_req = DataRequest.objects.all()[0]
        self.assertEqual(data_req.institute.full_name, 'University')
        self.assertEqual(data_req.climate_model.short_name, 'my_model')
        self.assertEqual(data_req.experiment.short_name, 'my_expt')
        self.assertEqual(data_req.variable.var_id, 'var1')

    def test_02_data_submission(self):
        # Create a Data Submission and add files to it
        self.tlog("STARTING: test_00")
        test_dsub = self._make_data_submission()

        # Make some assertions
        for dfile_name in test_dsub.files:
            self.assertEqual(dfile_name, DataFile.objects.filter(name=dfile_name).first().name)

    def test_03_move_files_to_tape(self):
        test_dsub = self._make_data_submission()

        data_submission = DataSubmission.objects.all()[0]

        for df in data_submission.get_data_files():
            df.tape_url = 'batch_id:4037'
            df.online = False
            df.save()

        # Make some assertions
        for dfile_name in test_dsub.files:
            self.assertFalse(DataFile.objects.filter(name=dfile_name).first().online)
            self.assertEqual(DataFile.objects.filter(name=dfile_name).first().tape_url, 'batch_id:4037')

    def test_04_restore_from_tape(self):
        # Do everything that was done for moving to tape
        test_dsub = self._make_data_submission()

        data_submission = DataSubmission.objects.all()[0]

        for df in data_submission.get_data_files():
            df.tape_url = 'batch_id:4037'
            df.online = False
            df.save()

        # Check that data was moved to tape
        for dfile_name in test_dsub.files:
            self.assertFalse(DataFile.objects.filter(name=dfile_name).first().online)
            self.assertEqual(DataFile.objects.filter(name=dfile_name).first().tape_url, 'batch_id:4037')

        # Restore from tape
        for df in data_submission.get_data_files():
            df.online = True
            df.save()

        # Make some assertions to check that it's been restored
        for dfile_name in test_dsub.files:
            self.assertTrue(DataFile.objects.filter(name=dfile_name).first().online)
            # check that it's still held on tape
            self.assertEqual(DataFile.objects.filter(name=dfile_name).first().tape_url, 'batch_id:4037')

    def test_05_create_data_issue(self):
        pass

    def test_06_ingest_to_ceda(self):
        # Create a ata submission to start with
        test_dsub = self._make_data_submission()

        data_submission = DataSubmission.objects.all()[0]


        # Create a CEDA data set
        ceda_ds = get_or_create(CEDADataset, doi='doi:10.2514/1.A32039',
            catalogue_url='http://catalogue.ceda.ac.uk/uuid/85c7d0b09c974bd6abb07a324c2f427b',
            directory='/badc/some/dir')

        for df in data_submission.get_data_files():
            df.ceda_dataset = ceda_ds
            df.ceda_opendap_url = 'http://dap.ceda.ac.uk/data/badc/cmip5/some/dir/' + df.name
            df.ceda_download_url = 'http://browse.ceda.ac.uk/browse/badc/cmip5/' + df.name
            df.save()

        # Make some assertions
        for dfile_name in test_dsub.files:
            df = DataFile.objects.filter(name=dfile_name).first()
            self.assertEqual(df.ceda_dataset.doi, 'doi:10.2514/1.A32039')
            self.assertEqual(df.ceda_download_url, 'http://browse.ceda.ac.uk/browse/badc/cmip5/' + df.name)

    def test_07_publish_to_esgf(self):
        pass


    def _make_data_submission(self):
        """
        Create files and a data submission. Returns an DataSubmissionForTests
        object.
        """
        test_dsub = datasets.test_data_submission
        test_dsub.create_test_files()

        dsub = get_or_create(DataSubmission, status=STATUS_VALUES.ARRIVED,
                   incoming_directory=test_dsub.INCOMING_DIR,
                   directory=test_dsub.INCOMING_DIR)

        for dfile_name in test_dsub.files:
            path = os.path.join(test_dsub.INCOMING_DIR, dfile_name)
            m = metadata = _extract_file_metadata(path)

            proj = get_or_create(Project, short_name="CMIP6", full_name="6th Coupled Model Intercomparison Project")
            var = get_or_create(Variable, var_id=m["var_id"], long_name="Really good variable", units="1")
            climate_model = get_or_create(ClimateModel, short_name=m["climate_model"], full_name="Really good model")
            experiment = get_or_create(Experiment, short_name=m["experiment"], full_name="Really good experiment")

            dfile = DataFile.objects.create(name=dfile_name, incoming_directory=test_dsub.INCOMING_DIR,
                directory=test_dsub.INCOMING_DIR, size=os.path.getsize(path),
                project=proj, climate_model=climate_model,
                experiment=experiment, variable=var, frequency=m["frequency"],
                start_time=make_aware(m["start_time"], timezone=pytz.utc, is_dst=False),
                end_time=make_aware(m["end_time"], timezone=pytz.utc, is_dst=False),
                data_submission=dsub, online=True)

        return test_dsub


def get_suite(tests):
    suite = unittest.TestSuite()
    for test in tests:
        suite.addTest(TestWorkflows(test))

    return suite


if __name__ == "__main__":

    limited_suite = False
    limited_suite = True

    if limited_suite:
        tests = ['test_01_data_request', 'test_02_data_submission',
            'test_03_move_files_to_tape', 'test_04_restore_from_tape',
            'test_05_create_data_issue', 'test_06_ingest_to_ceda',
            'test_07_publish_to_esgf']
        suite = get_suite(tests)
        unittest.TextTestRunner(verbosity=2).run(suite)
    else:
        unittest.main()
