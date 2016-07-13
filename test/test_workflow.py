import os
import datetime
import unittest
import logging
import inspect

import test.test_datasets as datasets
from vocabs import *
from config import config
from config import get_dir_from_scheme

from pdata_app.utils.dbapi import *
from pdata_app.models import *
from pdata_app.models import __all__ as all_class_names

classes = [eval(cls_name) for cls_name in all_class_names]

# Utility functions for test workflow
def _clear_db():
    print "Clearing out database at start..."
    for cls in classes:
        cls.objects.all().delete()

    print "Deleted all!\n"

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


class PdataBaseTest(unittest.TestCase):

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
        # For each test: clear db
        _clear_db()

        # Set up global settings
        self.settings = Settings.objects.create(is_paused=False)

    def tearDown(self):
        pass

    def _common_dataset_setup(self, ds):
        "Common setup for all workflow tests - depending on dataset provided as ``ds``."
        # Create test files
        ds.create_test_files()

        # Create a Chain consisting of three controllers
        scheme = "CMIP6-MOHC"
        chain = create_chain(scheme, [QCController, IngestController, PublishController], completed_externally=False)
        incoming_dir = get_dir_from_scheme(scheme, "incoming_dir")

        # Add the dataset to the db
        dataset = get_or_create(Dataset, incoming_dir=incoming_dir, name=ds.id,
                                chain=chain, arrival_time=timezone.now())

        # Add files
        for fname in ds.files:
            insert(File, name=fname, directory=incoming_dir, size=10, dataset=dataset)

        return dataset

    def test_00(self):
        # Create a Data Submission and add files to it
        self.tlog("STARTING: test_00")
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
                                            start_time=m["start_time"], end_time=m["end_time"],
                                            data_submission=dsub, online=True)

        # Make some assertions
        for dfile_name in test_dsub.files:
            self.assertEqual(dfile_name, DataFile.objects.filter(name=dfile_name).first().name)

    def test_01(self):
        # d1 - 3 good files
        self.tlog("STARTING: test_01")
        ds = datasets.d1
        dataset = self._common_dataset_setup(ds)

        # Sleep and then assert that all are completed
        time.sleep(10)
        self._assert_all_completed(dataset)

    def test_02(self):
        # d2 - 2 good files, 1 bad file
        self.tlog("STARTING: test_02")
        ds = datasets.d2
        dataset = self._common_dataset_setup(ds)

        # Sleep and then assert that all are completed
        time.sleep(10)
        self._assert_failed_at(dataset, stage=1)

    def test_03(self):
        # d3 - 3 good files, but IOError raised during Ingest process
        self.tlog("STARTING: test_03")
        ds = datasets.d3
        dataset = self._common_dataset_setup(ds)

        # Sleep and then assert that all are completed
        time.sleep(10)
        self._assert_failed_at(dataset, stage=2)

    def test_04(self):
        # d4 - 3 good files, but withdraw afterwards
        self.tlog("STARTING: test_04")
        ds = datasets.d4
        dataset = self._common_dataset_setup(ds)

        # Sleep and then assert that all are completed
        time.sleep(10)
        self._assert_all_completed(dataset)

        # Now withdraw and check all undo actions work
        dataset.is_withdrawn = True
        dataset.save()

        time.sleep(20)
        self._assert_empty(dataset)

    def _common_withdraw_test(self, controller_name, ds):
        dataset = self._common_dataset_setup(ds)

        # Sleep briefly so that it is part through running and then withdraw mid-transaction
        while 1:
            if get_dataset_status(dataset, controller_name) in \
                    (STATUS_VALUES.PENDING_DO, STATUS_VALUES.DOING, STATUS_VALUES.DONE):
                break
            time.sleep(1)

        dataset.is_withdrawn = True
        dataset.save()

        time.sleep(40)
        self._assert_empty(dataset)

    def test_05(self):
        # d5 - 3 good files, but withdraw during QC
        self.tlog("STARTING: test_05")
        ds = datasets.d5
        self._common_withdraw_test("QCController", ds)

    def test_06(self):
        # d6 - 3 good files, but withdraw during Ingest
        self.tlog("STARTING: test_06")
        ds = datasets.d5
        self._common_withdraw_test("IngestController", ds)


    def _assert_all_completed(self, dataset):
        self.log.warn("Checking dataset is COMPLETED: %s" % dataset.name)
        chain = dataset.chain
        if chain.completed_externally:
            assert dataset.processing_status == PROCESSING_STATUS_VALUES.COMPLETED

        for stage_name in get_ordered_process_stages(chain):
            self.tlog("%s, %s --> %s" % (dataset, stage_name, get_dataset_status(dataset, stage_name)))
            assert get_dataset_status(dataset, stage_name) == STATUS_VALUES.DONE
            self.log.warn("CHECKED PROCESS STATUS: %s, %s" % (dataset.name, stage_name))

        self.log.warn("COMPLETED COMPLETION CHECK!")

    def _assert_failed_at(self, dataset, stage):
        self.log.warn("Checking dataset FAILED at stage %d: %s" % (stage, dataset.name))
        chain = dataset.chain
        stages = ["dummy"] + get_ordered_process_stages(chain)

        stage_name = stages[stage]
        self.tlog("%s, %s --> %s" % (dataset, stage_name, get_dataset_status(dataset, stage_name)))
        assert get_dataset_status(dataset, stage_name) == STATUS_VALUES.FAILED

        self.log.warn("COMPLETED FAILURE CHECK!")

    def _assert_empty(self, dataset):
        self.log.warn("Checking dataset is EMPTY: %s" % dataset.name)
        chain = dataset.chain

        for stage_name in get_ordered_process_stages(chain):
            self.tlog("%s, %s --> %s" % (dataset, stage_name, get_dataset_status(dataset, stage_name)))
            assert get_dataset_status(dataset, stage_name) == STATUS_VALUES.EMPTY
            self.log.warn("CHECKED PROCESS STATUS: %s, %s" % (dataset.name, stage_name))

        self.log.warn("COMPLETED EMPTY CHECK!")


def get_suite(tests):
    suite = unittest.TestSuite()
    for test in tests:
        suite.addTest(TestWorkflows(test))

    return suite

if __name__ == "__main__":

    limited_suite = False
    limited_suite = True

    if limited_suite:
        tests = ['test_00']
        suite = get_suite(tests)
        unittest.TextTestRunner(verbosity=2).run(suite)
    else:
        unittest.main()
