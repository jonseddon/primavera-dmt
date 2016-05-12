import time
from multiprocessing import Process
import unittest
import logging
import inspect

from django.utils import timezone

import test.test_datasets as datasets
from test.mock_controllers import *
from vocabs import *
from config import config
from config import get_dir_from_scheme


from crepe_app.models import *
from crepe_app.models import __all__ as all_class_names

classes = [eval(cls_name) for cls_name in all_class_names]

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

def _run_controller(controller):
    cont = controller()
    cont.start()

class CrepeBaseTest(unittest.TestCase):

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

class TestWorkflows(CrepeBaseTest):

    def setUp(self):
        # For each test: clear db; run controllers
        _clear_db()

        # Set up global settings
        self.settings = Settings.objects.create(is_paused=False)

        # Invoke controllers to start running
        self.procs = {}
        for contr in (QCController, IngestController, PublishController):
            self.tlog("Starting controller: %s" % contr.name)
            self.procs[contr.name] = Process(target=_run_controller, args=(contr,))
            self.procs[contr.name].start()
            time.sleep(0.4)

    def tearDown(self):
        # For each test, stop controllers
        keys = self.procs.keys()

        for contr_name in keys:
            self.tlog("Closing down controller: %s" % contr_name)
            self.procs[contr_name].terminate()
            del self.procs[contr_name]

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

    def test_01(self):
        # d1 - 3 good files
        ds = datasets.d1
        dataset = self._common_dataset_setup(ds)

        # Sleep and then assert that all are completed
        time.sleep(10)
        self._assert_all_completed(dataset)

    def test_02(self):
        # d2 - 2 good files, 1 bad file
        ds = datasets.d2
        dataset = self._common_dataset_setup(ds)

        # Sleep and then assert that all are completed
        time.sleep(10)
        self._assert_failed_at(dataset, stage=1)

    def test_03(self):
        # d3 - 3 good files, but IOError raised during Ingest process
        ds = datasets.d3
        dataset = self._common_dataset_setup(ds)

        # Sleep and then assert that all are completed
        time.sleep(10)
        self._assert_failed_at(dataset, stage=2)

    def test_04(self):
        # d4 - 3 good files, but withdraw afterwards
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

        time.sleep(30)
        self._assert_empty(dataset)

    def test_05(self):
        # d5 - 3 good files, but withdraw during QC
        ds = datasets.d5
        self._common_withdraw_test("QCController", ds)

    def test_06(self):
        # d6 - 3 good files, but withdraw during Ingest
        ds = datasets.d5
        self._common_withdraw_test("IngestController", ds)

    def test_07(self):
        # d7 - 3 good files, but withdraw during Publish
        ds = datasets.d5
        self._common_withdraw_test("PublishController", ds)

    def test_08(self):
        # d8 - 2 good files, 1 bad file - withdraw after failure
        ds = datasets.d2
        dataset = self._common_dataset_setup(ds)

        # Sleep briefly so that it is part through running and then withdraw mid-transaction
        time.sleep(10)
        dataset.is_withdrawn = True
        dataset.save()

        time.sleep(10)
        self._assert_empty(dataset)
        
    def test_09(self):
        # 09 - during ingest put on hold, check stuck in Publish stage
        ds = datasets.d1
        dataset = self._common_dataset_setup(ds)
        controller_name = "PublishController"

        # Sleep briefly so that it is part through running and then withdraw mid-transaction
        expected_statuses = (STATUS_VALUES.PENDING_DO, STATUS_VALUES.DOING)
        while 1:
            if get_dataset_status(dataset, controller_name) in expected_statuses:
                self.settings.is_paused = True
                self.settings.save()
                break
            time.sleep(0.01)

        time.sleep(10)
        assert get_dataset_status(dataset, controller_name) in expected_statuses


    def test_10(self):
        # 10 - during the ingest stage the Ingest controller is killed.
        # It then gets re-started and it needs to detect that it is supposed to be
        # currently processing something and needs to resolve that before continuing.

        # d6 - 3 normal files - 1 to be detected by mock ingest controller and to slow
        # down so that we can simulate the controller switching off.
        ds = datasets.d6
        dataset = self._common_dataset_setup(ds)

        contr = IngestController

        # Sleep briefly so that the ingestion is underway - then kill the Ingest
        expected_status = STATUS_VALUES.DOING
        while 1:
            self.tlog("Waiting for task to be 'DOING'...")
            if get_dataset_status(dataset, contr.name) == expected_status:
                self.tlog("Terminating %s" % contr.name, "WARN")
                self.procs[contr.name].terminate()
                break
            time.sleep(1)

        # Sleep for a while and then re-start the controller
        time.sleep(3)
        self.tlog("Re-starting %s" % contr.name, "WARN")
        self.procs[contr.name] = Process(target=_run_controller, args=(contr,))
        self.procs[contr.name].start()

        # Sleep and then assert that all are completed
        time.sleep(30)
        self._assert_all_completed(dataset)

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

if __name__ == "__main__":

    unittest.main()
