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


def _run_controller(controller):
    cont = controller()
    cont.start()

def test_workflow():
    # 1. Create a Chain consisting of two controller ("A" and "B")
    chain1 = create_chain("TestChain1", [AController, BController])
    chain2 = create_chain("TestChain2", [BController])

    dir1 = "/inc1"
    dir2 = "/inc2"

    # 2. Add 2 Datasets that use that chain
    d1 = get_or_create(Dataset, incoming_dir=dir1, name="A.a1.v1", chain=chain1,
                       arrival_time=timezone.now(), processing_status=PROCESSING_STATUS_VALUES.IN_PROGRESS,
                       is_withdrawn=False)
    d2 = get_or_create(Dataset, incoming_dir=dir2, name="B.a2.v1", chain=chain2,
                       arrival_time=timezone.now(), processing_status=PROCESSING_STATUS_VALUES.IN_PROGRESS,
                       is_withdrawn=False)

    # 3. Set the Status for each dataset
    #set_empty(d1)
    #set_empty(d2)

    # 4. Add some files to each
    insert(File, name="A_f1", directory=dir1, size=10, dataset=d1)
    insert(File, name="A_f2", directory=dir1, size=10, dataset=d1)
    insert(File, name="B_f1", directory=dir2, size=10, dataset=d2)

    # 5. Invoke controllers to start running
    pA = Process(target=_run_controller, args=(AController,))
    pA.start()
    time.sleep(10)
    pB = Process(target=_run_controller, args=(BController,))
    pB.start()

    # 6. Sleep for 10 seconds
    time.sleep(1)
    #print dir(pA)

    # 7. Analyse results
    print "Remember to run: python clear.py - TO DELETE ALL!"

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

    @classmethod
    def tearDownClass(self):
        self.log.info("Removing all content after running tests.")

class TestWorkflows(CrepeBaseTest):

    def setUp(self):
        # For each test, run controllers
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

    def test_ds1(self):
        # Create test files
        ds = datasets.d1
        ds.create_test_files()

        # Create a Chain consisting of three controllers
        scheme = "CMIP6-MOHC"
        chain = create_chain(scheme, [QCController, IngestController, PublishController],
                             completed_externally=False)
        incoming_dir = get_dir_from_scheme(scheme, "incoming_dir")

        # Add the dataset to the db
        dataset = get_or_create(Dataset, incoming_dir=incoming_dir, name=ds.id,
                                chain=chain, arrival_time=timezone.now())

        # Add files
        for fname in ds.files:
            insert(File, name=fname, directory=incoming_dir, size=10, dataset=dataset)

        # Sleep and then assert that all are completed
        time.sleep(10)
        self._assert_all_completed(dataset)

    def test_ds2(self):
        # Create test files
        ds = datasets.d2
        ds.create_test_files()

        # Create a Chain consisting of three controllers
        scheme = "CMIP6-MOHC"
        chain = create_chain(scheme, [QCController, IngestController, PublishController],
                             completed_externally=False)
        incoming_dir = get_dir_from_scheme(scheme, "incoming_dir")

        # Add the dataset to the db
        dataset = get_or_create(Dataset, incoming_dir=incoming_dir, name=ds.id,
                                chain=chain, arrival_time=timezone.now())

        # Add files
        for fname in ds.files:
            insert(File, name=fname, directory=incoming_dir, size=10, dataset=dataset)

        # Sleep and then assert that all are completed
        time.sleep(10)
        self._failed_at(dataset, stage=1)

    def test_ds3(self):
        pass

    def test_ds4(self):
        pass

    def test_ds5(self):
        pass

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

    def _failed_at(self, dataset, stage):
        self.log.warn("Checking dataset FAILED at stage %d: %s" % (stage, dataset.name))
        chain = dataset.chain
        stages = ["dummy"] + get_ordered_process_stages(chain)

        stage_name = stages[stage]
        self.tlog("%s, %s --> %s" % (dataset, stage_name, get_dataset_status(dataset, stage_name)))
        assert get_dataset_status(dataset, stage_name) == STATUS_VALUES.FAILED

        self.log.warn("COMPLETED FAILURE CHECK!")

if __name__ == "__main__":

    #tester = TestWorkflows()
    #tester.test_ds1()
    unittest.main()