from multiprocessing import Process
import time

from django.utils import timezone

from crepe_app.models import *
from crepe_app.utils.dbapi import *
from test.mock_controllers import *
from vocabs import *

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
    set_empty(d1)
    set_empty(d2)

    # 4. Add some files to each
    insert(File, name="A_f1", directory=dir1, size=10, dataset=d1)
    insert(File, name="A_f2", directory=dir1, size=10, dataset=d1)
    insert(File, name="B_f1", directory=dir2, size=10, dataset=d2)

    # 5. Invoke controllers to start running
    pA = Process(target=_run_controller, args=(AController,))
    pA.start()
    time.sleep(0.2)
    pB = Process(target=_run_controller, args=(BController,))
    pB.start()

    # 6. Sleep for 10 seconds
    time.sleep(1)

    # 7. Analyse results
    print "Remember to run: python clear.py - TO DELETE ALL!"

if __name__ == "__main__":

    test_workflow()