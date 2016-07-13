import glob, os
from datetime import datetime

from pdatalib.controllers.base_controller import BaseController
from pdata_app.models import *
import pdata_app.utils.dbapi as api

class LocateController(BaseController):

    purpose = """
Locate files that have arrived in the incoming directory:
 * deduce their datasets and identifiers
 * record in the database
"""

    def _get_pending_tasks(self):
        tasks = glob.glob('test_data/incoming/*.txt')
        return tasks

    def run_task(self, task):
        fpath = task
        dr, fname = os.path.split(fpath)
        ds_id = fname.split("_")[1].split(".")[0]

        chain = api.get_or_create(Chain, name="test_chain_1", completed_externally=False)

        dataset = api.match_one(Dataset, name=ds_id, incoming_dir=dr)
        if not dataset:
            dataset = api.insert(Dataset, name=ds_id, incoming_dir=dr, chain=chain,
                                    arrival_time=datetime.now(), processing_status='IN_PROGRESS')

        if api.exists(File, name=fname, directory=dr, dataset=dataset):
            return

        size = os.path.getsize(fpath)
        api.insert(File, name=fname, directory=dr, size=size, dataset=dataset)
        print "COUNT:", api.count(File)
