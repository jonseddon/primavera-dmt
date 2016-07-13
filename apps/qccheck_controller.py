import glob, os
from datetime import datetime

from pdatalib.controllers.base_controller import BaseController
from pdata_app.pdata_exceptions.pdata_exceptions import *
from pdata_app.models import *
import pdata_app.utils.dbapi as api

class QCCheckController(BaseController):

    purpose = """
Check that each file in a dataset is valid:
 * check the file name matches the required convention
 * check that the variable ID in the file name matches that in the file
"""

    def run_task(self, task):
        # Each task is identified by a dataset ID
        ds_id = task
        dataset = api.match_one(Dataset, name=ds_id)
        if not dataset:
            raise PdataDatasetError("Must tell people about this error...cannot find dataset.")

        files = api.get_dataset_files(dataset)

        try:
            for file in files:
                self._check_contents_of_file(file)
        except Exception, err:
            raise Exception(err)

        self.advance(task)

    def _get_file_contents(self, path):
        f = open(path)
        data = f.read().strip()
        f.close()
        return data

    def _check_contents_of_file(self, file):
        loc = os.path.join(file.directory, file.name)
        var_id_content = self._get_file_contents(loc)
        var_id_fname = file.name.split("_")[0]

        if var_id_content != var_id_fname:
            raise PdataFileContentsError(loc)