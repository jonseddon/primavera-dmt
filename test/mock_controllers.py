from crepelib.controllers.base_controller import BaseController
from crepe_app.utils.dbapi import *

class AController(BaseController):

    def run_do(self, task):
        get_dataset_files(task.dataset).update(directory="/A")



class BController(BaseController):

    def run_do(self, task):
        get_dataset_files(task.dataset).update(directory="/B")
