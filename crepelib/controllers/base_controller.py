import time
import logging
logging.basicConfig()
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

import crepe_app.utils.dbapi as api
from vocabs import *

# Decorators
def decwrap(func):
    def wrapper(controller, *args, **kwargs):
        log.info("Start: %s" % func.__name__)
        #log.info("ARGS: %s, %s" % (str(controller), str(kwargs)))
        result = func(controller, *args, **kwargs)

        log.info("End: %s" % func.__name__)
        return result

    return wrapper



class BaseController(object):

    name = "BaseController"
    purpose = "Base class only"

    def __init__(self):
        self._setup_logger()

    def _setup_logger(self):
        self.log = logging.getLogger(self.name)
        self.log.setLevel(logging.INFO)

    def start(self):
        self._run()

    def _map_running_status(self, task):
        return {"do": STATUS_VALUES.DOING,
                "undo": STATUS_VALUES.UNDOING}[task.action_type]

    def _map_completed_status(self, task):
        return {"do": STATUS_VALUES.DONE,
                "undo": STATUS_VALUES.EMPTY}[task.action_type]

    def _map_method(self, task):
        return {"do": self.do_task,
                "undo": self.undo_task}[task.action_type]

    def _set_as_running(self, task):
        self.running = task
        api.set_dataset_status(task.dataset, self.name, self._map_running_status(task))

    def _set_as_completed(self, task):
        self.running = None
        api.set_dataset_status(task.dataset, self.name, self._map_completed_status(task))

    def _set_as_failed(self, task):
        self.running = None
        api.set_dataset_status(task.dataset, self.name, STATUS_VALUES.FAILED)

    def _run(self):
        while 1:
            self.load_tasks()
            self.log.warn("Tasks picked up: %s" % self.tasks)

            for task in self.tasks:
                self._set_as_running(task)
                runner = self._map_method(task)
                result = runner(task)

                while api.is_paused():
                    print "...............PAUSED................"
                    time.sleep(5)

                if result:
                    self._set_as_completed(task)
                else:
                    self._set_as_failed(task)

            self.log.info("Sleeping for a while: %s" % self.name)
            time.sleep(5)

    def load_tasks(self):
        self.tasks = self.get_tasks_from_db()

    def get_tasks_from_db(self):
        tasks = api.get_next_tasks(self.name)
        # Register tasks as being managed by this controller and pending
        for task in tasks:
            status_value = "PENDING_%s" % task.action_type.upper()
            api.set_dataset_status(task.dataset, self.name, status_value)

        return tasks

    #@decwrap
    def do_task(self, task):
        self.log.info("Running task: %s" % task)
        try:
            self._run_do(task)
            self.log.info("Completed DO: %s" % task)
            self.log_event(task.dataset, "DO", "SUCCESS")
            return True
        except Exception, err:
            self.log.warn("Failed DO: %s" % task)
            self.log_event(task.dataset, "DO", "FAILURE")
            return False

    def undo_task(self, task):
        self.log.info("Running task: %s" % task)
        try:
            self._run_undo(task)
            self.log_event(task.dataset, "UNDO", "SUCCESS")
            return True
        except Exception, err:
            self.log.warn("Failed UNDO: %s" % task)
            self.log_event(task.dataset, "UNDO", "FAILURE")
            return False

    def _run_do(self, task):
        raise NotImplementedError("Base class does not implement 'run_do' method.")

    def _run_undo(self, task):
        raise NotImplementedError("Base class does not implement 'run_undo' method.")

    def advance(self, task):
        # Tell database that the task is ready for the next stage
        pass

    def log_event(self, dataset, action_type, outcome):
        api.add_event(self.name, dataset, action_type, outcome)

if __name__ == "__main__":

    # Test only
    bc = BaseController()
    bc.start()