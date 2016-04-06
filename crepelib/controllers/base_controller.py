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

    purpose = "Base class only"

    def __init__(self):
        self.running_task = None
        self.name = self.__class__.__name__
#        self.tasks = {"pending": [], "running": None, "completed_cache": []}

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

    def _run(self):
        while 1:
            self.load_tasks()
            log.warn("IIIIIIIIII:%s" % self.tasks)
            #self.tasks["pending"].extend(tasks)

            for task in self.tasks:
                self._set_as_running(task)
                log.info("Starting task: %s" % task)
                runner = self._map_method(task)
                runner(task)
                self._set_as_completed(task)
                log.info("Completed task: %s" % task)

            log.info("Sleeping for a while: %s" % self.name)
            time.sleep(5)

    def load_tasks(self):
        self.tasks = self.get_tasks_from_db()

    def get_tasks_from_db(self):
        tasks = api.get_next_tasks(self.__class__)
        # Register tasks as being managed by this controller and pending
        for task in tasks:
            status_value = "PENDING_%s" % task.action_type.upper()
            api.set_dataset_status(task.dataset, self.name, status_value)

        return tasks

    @decwrap
    def do_task(self, task):
        log.info("Running task: %s" % task)
        try:
            self.run_do(task)
        except:
            raise Exception("Failed: %s" % task)

    def undo_task(self, task):
        log.info("Running task: %s" % task)
        try:
            self.run_undo(task)
        except:
            raise Exception("Failed: %s" % task)

    def rollback(self, task):
        log.warn("Rolling back...")

        running_task = self.tasks["running"]

        if running_task != task:
            raise Exception("Inconsistency: requested rollback of task that is different from running task: %s != %s"
                            % (task, running_task))

        self.tasks["running"] = None
        self.run_undo(task)

        if task in self.tasks["completed_cache"]:
            self.tasks["completed_cache"].remove(task)

        self.tasks["pending"].insert(0, task)
        log.warn("Rolled back: %s" % task)

    def run_do(self, task):
        raise NotImplementedError("Base class does not implement 'run_do' method.")

    def run_undo(self, task):
        raise NotImplementedError("Base class does not implement 'run_undo' method.")

    def advance(self, task):
        # Tell database that the task is ready for the next stage
        pass

if __name__ == "__main__":

    # Test only
    bc = BaseController()
    bc.start()