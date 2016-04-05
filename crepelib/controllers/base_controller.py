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
        self.tasks = {"pending": [], "running": None, "completed_cache": []}

    def start(self):
        self._run()

    def _run(self):
        x = 0

        while 1:
            x += 5
            tasks = self._get_pending_tasks()
            self.tasks["pending"].extend(tasks)

            for task in tasks:

                if 1:
                    self.tasks["running"] = task
                    self.tasks["pending"].remove(task)
                    log.info("Beginning to run %d tasks..." % len(tasks))

                    self.do_task(task)
                    log.info("Completed task: %s" % task)

                    self.tasks["completed_cache"].append(task)
                    self.tasks["running"] = None
                else:
                    self.rollback(task)

            time.sleep(5)

    def _get_pending_tasks(self):
        return self.get_tasks_from_db()

    def get_tasks_from_db(self):
        tasks = api.get_next_actions(self.__class__)
        # Register tasks as being managed by this controller
        for task in tasks:
            status_value = {"do": STATUS_VALUES.DOING,
                            "undo": STATUS_VALUES.UNDOING}[task.action_type]
            api.set_dataset_status(task.dataset, self.__class__, status_value)


    @decwrap
    def do_task(self, task):
        log.info("Running task: %s" % task)
        if task == "BAD":
            raise Exception("Failed: %s" % task)

        if 1:
            self.run_do(task)
        else:
            raise Exception("Failed: %s" % task)

    def undo_task(self, task):
        log.info("Running task: %s" % task)
        if task == "BAD":
            raise Exception("Failed: %s" % task)

        if 1:
            self.run_undo(task)
        else:
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