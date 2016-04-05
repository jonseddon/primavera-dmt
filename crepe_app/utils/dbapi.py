from crepe_app.models import *
from vocabs import *


def insert(cls, **props):
    obj = cls(**props)
    obj.save()
    return obj


def match_one(cls, **props):
    match = cls.objects.filter(**props)
    if match.count() == 1:
        return match.first()
    return False


def get_or_create(cls, **props):
    obj, created = cls.objects.get_or_create(**props)
    return obj


def exists(cls, **props):
    if cls.objects.filter(**props):
        return True
    else:
        return False


def count(cls):
    return cls.objects.count()


def get_dataset_files(dataset):
    return dataset.file_set()


def create_chain(name, controllers):
    chain, created = Chain.objects.get_or_create(name=name, completed_externally=False)
    print controllers
    for (i, controller) in enumerate(controllers):
        process_stage, created = ProcessStage.objects.get_or_create(name=controller.__name__)
        print process_stage, created
        pos = i + 1
        psic, created = ProcessStageInChain.objects.get_or_create(process_stage=process_stage,
                                                                  chain=chain, position=pos)
        print psic, psic.process_stage.name

    return chain


def advance_process_status(dataset):
    # Identify current process stage of dataset
    # Identify next process stage for dataset
    # Advance the Status record for this dataset to the next
    pass


def is_withdrawn(dataset):
    return dataset.is_withdrawn


def set_empty(dataset):
    statuses = dataset.status_set.all()
    for status in statuses:
        status.status_value = STATUS_VALUES.EMPTY
        status.save()


def find_chains_using_controller(controller):
    return Chain.objects.filter(processstageinchain__process_stage__name=controller.__name__)


def get_ordered_process_stages(chain):
    stages = [psic.process_stage for psic in chain.processstageinchain_set.order_by(position)]
    return stages


def get_previous_stage(controller, chain):
    stages = get_ordered_process_stages(chain)
    if controller == stages[0]:
        return None
    return chain[stages.index(controller) - 1]

def get_next_stage(controller, chain):
    stages = get_ordered_process_stages(chain)
    if controller == stages[-1]:
        return None
    return chain[stages.index(controller) + 1]

def get_datasets_using_chain(chain):
    return Dataset.objects.filter(chain=chain).order_by("arrival_time")

def get_dataset_status(dataset, controller):
    return dataset.status_set.get(process_stage__name=controller.__name__).status_value

def set_dataset_status(dataset, controller, status_value):
    status = dataset.status_set.get(process_stage__name=controller.__name__)
    status.status_value = status_value
    status.save()

def is_ready_to_do(dataset, controller, chain):
    previous_controller = get_previous_stage(controller, chain)
    current_controller = controller

    if (not previous_controller or (get_dataset_status(previous_controller) == STATUS_VALUES.DONE)) and \
                    get_dataset_status(current_controller) == STATUS_VALUES.EMPTY:
        return True

def is_ready_to_undo(dataset, controller, chain):
    next_controller = get_next_stage(controller, chain)
    current_controller = controller

    if get_dataset_status(next) == STATUS_VALUES.EMPTY and get_dataset_status(current_controller) == STATUS_VALUES.DONE:
        set_current_status(STATUS_VALUES.UNDOING)
        undo(current_controller)

class Task(object):
    ALLOWED_TYPES = ("do", "undo")

    def __init__(self, id, action_type):
        self.id = id
        if action_type not in Task.ALLOWED_TYPES:
            raise Exception("Direction must be one of: %s, not '%s'." % (str(Task.ALLOWED_TYPES), action_type))

        self.action_type = action_type

def get_next_actions(controller):
    chains = find_chains_using_controller(controller)
    print "FOUND CHAINS:", chains
    actions = []

    for chain in chains:

        datasets = get_datasets_using_chain(chain)

        for dataset in datasets:

            # If dataset is withdrawn we can ignore it
            if dataset.is_withdrawn == False:
                continue

            # If ready to "do" or "undo" then add to action list
            if is_ready_to_do(dataset, controller, chain):
                actions.append(Task(dataset, "do"))
            elif is_ready_to_undo(dataset, controller, chain):
                actions.append(Task(dataset, "undo"))
