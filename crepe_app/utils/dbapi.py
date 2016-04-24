from django.utils import timezone

from crepe_app.models import *
from crepelib.task import Task
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


def get_checksum(file, checksum_type="MD5"):
    return file.checksum_set.get(checksum_type=checksum_type).checksum_value


def count(cls):
    return cls.objects.count()


def get_dataset_files(dataset):
    return dataset.file_set.all()


def create_chain(name, controllers, completed_externally=False):
    chain, created = Chain.objects.get_or_create(name=name, completed_externally=completed_externally)

    for (i, controller) in enumerate(controllers):
        process_stage, created = ProcessStage.objects.get_or_create(name=controller.__name__)
        print process_stage, created
        pos = i + 1
        psic, created = ProcessStageInChain.objects.get_or_create(process_stage=process_stage,
                                                                  chain=chain, position=pos)
        print psic, psic.process_stage.name

    return chain


def add_event(controller_name, dataset, action_type, outcome):
    succeeded = {"SUCCESS": True, "FAILURE": False}[outcome]
    process_stage = ProcessStage.objects.get(name=controller_name)

    Event.objects.create(dataset=dataset, process_stage=process_stage, action_type=action_type,
                         succeeded=succeeded, date_time=timezone.now(), message="")


def advance_process_status(dataset):
    # Identify current process stage of dataset
    # Identify next process stage for dataset
    # Advance the Status record for this dataset to the next
    pass


def is_withdrawn(dataset):
    return dataset.is_withdrawn


def find_chains(stage_name):
    return Chain.objects.filter(processstageinchain__process_stage__name=stage_name)

def get_ordered_process_stages(chain):
    stages = [psic.process_stage.name for psic in chain.processstageinchain_set.order_by("position")]
    return stages

def get_previous_stage(stage_name, chain):
    stages = get_ordered_process_stages(chain)
    if stage_name == stages[0]:
        return None
    return stages[stages.index(stage_name) - 1]

def get_next_stage(stage_name, chain):
    stages = get_ordered_process_stages(chain)
    if stage_name == stages[-1]:
        return None
    return stages[stages.index(stage_name) + 1]

def is_final_stage(stage_name, scheme):
    chain = Chain.objects.get(name=scheme)
    return get_ordered_process_stages(chain)[-1] == stage_name

def get_datasets_using_chain(chain):
    return Dataset.objects.filter(chain=chain).order_by("arrival_time")

def get_dataset_status(dataset, stage_name):
    "Return dataset status if it can be read; or return None."
    try:
        status = dataset.status_set.get(process_stage__name=stage_name).status_value
        return status
    except:
        return None

def set_dataset_status(dataset, stage_name, status_value):
    # NOTE: Derives the dataset.processing_status value from the stage status value
    if dataset.processing_status == PROCESSING_STATUS_VALUES.PAUSED:
        # Keep paused
        processing_status = PROCESSING_STATUS_VALUES.PAUSED
    elif status_value == STATUS_VALUES.EMPTY:
        processing_status = PROCESSING_STATUS_VALUES.NOT_STARTED
    elif status_value == STATUS_VALUES.DONE:
        processing_status = PROCESSING_STATUS_VALUES.COMPLETED
    else:
        processing_status = PROCESSING_STATUS_VALUES.IN_PROGRESS

    # Set processing status
    dataset.processing_status = processing_status
    dataset.save()

    # Set status for stage name for this dataset
    status = dataset.status_set.get(process_stage__name=stage_name)
    status.status_value = status_value
    status.save()

def is_ready_to_do(dataset, stage_name, chain):
    if dataset.is_withdrawn: return False

    previous = get_previous_stage(stage_name, chain)
    current_stage = stage_name

    if (not previous or (previous and get_dataset_status(dataset, previous) == STATUS_VALUES.DONE)) and \
                    get_dataset_status(dataset, current_stage) == STATUS_VALUES.EMPTY:
        return True

def is_ready_to_undo(dataset, stage_name, chain):
    next_stage = get_next_stage(stage_name, chain)
    current_stage = stage_name

    if dataset.is_withdrawn:
        if (get_dataset_status(dataset, current_stage) in (STATUS_VALUES.DONE, STATUS_VALUES.FAILED)) and \
                (not next_stage or (get_dataset_status(dataset, next_stage) == STATUS_VALUES.EMPTY)):
            return True

#    if ((not next_stage and dataset.is_withdrawn) or
#                (get_dataset_status(dataset, next_stage) == STATUS_VALUES.EMPTY)) and \
#                    get_dataset_status(dataset, current_stage) == STATUS_VALUES.DONE:
#        return True

def get_next_tasks(stage_name):
    chains = find_chains(stage_name)
    tasks = []

    for chain in chains:

        datasets = get_datasets_using_chain(chain)

        for dataset in datasets:
            print "Checking:", dataset.name
            # Add tasks to list
            # If ready to "do" or "undo" then add to action list
            if is_ready_to_do(dataset, stage_name, chain):
                tasks.append(Task(chain.name, dataset, "do"))
            elif is_ready_to_undo(dataset, stage_name, chain):
                tasks.append(Task(chain.name, dataset, "undo"))

    return tasks
