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


def count(cls):
    return cls.objects.count()


def get_dataset_files(dataset):
    return dataset.file_set.all()


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

def get_datasets_using_chain(chain):
    return Dataset.objects.filter(chain=chain).order_by("arrival_time")

def get_dataset_status(dataset, stage_name):
    return dataset.status_set.get(process_stage__name=stage_name).status_value

def set_dataset_status(dataset, stage_name, status_value):
    status = dataset.status_set.get(process_stage__name=stage_name)
    status.status_value = status_value
    status.save()

def is_ready_to_do(dataset, stage_name, chain):
    previous_stage = get_previous_stage(stage_name, chain)
    current_stage = stage_name

    if (not previous_stage or (get_dataset_status(dataset, previous_stage) == STATUS_VALUES.DONE)) and \
                    get_dataset_status(dataset, current_stage) == STATUS_VALUES.EMPTY:
        return True

def is_ready_to_undo(dataset, stage_name, chain):
    next_stage = get_next_stage(stage_name, chain)
    current_stage = stage_name

    if (not next_stage or (get_dataset_status(dataset, next_stage) == STATUS_VALUES.EMPTY)) and \
                    get_dataset_status(dataset, current_stage) == STATUS_VALUES.DONE:
        return True

def get_next_tasks(controller):
    chains = find_chains_using_controller(controller)
    print "FOUND CHAINS:", chains
    tasks = []

    for chain in chains:

        datasets = get_datasets_using_chain(chain)
        print "FOUND DATASETS: %s" % str(datasets)

        for dataset in datasets:

            # If dataset is withdrawn we can ignore it
            if dataset.is_withdrawn == True:
                continue

            # If ready to "do" or "undo" then add to action list
            if is_ready_to_do(dataset, controller.__name__, chain):
                tasks.append(Task(dataset, "do"))
            elif is_ready_to_undo(dataset, controller.__name__, chain):
                tasks.append(Task(dataset, "undo"))
            
    return tasks
