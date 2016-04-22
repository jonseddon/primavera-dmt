from django.shortcuts import render

# Create your views here.
from django.shortcuts import render_to_response

from crepe_app.models import *
from crepe_app.utils import dbapi


def view_files(request):
    records = File.objects.all()
    return render_to_response('files.html', {'request': request,
                                             'records': records})


def view_chains(request):
    records = Chain.objects.all()
    return render_to_response('chains.html', {'request': request,
                                              'records': records})


def view_datasets(request):
    datasets = Dataset.objects.all()
    records = []

    for dataset in datasets:
        stage_names = dbapi.get_ordered_process_stages(dataset.chain)
        stages = [(stage_name, dbapi.get_dataset_status(dataset, stage_name)) for stage_name in stage_names]
        records = [{"name": dataset.name, "processing_status": dataset.processing_status,
                    "stages": stages, "files": dataset.file_set.all()}]

    return render_to_response('datasets.html', {'request': request,
                                                'records': records})


def view_events(request):
    events = Event.objects.order_by("date_time").reverse()
    records = []
    success_map = {True: "SUCCEEDED", False: "FAILED"}
    font_map = {True: "black", False: "red"}

    headings = [("Time", 100), ("Dataset", 200), ("Process Stage", 100), ("Message", 50),
                ("Action Type", 80), ("Succeeded?", 80)]
    for e in events:
        records.append({"content": (e.date_time, e.dataset, e.process_stage.name.replace("Controller", ""),
                        e.message, e.action_type, success_map[e.succeeded]),
                        "style": font_map[e.succeeded]})

    return render_to_response('events.html', {'request': request, 'headings': headings, 'records': records})


def view_home(request):
    return render_to_response('home.html', {'request': request})
