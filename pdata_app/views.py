from django.shortcuts import render

# Create your views here.
from django.shortcuts import render_to_response
from django.http import JsonResponse, HttpResponseNotFound

from pdata_app.models import *
from pdata_app.utils import dbapi
from pdata_app.utils.common import get_class_by_name


def view_files(request):
    records = File.objects.all()
    return render_to_response('files.html', {'request': request, 'page_title': 'Files',
                                             'records': records})


def view_chains(request):
    records = Chain.objects.all()
    return render_to_response('chains.html', {'request': request, 'page_title': 'Chains',
                                              'records': records})


def view_datasets(request):
    datasets = Dataset.objects.all()
    records = []

    for dataset in datasets:
        stage_names = dbapi.get_ordered_process_stages(dataset.chain)
        stages = [(stage_name, dbapi.get_dataset_status(dataset, stage_name)) for stage_name in stage_names]
        records = [{"name": dataset.name, "processing_status": dataset.processing_status,
                    "stages": stages, "files": dataset.file_set.all()}]

    return render_to_response('datasets.html', {'request': request,  'page_title': 'Datasets',
                                                'records': records})


def view_events(request):
    events = Event.objects.order_by("date_time").reverse()
    records = []
    success_map = {True: "SUCCEEDED", False: "FAILED"}
    font_map = {True: "black", False: "red"}

    fields = [("Time", "date_time", 100), ("Dataset", "dataset__name", 200),
                ("Process Stage", "process_stage__name", 100), ("Message", "message", 50),
                ("Action Type", "action_type", 80), ("Succeeded?", "succeeded", 80),
                ("Withdrawn?", "dataset__is_withdrawn", 80)]
    for e in events:
        records.append({"content": (e.date_time, e.dataset, e.process_stage.name.replace("Controller", ""),
                        e.message, e.action_type, success_map[e.succeeded], e.dataset.is_withdrawn),
                        "style": font_map[e.succeeded]})

    return render_to_response('events.html', {'request': request,  'page_title': 'Events',
                                              'fields': fields, 'records': records})


def view_home(request):
    return render_to_response('home.html', {'request': request, 'page_title': 'The CEDA Dataset Pipeline App'})


def _match_by_name(model, term):
    model_lookups = {Dataset: "name",
                     File: "name",
                     ProcessStage: "name"}

    prop = model_lookups[model]
    lookup = "%s__icontains" % prop
    return [getattr(obj, prop) for obj in model.objects.filter(**{lookup: term})]

def view_autocomplete_list(request, model_name):
    try:
        model = get_class_by_name(model_name)
    except Exception, err:
        return HttpResponseNotFound(str(err))

    term = request.GET.get("term", "")
    values = _match_by_name(model, term)
    return JsonResponse(values, safe=False)