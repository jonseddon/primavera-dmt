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


def view_home(request):
    return render_to_response('home.html', {'request': request})
