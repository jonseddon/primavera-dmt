from django.shortcuts import render

# Create your views here.
from django.shortcuts import render_to_response

from pdata_app.models import DataFile, DataSubmission, ESGFDataset, CEDADataset


def view_data_submissions(request):
    data_submissions = DataSubmission.objects.all()
    return render(request, 'data_submissions.html', {'request': request,  'page_title': 'Data Submissions',
                                                'records': data_submissions})


def view_data_files(request):
    data_files = DataFile.objects.all()
    return render(request, 'data_files.html', {'request': request,  'page_title': 'Data Files',
                                                'records': data_files})


def view_ceda_datasets(request):
    ceda_datasets = CEDADataset.objects.all()
    return render(request, 'ceda_datasets.html', {'request': request,  'page_title': 'CEDA Datasets',
                                                'records': ceda_datasets})


def view_esgf_datasets(request):
    esgf_datasets = ESGFDataset.objects.all()
    return render(request, 'esgf_datasets.html', {'request': request,  'page_title': 'ESGF Datasets',
                                                'records': esgf_datasets})


def view_home(request):
    return render_to_response('home.html', {'request': request, 'page_title': 'The PRIMAVERA DMT'})
"""

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
"""