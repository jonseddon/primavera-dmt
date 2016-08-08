from django.shortcuts import render, HttpResponseRedirect
from django.core.urlresolvers import reverse
from django.shortcuts import get_list_or_404

from pdata_app.models import (DataFile, DataSubmission, ESGFDataset, CEDADataset,
    DataRequest)


def view_data_submissions(request):
    data_submissions = DataSubmission.objects.all()
    return render(request, 'data_submissions.html', {'request': request,
        'page_title': 'Data Submissions', 'records': data_submissions})


def view_data_files(request):
    data_files = DataFile.objects.all()
    return render(request, 'data_files.html', {'request': request,
        'page_title': 'Data Files', 'records': data_files})


def view_ceda_datasets(request):
    ceda_datasets = CEDADataset.objects.all()
    return render(request, 'ceda_datasets.html', {'request': request,
        'page_title': 'CEDA Datasets', 'records': ceda_datasets})


def view_esgf_datasets(request):
    esgf_datasets = ESGFDataset.objects.all()
    return render(request, 'esgf_datasets.html', {'request': request,
        'page_title': 'ESGF Datasets', 'records': esgf_datasets})


def view_data_requests(request):
    data_reqs = DataRequest.objects.all()
    return render(request, 'data_requests.html', {'request': request,
        'page_title': 'Data Requests', 'records': data_reqs})


def view_home(request):
    return render(request, 'home.html', {'request': request,
        'page_title': 'The PRIMAVERA DMT'})


def view_variable_query(request):
    return render(request, 'variable_query.html', {'request': request,
        'page_title': 'Variable Query'})


def view_variable_query_form(request):
    var_id = request.POST['var_id']
    return HttpResponseRedirect(reverse('variable_query_results', args=[var_id]))


def view_variable_query_results(request, var_id):
    files = DataFile.objects.filter(variable__var_id=var_id)
    if not files:
        return render(request, 'variable_query.html', {'request': request,
        'page_title': 'Variable Query',
        'message': 'Variable: {} not found'.format(var_id)})
    else:
        return render(request, 'variable_query_results.html', {'request': request,
            'page_title': 'Variable Query Results', 'var_id': var_id,
            'files': files})
