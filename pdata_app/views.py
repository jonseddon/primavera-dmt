from django.shortcuts import render, HttpResponseRedirect
from django.core.urlresolvers import reverse
from django.db import connection
from django.db.models import Min, Max

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
    # TODO: add some defensive coding to handle no results returned, etc.
    files = DataFile.objects.filter(variable__var_id=var_id)

    if not files:
        return render(request, 'variable_query.html', {'request': request,
        'page_title': 'Variable Query',
        'message': 'Variable: {} not found'.format(var_id)})

    vbles_found = []

    # loop through the unique combinations
    cursor = connection.cursor()
    variable_id_query = cursor.execute("SELECT id FROM pdata_app_variable WHERE "
        "var_id=%s", [var_id])
    variable_id, = variable_id_query.fetchone()
    uniq_rows = cursor.execute('SELECT DISTINCT frequency, climate_model_id, '
        'experiment_id, project_id FROM pdata_app_datafile WHERE '
        'variable_id=%s', [variable_id])

    for row in uniq_rows.fetchall():
        # unpack the four items from each distinct set of files
        frequency, climate_model, experiment, project = row
        # find all of the files that contain these distinct items
        row_files = DataFile.objects.filter(variable__var_id=var_id,
            frequency=frequency, climate_model_id=climate_model,
            experiment_id=experiment, project_id=project)
        # get first file in the set
        first_file = row_files.first()
        # find the earliest start and latest end times of the set
        start_time = row_files.aggregate(Min('start_time'))['start_time__min']
        end_time = row_files.aggregate(Max('end_time'))['end_time__max']
        # save the information in a dictionary
        vbles_found.append({
            'project': first_file.project.short_name,
            'model': first_file.climate_model.short_name,
            'experiment': first_file.experiment.short_name,
            'frequency': first_file.frequency,
            'start_date': '{:04d}-{:02d}-{:02d}'.format(start_time.year,
                start_time.month, start_time.day),
            'end_date': '{:04d}-{:02d}-{:02d}'.format(end_time.year,
                end_time.month, end_time.day)
        })
        # TODO num files, online, ensemble

    return render(request, 'variable_query_results.html', {'request': request,
        'page_title': 'Variable Query Results', 'var_id': var_id,
        'variables': vbles_found})
