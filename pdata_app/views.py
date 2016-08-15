import os
from operator import attrgetter

from django.shortcuts import render
from django.db import connection
from django.db.models import Min, Max

from pdata_app.models import (DataFile, DataSubmission, ESGFDataset, CEDADataset,
    DataRequest, Variable)
from vocabs.vocabs import ONLINE_STATUS


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
    request_params = request.GET

    if not request_params:
        return render(request, 'variable_query.html', {'request': request,
            'page_title': 'Variable Query'})

    var_id = request_params.get('var_id')

    # Parameter specified
    if not var_id:
        msg = 'Please specify a Variable'
        return render(request, 'variable_query.html', {'request': request,
            'page_title': 'Variable Query', 'message': msg})

    # Everything supplied so start processing

    # TODO: add some defensive coding to handle no results returned, etc.

    # see if any files contain the variable requested
    files = DataFile.objects.filter(variable__var_id=var_id)
    if not files:
        return render(request, 'variable_query.html', {'request': request,
        'page_title': 'Variable Query',
        'message': 'Variable: {} not found'.format(var_id)})

    file_sets_found = []

    # get the variable_id primary key from the var_id name
    variable_id = Variable.objects.filter(var_id=var_id).first().id

    # loop through the unique combinations
    cursor = connection.cursor()
    uniq_rows = cursor.execute('SELECT DISTINCT frequency, climate_model_id, '
        'experiment_id, project_id, rip_code FROM pdata_app_datafile WHERE '
        'variable_id=%s', [variable_id])

    for row in uniq_rows.fetchall():
        # unpack the four items from each distinct set of files
        frequency, climate_model, experiment, project, rip_code = row
        # find all of the files that contain these distinct items
        row_files = DataFile.objects.filter(variable__var_id=var_id,
            frequency=frequency, climate_model_id=climate_model,
            experiment_id=experiment, project_id=project, rip_code=rip_code)
        # generate some summary info about the files
        files_online = row_files.filter(online=True).count()
        files_offline = row_files.filter(online=False).count()
        if files_offline:
            if files_online:
                online_status = ONLINE_STATUS.partial
            else:
                online_status = ONLINE_STATUS.offline
        else:
            online_status = ONLINE_STATUS.online
        num_files = row_files.count()
        # directories where the files currently are
        directories = ', '.join(sorted(set([df.directory for df in row_files])))
        # get first file in the set
        first_file = row_files.first()
        # find the earliest start and latest end times of the set
        start_time = row_files.aggregate(Min('start_time'))['start_time__min']
        end_time = row_files.aggregate(Max('end_time'))['end_time__max']
        # save the information in a dictionary
        file_sets_found.append({
            'project': first_file.project.short_name,
            'model': first_file.climate_model.short_name,
            'experiment': first_file.experiment.short_name,
            'frequency': first_file.frequency,
            'rip_code': rip_code,
            'num_files': num_files,
            'online_status': online_status,
            'directory': directories,
            'start_date': '{:04d}-{:02d}-{:02d}'.format(start_time.year,
                start_time.month, start_time.day),
            'end_date': '{:04d}-{:02d}-{:02d}'.format(end_time.year,
                end_time.month, end_time.day),
            'ceda_dl_url': _find_common_directory(row_files, 'ceda_download_url'),
            'ceda_od_url': _find_common_directory(row_files, 'ceda_opendap_url'),
            'esgf_dl_url': _find_common_directory(row_files, 'esgf_download_url'),
            'esgf_od_url': _find_common_directory(row_files, 'esgf_opendap_url')
        })
        # TODO version
        # TODO unit test

    return render(request, 'variable_query_results.html', {'request': request,
        'page_title': '{}: Variable Query Results'.format(var_id),
        'var_id': var_id, 'file_sets': file_sets_found})


def view_outstanding_query(request):
    # TODO: add autocomplete: http://flaviusim.com/blog/AJAX-Autocomplete-Search-with-Django-and-jQuery/
    request_params = request.GET

    # Basic page request with nothing specified
    if not request_params:
        return render(request, 'outstanding_query.html', {'request': request,
            'page_title': 'Outstanding Data Query'})

    project = request_params.get('project')
    model = request_params.get('model')
    experiment = request_params.get('experiment')

    # Some parameters specified
    if not (model and experiment and project):
        msg = 'Please specify a Project, Model and Experiment'
        return render(request, 'outstanding_query.html', {'request': request,
            'page_title': 'Outstanding Data Query', 'message': msg})

    # Everything supplied so start processing

    # Identify all of the data requests that match this query
    data_reqs = DataRequest.objects.filter(climate_model__short_name=model,
        experiment__short_name=experiment)

    if not data_reqs:
        msg = 'No data requests match that query'
        return render(request, 'outstanding_query.html', {'request': request,
            'page_title': 'Outstanding Data Query', 'message': msg})

    outstanding_reqs = []

    for req in data_reqs:
        req_files = DataFile.objects.filter(
            climate_model__id=req.climate_model_id,
            experiment__id=req.experiment_id,
            variable__id=req.variable_id,
            frequency=req.frequency)

        if not req_files:
            # no files found so request not satisfied
            outstanding_reqs.append(req)

    # TODO: sort the results by table and then variable
    outstanding_reqs.sort(key=attrgetter('variable.var_id'))

    return render(request, 'outstanding_query_results.html', {'request': request,
        'page_title': 'Outstanding Data Query', 'records': outstanding_reqs})


def _find_common_directory(query_set, attribute, separator='/'):
    """
    Takes a query set and an attribute present on objects in that query set. It
    finds the prefix that is common to that attribute on all of the objects. It
    returns the directory part of this common prefix (everything before the
    final separator).

    :param django.db.models.query.QuerySet query_set: The query set to search through.
    :param str attribute: The name of the attribute to find.
    :param str separator: The separator between directories.
    :returns: The prefix that is common to all attributes.
    :raises AttributeError: If attribute does not exist.
    """
    common_prefix = os.path.commonprefix(sorted(set(
        [getattr(qi, attribute) for qi in query_set])))
    common_dir, __ = common_prefix.rsplit(separator, 1)

    return common_dir
