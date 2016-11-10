import os
import urllib

import cf_units

from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.decorators import login_required
from django.shortcuts import render, redirect
from django.db import connection
from django.http import HttpResponseRedirect
from django.core.urlresolvers import reverse

from .models import (DataFile, DataSubmission, ESGFDataset, CEDADataset,
                     DataRequest, DataIssue, VariableRequest, Settings,
                     standardise_time_unit)
from .forms import CreateSubmissionForm
from .tables import (DataRequestTable, DataFileTable, DataSubmissionTable,
                     ESGFDatasetTable, CEDADatasetTable, DataIssueTable)
from .filters import (DataRequestFilter, DataFileFilter, DataSubmissionFilter,
                      ESGFDatasetFilter, CEDADatasetFilter, DataIssueFilter)
from .utils.table_views import PagedFilteredTableView
from vocabs.vocabs import ONLINE_STATUS, STATUS_VALUES


class DataRequestList(PagedFilteredTableView):
    model = DataRequest
    table_class = DataRequestTable
    filter_class = DataRequestFilter
    page_title = 'Data Requests'


class DataFileList(PagedFilteredTableView):
    model = DataFile
    table_class = DataFileTable
    filter_class = DataFileFilter
    page_title = 'Data Files'


class DataSubmissionList(PagedFilteredTableView):
    model = DataSubmission
    table_class = DataSubmissionTable
    filter_class = DataSubmissionFilter
    page_title = 'Data Submissions'


class ESGFDatasetList(PagedFilteredTableView):
    model = ESGFDataset
    table_class = ESGFDatasetTable
    filter_class = ESGFDatasetFilter
    page_title = 'ESGF Datasets'


class CEDADatasetList(PagedFilteredTableView):
    model = CEDADataset
    table_class = CEDADatasetTable
    filter_class = CEDADatasetFilter
    page_title = 'CEDA Datasets'


class DataIssueList(PagedFilteredTableView):
    model = DataIssue
    table_class = DataIssueTable
    filter_class = DataIssueFilter
    page_title = 'Data Issues'


def view_login(request):
    if request.method == 'GET':
        next_page = request.GET.get('next')
        if next_page:
            return render(request, 'login.html', {'request': request,
                'page_title': 'Login', 'next': next_page})
        else:
            return render(request, 'login.html', {'request': request,
                'page_title': 'Login'})

    username = request.POST['username']
    password = request.POST['password']
    next_page = request.POST['next']
    user = authenticate(username=username, password=password)
    if user is not None and user.is_active:
        login(request, user)
        if next_page:
            return redirect(next_page)
        else:
            return redirect('home')
    else:
        if next_page:
            return render(request, 'login.html', {'request': request,
                'page_title': 'Login', 'next': next_page, 'errors': True})
        else:
            return render(request, 'login.html', {'request': request,
                'page_title': 'Login', 'errors': True})


def view_logout(request):
    logout(request)
    return redirect('home')


def view_home(request):
    return render(request, 'home.html', {'request': request,
        'page_title': 'The PRIMAVERA DMT'})


def view_retrieval_request(request):
    return render(request, 'retrieval_request.html', {'request': request,
        'page_title': 'Retrieval Request'})


def view_variable_query(request):
    """
    Similarly to view_outstanding_query(), the results from this view should not
    be displayed with django-tables and instead the jQuery data-tables package
    should be used for displaying the results.
    """
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

    # see if any files contain the variable requested
    files = DataFile.objects.filter(variable_request__cmor_name=var_id)
    if not files:
        return render(request, 'variable_query.html', {'request': request,
        'page_title': 'Variable Query',
        'message': 'Variable: {} not found'.format(var_id)})

    file_sets_found = []

    # get the variable_id primary key from the var_id name
    variable_id = VariableRequest.objects.filter(cmor_name=var_id).first().id

    # loop through the unique combinations
    cursor = connection.cursor()
    uniq_rows = cursor.execute('SELECT DISTINCT frequency, climate_model_id, '
        'experiment_id, project_id, rip_code FROM pdata_app_datafile WHERE '
        'variable_request_id=%s', [variable_id])

    for row in uniq_rows.fetchall():
        # unpack the four items from each distinct set of files
        frequency, climate_model, experiment, project, rip_code = row

        # find all of the files that contain these distinct items
        row_files = DataFile.objects.filter(variable_request__cmor_name=var_id,
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

        # the tape urls of these files
        unique_tape_urls = sorted(set([df.tape_url for df in row_files
                                       if df.tape_url]))
        if unique_tape_urls:
            tape_urls = ', '.join(unique_tape_urls)
        else:
            tape_urls = '--'

        # the versions of these files
        unique_versions = sorted(set([df.version for df in row_files
                                      if df.version]))
        if unique_versions:
            versions = ', '.join(unique_versions)
        else:
            versions = '--'

        # get first file in the set
        first_file = row_files.first()

        # find the earliest start time of the set
        std_units = Settings.get_solo().standard_time_units

        start_times = row_files.values_list('start_time', 'time_units',
            'calendar')
        std_start_times = [
            (standardise_time_unit(time, unit, std_units, cal), cal)
            for time, unit, cal in start_times
        ]
        start_nones_removed = [(std_time, cal)
                               for std_time, cal in std_start_times
                               if std_time is not None]
        if start_nones_removed:
            start_float, calendar = min(start_nones_removed, key=lambda x: x[0])
            start_obj = cf_units.num2date(start_float, std_units, calendar)
            start_string = '{:04d}-{:02d}-{:02d}'.format(start_obj.year,
                start_obj.month, start_obj.day)
        else:
            start_string = '--'

        # find the latest end time of the set
        end_times = row_files.values_list('end_time', 'time_units',
            'calendar')
        std_end_times = [
            (standardise_time_unit(time, unit, std_units, cal), cal)
            for time, unit, cal in end_times
        ]
        end_nones_removed = [(std_time, cal)
                             for std_time, cal in std_end_times
                             if std_time is not None]
        if end_nones_removed:
            end_float, calendar = max(end_nones_removed, key=lambda x: x[0])
            end_obj = cf_units.num2date(end_float, std_units, calendar)
            end_string = '{:04d}-{:02d}-{:02d}'.format(end_obj.year,
                end_obj.month, end_obj.day)
        else:
            end_string = '--'

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
            'start_date': start_string,
            'end_date': end_string,
            'tape_urls': tape_urls,
            'versions': versions,
            'ceda_dl_url': _find_common_directory(row_files, 'ceda_download_url'),
            'ceda_od_url': _find_common_directory(row_files, 'ceda_opendap_url'),
            'esgf_dl_url': _find_common_directory(row_files, 'esgf_download_url'),
            'esgf_od_url': _find_common_directory(row_files, 'esgf_opendap_url')
        })

    return render(request, 'variable_query_results.html', {'request': request,
        'page_title': '{}: Variable Query Results'.format(var_id),
        'var_id': var_id, 'file_sets': file_sets_found})


def view_outstanding_query(request):
    """
    Note: this view should not be displayed using django-tables unlike many of
    the other views. The other views are implemented in a single SQL query.
    django-tables will limit the query to only the subset of records that will
    be displayed on the next page. This view_outstanding_query uses user code
    to filter the results. Calling it repeatedly will be slow and django-tables
    subsetting could result in unpredictable output. Instead, all of the results
    from this query are sent to the output web page. There, the datatables
    plugin for jQuery (https://datatables.net/) is used to filter and paginate
    the query results on the page.
    """
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

    excludes = []

    for req in data_reqs:
        if req.rip_code:
            req_files = DataFile.objects.filter(
                climate_model__id=req.climate_model_id,
                experiment__id=req.experiment_id,
                variable_request__id=req.variable_request_id,
                rip_code=req.rip_code)
        else:
            req_files = DataFile.objects.filter(
                climate_model__id=req.climate_model_id,
                experiment__id=req.experiment_id,
                variable_request__id=req.variable_request_id)

        if req_files:
            # files found so request is satisfied so remove this request
            excludes.append(req.id)

    outstanding_reqs = data_reqs.exclude(id__in=excludes)

    return render(request, 'outstanding_query_results.html', {'request': request,
        'page_title': 'Outstanding Data Query', 'records': outstanding_reqs})


@login_required(login_url='/login/')
def create_submission(request):
    if request.method == 'POST':
        form = CreateSubmissionForm(request.POST)
        if form.is_valid():
            submission = form.save(commit=False)
            submission.incoming_directory = os.path.normpath(
                submission.incoming_directory)
            submission.directory = submission.incoming_directory
            submission.status = STATUS_VALUES['PENDING_PROCESSING']
            submission.user = request.user
            submission.save()
            return _custom_redirect('data_submissions', sort='-date_submitted')
    else:
        form = CreateSubmissionForm()
    return render(request, 'create_submission_form.html', {'form': form,
        'page_title': 'Create Data Submission'})


def _custom_redirect(url_name, *args, **kwargs):
    """
    Generate an HTTP redirect to URL with GET parameters.

    For example: _custom_redirect('data_submissions', sort='-date_submitted')
    should redirect to data_submissions/?sort=-date_submitted

    From: http://stackoverflow.com/a/3766452

    :param str url_name:
    :param str args:
    :param kwargs:
    :return: An HTTP response
    """
    url = reverse(url_name, args=args)
    params = urllib.urlencode(kwargs)
    return HttpResponseRedirect(url + "?%s" % params)


def _find_common_directory(query_set, attribute, separator='/'):
    """
    Takes a query set and an attribute present on objects in that query set. It
    finds the prefix that is common to that attribute on all of the objects. It
    returns the directory part of this common prefix (everything before the
    final separator).

    :param django.db.models.query.QuerySet query_set: The query set to search
        through.
    :param str attribute: The name of the attribute to find.
    :param str separator: The separator between directories.
    :returns: The prefix that is common to all attributes or None if attribute
        has not been set on any items.
    :raises AttributeError: If attribute does not exist.
    """
    uniq_query_items = sorted(set([getattr(qi, attribute)
                                   for qi in query_set
                                   if getattr(qi, attribute)]))
    if uniq_query_items and uniq_query_items != [None]:
        common_prefix = os.path.commonprefix(uniq_query_items)
        common_dir, __ = common_prefix.rsplit(separator, 1)
        return common_dir
    else:
        return None
