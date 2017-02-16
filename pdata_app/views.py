import os
import re
import urllib

from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.decorators import login_required
from django.db.models import Sum
from django.shortcuts import render, redirect
from django.http import HttpResponseRedirect
from django.core.urlresolvers import reverse

from .models import (DataFile, DataSubmission, ESGFDataset, CEDADataset,
                     DataRequest, DataIssue, VariableRequest, RetrievalRequest)
from .forms import CreateSubmissionForm, CreateRetrievalForm
from .tables import (DataRequestTable, DataFileTable, DataSubmissionTable,
                     ESGFDatasetTable, CEDADatasetTable, DataIssueTable,
                     VariableRequestQueryTable, DataReceivedTable,
                     RetrievalRequestTable)
from .filters import (DataRequestFilter, DataFileFilter, DataSubmissionFilter,
                      ESGFDatasetFilter, CEDADatasetFilter, DataIssueFilter,
                      VariableRequestQueryFilter, RetrievalRequestFilter)
from .utils.table_views import PagedFilteredTableView, DataRequestsFilteredView
from vocabs.vocabs import STATUS_VALUES


class DataRequestList(DataRequestsFilteredView):
    model = DataRequest
    table_class = DataRequestTable
    filter_class = DataRequestFilter
    page_title = 'Data Requests'
    message = 'These are the data requests made...'


class OutstandingDataRequestList(DataRequestsFilteredView):
    model = DataRequest
    table_class = DataRequestTable
    filter_class = DataRequestFilter
    page_title = 'Outstanding Data Requests'
    get_outstanding_data = True
    message = ('No files have been received for the data requests below. '
               'Please note that this is only a list of the data requests '
               'that no data at all has been received for. The absence of a '
               'data request from this list does not mean that all years '
               'have been received yet.')


class ReceivedDataRequestList(DataRequestsFilteredView):
    model = DataRequest
    table_class = DataReceivedTable
    filter_class = DataRequestFilter
    page_title = 'Variables Received'
    get_received_data = True
    message = 'The following data has been received:'


class VariableRequestList(PagedFilteredTableView):
    model = VariableRequest
    table_class = VariableRequestQueryTable
    filter_class = VariableRequestQueryFilter
    page_title = 'Variable Request'


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


class RetrievalRequestList(PagedFilteredTableView):
    model = RetrievalRequest
    table_class = RetrievalRequestTable
    filter_class = RetrievalRequestFilter
    page_title = 'Retrieval Requests'


def view_login(request):
    if request.method == 'GET':
        next_page = request.GET.get('next')
        if next_page:
            return render(request, 'pdata_app/login.html', {'request': request,
                'page_title': 'Login', 'next': next_page})
        else:
            return render(request, 'pdata_app/login.html', {'request': request,
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
            return render(request, 'pdata_app/login.html', {'request': request,
                'page_title': 'Login', 'next': next_page, 'errors': True})
        else:
            return render(request, 'pdata_app/login.html', {'request': request,
                'page_title': 'Login', 'errors': True})


def view_logout(request):
    logout(request)
    return redirect('home')


def view_home(request):
    return render(request, 'pdata_app/home.html', {'request': request,
        'page_title': 'The PRIMAVERA DMT'})


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
            return _custom_redirect('data_submissions')
    else:
        form = CreateSubmissionForm()
    return render(request, 'pdata_app/create_submission_form.html',
                  {'form': form,
                   'page_title': 'Create Data Submission'})


@login_required(login_url='/login/')
def confirm_retrieval(request):
    if request.method == 'POST':
        data_req_ids = []
        for key in request.POST:
            components = re.match(r'^request_data_req_(\d+)$', key)
            if components:
                data_req_ids.append(int(components.group(1)))
        data_req_strs = [str(DataRequest.objects.filter(id=req).first())
                         for req in data_req_ids]
        request_size = sum([DataRequest.objects.filter(id=req).first().
                            datafile_set.aggregate(Sum('size'))['size__sum']
                            for req in data_req_ids])
        # TODO include link to return to query page
        return render(request, 'pdata_app/confirm_retrieval_request.html',
                      {'request': request, 'data_reqs':data_req_strs,
                       'request_size': request_size,
                       'page_title': 'Confirm Retrieval Request'})
    else:
        # TODO do something intelligent
        pass


@login_required(login_url='/login/')
def create_retrieval(request):
    if request.method == 'POST':
        form = CreateRetrievalForm(request.POST)
        if form.is_valid():
            retrieval = form.save(commit=False)
            retrieval.user = request.user
            retrieval.save()
            # add the data requests to the retrieval request now
            return _custom_redirect('retrieval_requests')
        else:
            return render(request, 'pdata_app/retrieval_request_error.html',
                      {'request': request,
                       'page_title': 'Retrieval Request'})

    else:
        return render(request, 'pdata_app/retrieval_request_error.html',
                      {'request': request,
                       'page_title': 'Retrieval Request'})


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
