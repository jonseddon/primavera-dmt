from urllib import urlencode

from django.utils.html import format_html
from django.urls import reverse
import django_tables2 as tables

from .models import (DataRequest, DataSubmission, DataFile, ESGFDataset,
                     CEDADataset, DataIssue, VariableRequest)

DEFAULT_VALUE = '--'


class DataFileTable(tables.Table):
    class Meta:
        model = DataFile
        attrs = {'class': 'paleblue'}
        exclude = ('id', 'incoming_directory', 'size', 'project', 'start_time',
                   'end_time', 'variable_request', 'frequency', 'rip_code',
                   'time_units', 'calendar', 'data_submission', 'esgf_dataset',
                   'ceda_dataset', 'ceda_download_url', 'ceda_opendap_url',
                   'esgf_download_url', 'esgf_opendap_url')

    cmor_name = tables.Column(empty_values=(), verbose_name='CMOR Name',
                              orderable=False)
    mip_table = tables.Column(empty_values=(), verbose_name='MIP Table',
                              orderable=False)
    checksum = tables.Column(empty_values=(), verbose_name='Checksum',
                             orderable=False)
    num_dataissues = tables.Column(empty_values=(),
                                   verbose_name='# Data Issues',
                                   orderable=False)

    def render_cmor_name(self, record):
        return record.variable_request.cmor_name

    def render_mip_table(self, record):
        return record.variable_request.table_name

    def render_checksum(self, record):
        checksum = record.checksum_set.first()
        if checksum:
            return '{}: {}'.format(checksum.checksum_type,
                                   checksum.checksum_value)
        else:
            return DEFAULT_VALUE

    def render_num_dataissues(self, record):
        num_dataissues = record.dataissue_set.count()
        url_query = urlencode({'data_file': record.id,
                               'data_file_string': '{} ({})'.format(
                                   record.name,
                                   record.directory)})
        return format_html('<a href="{}?{}">{}</a>'.format(
            reverse('data_issues'),
            url_query,
            num_dataissues
        ))


class DataSubmissionTable(tables.Table):
    class Meta:
        model = DataSubmission
        attrs = {'class': 'paleblue'}
        exclude = ('id', 'incoming_directory')
        sequence = ('directory', 'status', 'date_submitted', 'user',
                    'online_status', 'num_files', 'num_issues',
                    'earliest_date', 'latest_date', 'tape_urls',
                    'file_versions')
        order_by = '-date_submitted'

    online_status = tables.Column(empty_values=(), orderable=False)
    num_files = tables.Column(empty_values=(), verbose_name='# Data Files',
                              orderable=False)
    num_issues = tables.Column(empty_values=(), verbose_name='# Data Issues',
                               orderable=False)
    earliest_date = tables.Column(empty_values=(), orderable=False)
    latest_date = tables.Column(empty_values=(), orderable=False)
    tape_urls = tables.Column(empty_values=(), verbose_name='Tape URLs',
                              orderable=False)
    file_versions = tables.Column(empty_values=(), orderable=False)

    def render_date_submitted(self, value):
        return value.strftime('%Y-%m-%d %H:%M')

    def render_online_status(self, record):
        if record.status in ['PENDING_PROCESSING', 'ARRIVED']:
            return DEFAULT_VALUE
        else:
            return record.online_status()

    def render_num_files(self, record):
        if record.status in ['PENDING_PROCESSING', 'ARRIVED']:
            return DEFAULT_VALUE
        else:
            num_datafiles = len(record.get_data_files())
            url_query = urlencode({'data_submission': record.id,
                                   'data_submission_string': '{}'.format(
                                       record.directory)})
            return format_html('<a href="{}?{}">{}</a>'.format(
                reverse('data_files'),
                url_query,
                num_datafiles
            ))

    def render_num_issues(self, record):
        if record.status in ['PENDING_PROCESSING', 'ARRIVED']:
            return DEFAULT_VALUE
        else:
            num_dataissues = len(record.get_data_issues())
            url_query = urlencode({'data_submission': record.id,
                                   'data_submission_string': '{}'.format(
                                       record.directory)})
            return format_html('<a href="{}?{}">{}</a>'.format(
                reverse('data_issues'),
                url_query,
                num_dataissues
            ))

    def render_earliest_date(self, record):
        if record.status in ['PENDING_PROCESSING', 'ARRIVED']:
            return DEFAULT_VALUE
        else:
            start_time = record.start_time()
            if start_time:
                return start_time
            else:
                return DEFAULT_VALUE

    def render_latest_date(self, record):
        if record.status in ['PENDING_PROCESSING', 'ARRIVED']:
            return DEFAULT_VALUE
        else:
            end_time = record.end_time()
            if end_time:
                return end_time
            else:
                return DEFAULT_VALUE

    def render_tape_urls(self, record):
        if record.status in ['PENDING_PROCESSING', 'ARRIVED']:
            return DEFAULT_VALUE
        else:
            tape_urls = record.get_tape_urls()
            return _to_comma_sep(tape_urls)

    def render_file_versions(self, record):
        if record.status in ['PENDING_PROCESSING', 'ARRIVED']:
            return DEFAULT_VALUE
        else:
            file_versions = record.get_file_versions()
            return _to_comma_sep(file_versions)


class DataRequestTable(tables.Table):
    class Meta:
        model = DataRequest
        attrs = {'class': 'paleblue'}
        exclude = ('id', 'time_units', 'calendar', 'variable_request')
        sequence = ('project', 'institute', 'climate_model', 'experiment',
                    'rip_code', 'cmor_name', 'mip_table', 'start_time',
                    'end_time')

    cmor_name = tables.Column(empty_values=(), verbose_name='CMOR Name',
                              orderable=False)
    mip_table = tables.Column(empty_values=(), verbose_name='MIP Table',
                              orderable=False)

    def render_start_time(self, record):
        return record.start_date_string()

    def render_end_time(self, record):
        return record.end_date_string()

    def render_cmor_name(self, record):
        return record.variable_request.cmor_name

    def render_mip_table(self, record):
        return record.variable_request.table_name


class ESGFDatasetTable(tables.Table):
    class Meta:
        model = ESGFDataset
        attrs = {'class': 'paleblue'}
        exclude = ('id',)

    def render_ceda_dataset(self, value):
        url_query = urlencode({'directory':  value.directory})
        return format_html('<a href="{}?{}">{}</a>',
                           reverse('ceda_datasets'),
                           url_query, value.directory)

    def render_data_submission(self, value):
        url_query = urlencode({'directory':  value.directory})
        return format_html('<a href="{}?{}">{}</a>',
                           reverse('data_submissions'),
                           url_query, value.directory)


class CEDADatasetTable(tables.Table):
    class Meta:
        model = CEDADataset
        attrs = {'class': 'paleblue'}
        exclude = ('id',)

    def render_doi(self, value):
        if 'doi:' in value:
            doi_url = value.replace('doi:', 'https://doi.org/')
            url_html = '<a href="{url}">{url}</a>'.format(url=doi_url)
            return format_html(url_html)
        else:
            return value


class DataIssueTable(tables.Table):
    class Meta:
        model = DataIssue
        attrs = {'class': 'paleblue'}

    num_files_affected = tables.Column(empty_values=(), orderable=False,
                                       verbose_name='Total # Files Affected')

    def render_date_time(self, value):
        return value.strftime('%Y-%m-%d %H:%M')

    def render_num_files_affected(self, record):
        num_files_affected = record.data_file.count()
        url_query = urlencode({'data_issue': record.id,
                               'data_issue_string': '{} ({})\n{}{}'.format(
                                   record.reporter,
                                   record.date_time.strftime('%Y-%m-%d %H:%M'),
                                   record.issue[:100],
                                   '...' if len(record.issue) > 100 else ''
                               )})
        return format_html('<a href="{}?{}">{}</a>',
                           reverse('data_files'),
                           url_query, num_files_affected)


class VariableRequestQueryTable(tables.Table):
    class Meta:
        model = VariableRequest
        attrs = {'class': 'paleblue'}
        exclude = ('id',)
        sequence = ('cmor_name', 'table_name', 'long_name', 'units',
                    'frequency', 'standard_name', 'var_name', 'cell_methods',
                    'cell_measures', 'dimensions', 'positive', 'uid',
                    'variable_type', 'modeling_realm')
        order_by = 'var_name'


def _to_comma_sep(list_values):
    """
    Removes any None, False or blank items from a list and then converts the
    list to a string of comma separated values.
    """
    if list_values:
        actual_vals = [item for item in list_values if item]
        unique_vals = list(set(actual_vals))

        # remove duplicates and return
        if unique_vals:
            return ', '.join(unique_vals)
        else:
            return DEFAULT_VALUE
    else:
        return DEFAULT_VALUE
