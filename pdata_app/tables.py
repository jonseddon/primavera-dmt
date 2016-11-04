import django_tables2 as tables

from .models import DataRequest, DataSubmission, DataFile

DEFAULT_VALUE = '--'


class DataFileTable(tables.Table):
    class Meta:
        model = DataFile
        attrs = {'class': 'paleblue'}
        exclude = ('id', 'incoming_directory', 'size', 'project', 'institute',
                   'climate_model', 'experiment', 'start_time', 'end_time',
                   'variable_request', 'frequency', 'rip_code', 'time_units',
                   'calendar', 'data_submission', 'esgf_dataset',
                   'ceda_dataset', 'ceda_download_url', 'ceda_opendap_url',
                   'esgf_download_url', 'esgf_opendap_url')

    def render_data_submission(self, value):
        return '{}'.format(value.directory)


class DataSubmissionTable(tables.Table):
    class Meta:
        model = DataSubmission
        attrs = {'class': 'paleblue'}
        exclude = ('id', 'incoming_directory', 'user')
        sequence = ('directory', 'status', 'date_submitted', 'online_status',
                    'num_files', 'num_issues', 'earliest_date', 'latest_date',
                    'tape_urls', 'file_versions')

    online_status = tables.Column(empty_values=())
    num_files = tables.Column(empty_values=())
    num_issues = tables.Column(empty_values=())
    earliest_date = tables.Column(empty_values=())
    latest_date = tables.Column(empty_values=())
    tape_urls = tables.Column(empty_values=())
    file_versions = tables.Column(empty_values=())

    def render_online_status(self, record):
        if record.status in ['PENDING_PROCESSING', 'ARRIVED']:
            return DEFAULT_VALUE
        else:
            return record.online_status()

    def render_num_files(self, record):
        if record.status in ['PENDING_PROCESSING', 'ARRIVED']:
            return DEFAULT_VALUE
        else:
            return len(record.get_data_files())

    def render_num_issues(self, record):
        if record.status in ['PENDING_PROCESSING', 'ARRIVED']:
            return DEFAULT_VALUE
        else:
            return len(record.get_data_issues())

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
        exclude = ('id', 'time_units', 'calendar')
        sequence = ('project', 'institute', 'climate_model', 'experiment',
                    'rip_code', 'variable_request', 'start_time', 'end_time')

    def render_start_time(self, record):
        return record.start_date_string()

    def render_end_time(self, record):
        return record.end_date_string()

    def render_variable_request(self, value):
        return '{} ({})'.format(value.cmor_name, value.table_name)


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