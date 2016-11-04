import django_tables2 as tables

from .models import DataRequest, DataFile


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
