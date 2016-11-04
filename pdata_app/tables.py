import django_tables2 as tables

from .models import DataRequest


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
