import os

import django_tables2 as tables

from .models import VariableOccurrence

class VariableOccurrenceTable(tables.Table):
    class Meta:
        model = VariableOccurrence
        attrs = {'class': 'paleblue'}
        exclude = ('id', 'max_val', 'min_val', 'shape')

    data_file__batch_id = tables.Column(empty_values=(), verbose_name='Batch ID')
    data_file__status = tables.Column(empty_values=(), verbose_name='File Status')
    data_file__original_location = tables.Column(empty_values=(), verbose_name='Directory')

    def render_data_file(self, value):
        return os.path.basename(value.original_location)

    def render_variable(self, value):
        return value.var_name

    def render_data_file__original_location(self, record):
        return os.path.dirname(record.data_file.original_location)

    def render_data_file__batch_id(self, record):
        return record.data_file.batch_id

    def render_data_file__status(self, record):
        return record.data_file.status

    def order_variable(self, queryset, is_descending):
        queryset = queryset.order_by(('-' if is_descending else '') + 'variable__var_name')
        return (queryset, True)

