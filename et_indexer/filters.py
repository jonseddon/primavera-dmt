import django_filters

from .models import VariableOccurrence, Datafile


class VariableOccurrenceFilter(django_filters.FilterSet):
    class Meta:
        model = VariableOccurrence
        fields = ['variable', 'data_file', 'batch_id', 'status', 'directory']

    variable = django_filters.CharFilter(name='variable__var_name',
                                         lookup_expr='iexact')

    data_file = django_filters.CharFilter(name='data_file__original_location',
                                          lookup_expr='icontains')

    directory = django_filters.CharFilter(name='data_file__original_location',
                                          lookup_expr='icontains')

    batch_id = django_filters.NumberFilter(name='data_file__batch_id')

    status = django_filters.CharFilter(name='data_file__status',
                                       lookup_expr='icontains')


class DatafileFilter(django_filters.FilterSet):
    class Meta:
        model = Datafile

    batch_id = django_filters.NumberFilter(name='batch_id')

    status = django_filters.CharFilter(name='status',
                                       lookup_expr='icontains')

    original_location = django_filters.CharFilter(name='original_location',
                                         lookup_expr='icontains')

    directory = django_filters.CharFilter(name='original_location',
                                         lookup_expr='icontains')
