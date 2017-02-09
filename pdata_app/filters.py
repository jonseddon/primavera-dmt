import django_filters
from .models import (DataRequest, DataSubmission, DataFile, ESGFDataset,
                     CEDADataset, DataIssue, VariableRequest, RetrievalRequest)


class DataRequestFilter(django_filters.FilterSet):
    class Meta:
        model = DataRequest
        fields = ['project', 'institute', 'climate_model', 'experiment',
                  'variable_request', 'rip_code']

    project = django_filters.CharFilter(name='project__short_name',
                                        lookup_expr='icontains')

    institute = django_filters.CharFilter(name='institute__short_name',
                                          lookup_expr='icontains')

    climate_model = django_filters.CharFilter(name='climate_model__short_name',
                                              lookup_expr='icontains')

    experiment = django_filters.CharFilter(name='experiment__short_name',
                                           lookup_expr='icontains')

    cmor_name = django_filters.CharFilter(name='variable_request__cmor_name',
                                          lookup_expr='iexact')

    mip_table = django_filters.CharFilter(name='variable_request__table_name',
                                          lookup_expr='iexact')

    rip_code = django_filters.CharFilter(name='rip_code',
                                         lookup_expr='icontains')


class DataFileFilter(django_filters.FilterSet):
    class Meta:
        model = DataFile
        fields = ['name', 'directory', 'version', 'tape_url', 'grid']

    name = django_filters.CharFilter(name='name',
                                     lookup_expr='icontains')

    directory = django_filters.CharFilter(name='directory',
                                          lookup_expr='icontains')

    version = django_filters.CharFilter(name='version',
                                        lookup_expr='icontains')

    tape_url = django_filters.CharFilter(name='tape_url',
                                         lookup_expr='icontains')

    data_submission = django_filters.NumberFilter(name='data_submission__id')

    data_issue = django_filters.NumberFilter(name='dataissue__id')

    data_request = django_filters.NumberFilter(name='data_request__id')

    grid = django_filters.CharFilter(name='grid', lookup_expr='icontains')

    cmor_name = django_filters.CharFilter(name='variable_request__cmor_name',
                                          lookup_expr='iexact')

    mip_table = django_filters.CharFilter(name='variable_request__table_name',
                                          lookup_expr='iexact')

    institute = django_filters.CharFilter(name='institute__short_name',
                                          lookup_expr='icontains')

    climate_model = django_filters.CharFilter(name='climate_model__short_name',
                                          lookup_expr='icontains')

    experiment = django_filters.CharFilter(name='experiment__short_name',
                                          lookup_expr='icontains')


class DataSubmissionFilter(django_filters.FilterSet):
    class Meta:
        model = DataSubmission
        fields = ('status', 'directory', 'user')

    status = django_filters.CharFilter(name='status',
                                     lookup_expr='icontains')

    directory = django_filters.CharFilter(name='directory',
                                          lookup_expr='icontains')

    version = django_filters.CharFilter(name='datafile__version',
                                        lookup_expr='icontains')

    tape_url = django_filters.CharFilter(name='datafile__tape_url',
                                        lookup_expr='icontains')

    user = django_filters.CharFilter(lookup_expr='icontains')


class ESGFDatasetFilter(django_filters.FilterSet):
    class Meta:
        models = ESGFDataset
        fields = ('drs_id', 'version', 'directory', 'thredds_urls',
                  'ceda_dataset', 'data_submission')

    drs_id = django_filters.CharFilter(name='drs_id', lookup_expr='icontains')

    version = django_filters.CharFilter(name='version', lookup_expr='icontains')

    directory = django_filters.CharFilter(name='directory',
                                          lookup_expr='icontains')

    thredds_url = django_filters.CharFilter(name='thredds_url',
                                            lookup_expr='icontains')

    ceda_dataset = django_filters.CharFilter(name='ceda_dataset__directory',
                                             lookup_expr='icontains')

    data_submission = django_filters.CharFilter(
        name='data_submission__directory',
        lookup_expr='icontains'
    )


class CEDADatasetFilter(django_filters.FilterSet):
    class Meta:
        models = CEDADataset
        fields = ('catalogue_url', 'directory', 'doi')

    catalogue_url = django_filters.CharFilter(name='catalogue_url',
                                              lookup_expr='icontains')

    directory = django_filters.CharFilter(name='directory',
                                          lookup_expr='icontains')

    doi = django_filters.CharFilter(name='doi', lookup_expr='icontains')


class DataIssueFilter(django_filters.FilterSet):
    class Meta:
        models = DataIssue
        fields = ('issue', 'reporter', 'date_time', 'id', 'data_file')

    id = django_filters.NumberFilter(name='id')

    issue = django_filters.CharFilter(name='issue', lookup_expr='icontains')

    reporter = django_filters.CharFilter(name='reporter',
                                         lookup_expr='icontains')

    date_time = django_filters.DateFromToRangeFilter(name='date_time')

    data_file = django_filters.NumberFilter(name='data_file__id')

    data_submission = django_filters.NumberFilter(
        name='data_file__data_submission__id')

    data_request = django_filters.NumberFilter(
        name='data_file__data_request__id')


class VariableRequestQueryFilter(django_filters.FilterSet):
    class Meta:
        models = VariableRequest
        fields = ('table_name', 'long_name', 'units', 'var_name',
                  'standard_name', 'cell_methods', 'positive', 'variable_type',
                  'dimensions', 'cmor_name', 'modeling_realm', 'frequency',
                  'cell_measures', 'uid')

    table_name = django_filters.CharFilter(name='table_name',
                                           lookup_expr='icontains')

    long_name = django_filters.CharFilter(name='long_name',
                                          lookup_expr='icontains')

    units = django_filters.CharFilter(name='units',
                                      lookup_expr='icontains')

    var_name = django_filters.CharFilter(name='var_name',
                                         lookup_expr='icontains')

    standard_name = django_filters.CharFilter(name='standard_name',
                                              lookup_expr='icontains')

    cell_methods = django_filters.CharFilter(name='cell_methods',
                                             lookup_expr='icontains')

    positive = django_filters.CharFilter(name='positive',
                                         lookup_expr='icontains')

    variable_type = django_filters.CharFilter(name='variable_type',
                                              lookup_expr='icontains')

    dimensions = django_filters.CharFilter(name='dimensions',
                                           lookup_expr='icontains')

    cmor_name = django_filters.CharFilter(name='cmor_name',
                                          lookup_expr='iexact')

    modeling_realm = django_filters.CharFilter(name='modeling_realm',
                                               lookup_expr='icontains')

    frequency = django_filters.CharFilter(name='frequency',
                                          lookup_expr='icontains')

    cell_measures = django_filters.CharFilter(name='cell_measures',
                                              lookup_expr='icontains')

    uid = django_filters.CharFilter(name='uid',
                                    lookup_expr='icontains')

    nameless = django_filters.MethodFilter()

    def filter_nameless(self, queryset, value):
        if value:
            return queryset.filter(cmor_name__exact='')
        return queryset


class RetrievalRequestFilter(django_filters.FilterSet):
    class Meta:
        models = RetrievalRequest
        fields = ('id', 'requester', 'date_created')

    id = django_filters.NumberFilter(name='id')

    requester = django_filters.CharFilter(name='requester',
                                          lookup_expr='icontains')

    date_time = django_filters.DateFromToRangeFilter(name='date_created')
