import django_filters
from .models import (DataRequest, DataSubmission, DataFile, ESGFDataset,
                     CEDADataset)


class DataRequestFilter(django_filters.FilterSet):
    class Meta:
        model = DataRequest
        fields = ['project', 'institute', 'climate_model', 'experiment',
                  'variable_request', 'rip_code']

    project = django_filters.CharFilter(name='project__short_name',
                                        lookup_expr='contains')

    institute = django_filters.CharFilter(name='institute__short_name',
                                          lookup_expr='contains')

    climate_model = django_filters.CharFilter(name='climate_model__short_name',
                                              lookup_expr='contains')

    experiment = django_filters.CharFilter(name='experiment__short_name',
                                           lookup_expr='contains')

    variable_request = django_filters.CharFilter(name='variable_request__cmor_name',
                                                 lookup_expr='exact')

    rip_code = django_filters.CharFilter(name='rip_code',
                                         lookup_expr='contains')


class DataFileFilter(django_filters.FilterSet):
    class Meta:
        model = DataFile
        fields = ['name', 'directory', 'version', 'tape_url']

    name = django_filters.CharFilter(name='name',
                                         lookup_expr='contains')

    directory = django_filters.CharFilter(name='directory',
                                         lookup_expr='contains')

    version = django_filters.CharFilter(name='version',
                                         lookup_expr='contains')

    tape_url = django_filters.CharFilter(name='tape_url',
                                         lookup_expr='contains')


class DataSubmissionFilter(django_filters.FilterSet):
    class Meta:
        model = DataSubmission
        fields = ('status', 'directory')

    status = django_filters.CharFilter(name='status',
                                     lookup_expr='icontains')

    directory = django_filters.CharFilter(name='directory',
                                          lookup_expr='contains')

    version = django_filters.CharFilter(name='datafile__version',
                                        lookup_expr='contains')

    tape_url = django_filters.CharFilter(name='datafile__tape_url',
                                        lookup_expr='contains')


class ESGFDatasetFilter(django_filters.FilterSet):
    class Meta:
        models = ESGFDataset
        fields = ('drs_id', 'version', 'directory', 'thredds_urls',
                  'ceda_dataset', 'data_submission')

    drs_id = django_filters.CharFilter(name='drs_id', lookup_expr='contains')

    version = django_filters.CharFilter(name='version', lookup_expr='contains')

    directory = django_filters.CharFilter(name='directory',
                                          lookup_expr='contains')

    thredds_url = django_filters.CharFilter(name='thredds_url',
                                            lookup_expr='contains')

    ceda_dataset = django_filters.CharFilter(name='ceda_dataset__directory',
                                             lookup_expr='contains')

    data_submission = django_filters.CharFilter(
        name='data_submission__directory',
        lookup_expr='contains'
    )


class CEDADatasetFilter(django_filters.FilterSet):
    class Meta:
        models = CEDADataset
        fields = ('catalogue_url', 'directory', 'doi')

    catalogue_url = django_filters.CharFilter(name='catalogue_url',
                                              lookup_expr='contains')

    directory = django_filters.CharFilter(name='directory',
                                          lookup_expr='contains')

    doi = django_filters.CharFilter(name='doi', lookup_expr='contains')
