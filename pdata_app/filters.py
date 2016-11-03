import django_filters
from .models import DataRequest

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
                                                 lookup_expr='contains')


    rip_code = django_filters.CharFilter(name='rip_code',
                                         lookup_expr='contains')
