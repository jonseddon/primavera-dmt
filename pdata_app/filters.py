import django_filters
from .models import DataRequest

class DataRequestFilter(django_filters.FilterSet):
    class Meta:
        model = DataRequest
        fields = ['project', 'experiment']

    project = django_filters.CharFilter(name='project__short_name',
                                        lookup_expr='contains')

    experiment = django_filters.CharFilter(name='experiment__short_name',
                                           lookup_expr='contains')