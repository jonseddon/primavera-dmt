from pdata_app.models import *
from pdata_app.utils.common import as_bool
from pdata_app.serializers import EventSerializer

from django.http import Http404

from rest_framework import views
from rest_framework.response import Response
from rest_framework.renderers import TemplateHTMLRenderer
from rest_framework import filters
from rest_framework import generics


class EventListHTMLView(views.APIView):
    renderer_classes = [TemplateHTMLRenderer]
    template_name = 'event_list.html'

    def get(self, request):
        queryset = Event.objects.all()
        return Response({'events': queryset})


class EventListView(generics.ListAPIView):
    serializer_class = EventSerializer
#    filter_backends = (filters.OrderingFilter,)

    def _get(self, request, format=None):
        events = Event.objects.all()
        serialized_events = EventSerializer(events, many=True)
        return Response(serialized_events.data)

    def get_queryset(self):
        """
        This view should return a list of all the Events
        that match the user query.
        """
        order_param = self.request.query_params.get('order_by', 'date_time')
        # reverse is set by "-" prefix to order param
        page_number = int(self.request.query_params.get('page_number', 1))

        # Get records per page (limit to 100)
        recs_per_page = int(self.request.query_params.get('recs_per_page', 20))
        record_limit = 100
        if recs_per_page > record_limit: recs_per_page = record_limit

        # Apply filters if necessary
        dataset_match = self.request.query_params.get('dataset')
        if dataset_match:
            resp = Event.objects.filter(dataset__name__icontains=dataset_match)
        else:
            resp = Event.objects.all()

        process_stage_match = self.request.query_params.get('process_stage')
        if process_stage_match:
            resp = resp.filter(processstage__name__icontains=process_stage_match)

        is_withdrawn_match = self.request.query_params.get('withdrawn')
        if is_withdrawn_match:
            resp = resp.filter(dataset__is_withdrawn=as_bool(is_withdrawn_match, nofail=True))

        succeeded_match = self.request.query_params.get('succeeded')
        if succeeded_match:
            resp = resp.filter(succeeded=as_bool(succeeded_match, nofail=True))

        action_type_match = self.request.query_params.get('action_type')
        if action_type_match:
            resp = resp.filter(action_type=action_type_match)

        resp = resp.order_by(order_param)

        # Filter slice of records as specified
        first = recs_per_page * (page_number - 1)
        last = first + recs_per_page
        return resp[first:last]


class EventDetailView(views.APIView):

    def get_object(self, pk):
        try:
            return Event.objects.get(pk=pk)
        except Event.DoesNotExist:
            raise Http404

    def get(self, request, pk, format=None):
        event = self.get_object(pk)
        serialized_event = EventSerializer(event)
        return Response(serialized_event.data)


