from pdata_app.models import *

from rest_framework import serializers

class EventSerializer(serializers.ModelSerializer):

    class Meta:

        success_map = {True: "SUCCEEDED", False: "FAILED"}
        model = Event
        fields = ('id',
                  'dataset',
                  'process_stage',
                  'action_type',
                  'date_time',
                  'succeeded')
        depth = 2