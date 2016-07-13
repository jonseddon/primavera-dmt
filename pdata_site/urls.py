from django.conf.urls import patterns, include, url
from django.contrib.staticfiles.urls import staticfiles_urlpatterns

#from pdata_app import models

from rest_framework.urlpatterns import format_suffix_patterns
#import pdata_app.api

from django.contrib import admin
admin.autodiscover()

urlpatterns = patterns('',
    url(r'^admin/', include(admin.site.urls)),
    url(r'^files/$', 'pdata_app.views.view_data_files'),
    url(r'.*', 'pdata_app.views.view_home'),
    )

"""
                       url(r'^files/$', 'pdata_app.views.view_files'),
                       url(r'^chains/$', 'pdata_app.views.view_chains'),
                       url(r'^datasets/$', 'pdata_app.views.view_datasets'),
                       url(r'^events/$', 'pdata_app.views.view_events'),

                       url(r'^api/events/$', pdata_app.api.EventListView.as_view()),
                       url(r'^api/event/(?P<pk>[0-9]+)/$', pdata_app.api.EventDetailView.as_view()),
                       url(r'^html/events/$', pdata_app.api.EventListHTMLView.as_view()),

                       # Autocomplete options
    url(r'^api/autocomplete/(.+?)/$', 'pdata_app.views.view_autocomplete_list'),

                       url(r'.*', 'pdata_app.views.view_home'),
                       )
"""

urlpatterns += staticfiles_urlpatterns()