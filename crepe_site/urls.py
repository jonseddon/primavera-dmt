from django.conf.urls import patterns, include, url
from django.contrib.staticfiles.urls import staticfiles_urlpatterns

#from crepe_app import models

from rest_framework.urlpatterns import format_suffix_patterns
import crepe_app.api

from django.contrib import admin
admin.autodiscover()

urlpatterns = patterns('',
                       # Examples:
    # url(r'^$', 'crepe_site.views.home', name='home'),
    # url(r'^blog/', include('blog.urls')),
    url(r'^admin/', include(admin.site.urls)),
    url(r'^files/$', 'crepe_app.views.view_files'),
    url(r'^chains/$', 'crepe_app.views.view_chains'),
    url(r'^datasets/$', 'crepe_app.views.view_datasets'),
    url(r'^events/$', 'crepe_app.views.view_events'),

    url(r'^api/events/$', crepe_app.api.EventListView.as_view()),
    url(r'^api/event/(?P<pk>[0-9]+)/$', crepe_app.api.EventDetailView.as_view()),
    url(r'^html/events/$', crepe_app.api.EventListHTMLView.as_view()),

    # Autocomplete options
    url(r'^api/autocomplete/(.+?)/$', 'crepe_app.views.view_autocomplete_list'),

    url(r'.*', 'crepe_app.views.view_home'),
   )

urlpatterns += staticfiles_urlpatterns()