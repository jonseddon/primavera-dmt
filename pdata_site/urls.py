from django.conf.urls import patterns, include, url
from django.contrib.staticfiles.urls import staticfiles_urlpatterns

from rest_framework.urlpatterns import format_suffix_patterns
import pdata_app.views


from django.contrib import admin
admin.autodiscover()

urlpatterns = [
    url(r'^admin/', include(admin.site.urls)),
    url(r'^files/$', pdata_app.views.view_data_files),
    url(r'^submissions/$', pdata_app.views.view_data_submissions),
    url(r'^esgf_datasets/$', pdata_app.views.view_esgf_datasets),
    url(r'^ceda_datasets/$', pdata_app.views.view_ceda_datasets),
    url(r'.*', pdata_app.views.view_home),
]

urlpatterns += staticfiles_urlpatterns()