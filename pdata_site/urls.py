from django.conf.urls import include, url
from django.contrib.staticfiles.urls import staticfiles_urlpatterns

from rest_framework.urlpatterns import format_suffix_patterns
import pdata_app.views


from django.contrib import admin
admin.autodiscover()

urlpatterns = [
    url(r'^admin/', include(admin.site.urls)),
    url(r'^files/$', pdata_app.views.view_data_files, name='data_files'),
    url(r'^submissions/$', pdata_app.views.view_data_submissions, name='data_submissions'),
    url(r'^esgf_datasets/$', pdata_app.views.view_esgf_datasets, name='esgf_datasets'),
    url(r'^ceda_datasets/$', pdata_app.views.view_ceda_datasets, name='ceda_datasets'),
    url(r'^data_requests/$', pdata_app.views.view_data_requests, name='data_requests'),
    url(r'^variable_query/$', pdata_app.views.view_variable_query, name='variable_query'),
    url(r'^variable_query/(?P<var_id>[\w]+)$', pdata_app.views.view_variable_query_results,
        name='variable_query_results'),
    url(r'^variable_query_form$', pdata_app.views.view_variable_query_form,
        name='variable_query_form'),
    url(r'.*', pdata_app.views.view_home),
]

urlpatterns += staticfiles_urlpatterns()