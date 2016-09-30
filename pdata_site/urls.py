from django.conf.urls import include, url
from django.contrib.staticfiles.urls import staticfiles_urlpatterns

import pdata_app.views


from django.contrib import admin
admin.autodiscover()

urlpatterns = [
    url(r'^admin/', include(admin.site.urls)),

    url(r'^login/$', pdata_app.views.view_login, name='login'),

    url(r'^logout/$', pdata_app.views.view_logout, name='logout'),

    url(r'^files/$', pdata_app.views.view_data_files, name='data_files'),

    url(r'^submissions/$', pdata_app.views.view_data_submissions,
        name='data_submissions'),

    url(r'^esgf_datasets/$', pdata_app.views.view_esgf_datasets,
        name='esgf_datasets'),

    url(r'^ceda_datasets/$', pdata_app.views.view_ceda_datasets,
        name='ceda_datasets'),

    url(r'^data_requests/$', pdata_app.views.view_data_requests,
        name='data_requests'),

    url(r'^data_issues/$', pdata_app.views.view_data_issues,
        name='data_issues'),

    url(r'^variable_query/$', pdata_app.views.view_variable_query,
        name='variable_query'),

    url(r'^outstanding_query/$', pdata_app.views.view_outstanding_query,
        name='outstanding_query'),

    url(r'^create_submission/$', pdata_app.views.create_submission,
        name='create_submission'),

    url(r'.*', pdata_app.views.view_home, name='home'),
]

urlpatterns += staticfiles_urlpatterns()