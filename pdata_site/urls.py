from django.conf.urls import include, url
from django.contrib.staticfiles.urls import staticfiles_urlpatterns

import pdata_app.views
import et_indexer.views


from django.contrib import admin
admin.autodiscover()

urlpatterns = [
    url(r'^admin/', include(admin.site.urls)),

    url(r'^login/$', pdata_app.views.view_login, name='login'),

    url(r'^logout/$', pdata_app.views.view_logout, name='logout'),

    url(r'^password_change/$', pdata_app.views.view_change_password,
        name='password_change'),

    url(r'^password_change/done/$',
        pdata_app.views.view_change_password_success,
        name='password_change_done'),

    url(r'^files/$', pdata_app.views.DataFileList.as_view(), name='data_files'),

    url(r'^submissions/$', pdata_app.views.DataSubmissionList.as_view(),
        name='data_submissions'),

    url(r'^esgf_datasets/$', pdata_app.views.ESGFDatasetList.as_view(),
        name='esgf_datasets'),

    url(r'^ceda_datasets/$', pdata_app.views.CEDADatasetList.as_view(),
        name='ceda_datasets'),

    url(r'^data_requests/$', pdata_app.views.DataRequestList.as_view(),
        name='data_requests'),

    url(r'^outstanding_data/$',
        pdata_app.views.OutstandingDataRequestList.as_view(),
        name='outstanding_data'),

    url(r'^received_data/$',
        pdata_app.views.ReceivedDataRequestList.as_view(),
        name='received_data'),

    url(r'^data_issues/$', pdata_app.views.DataIssueList.as_view(),
        name='data_issues'),

    url(r'^variable_requests/$',
        pdata_app.views.VariableRequestList.as_view(),
        name='variable_requests'),

    url(r'^create_submission/$', pdata_app.views.create_submission,
        name='create_submission'),

    url(r'^retrieval_requests/$',
        pdata_app.views.RetrievalRequestList.as_view(),
        name='retrieval_requests'),

    url(r'^confirm_retrieval/$', pdata_app.views.confirm_retrieval,
        name='confirm_retrieval'),

    url(r'^create_retrieval/$', pdata_app.views.create_retrieval,
        name='create_retrieval'),

    url(r'^et_indexer/$', et_indexer.views.view_home, name='et_indexer'),

    url(r'^et_indexer/datafiles/$', et_indexer.views.DatafileList.as_view(),
        name='et_indexer_view_datafiles'),

    url(r'^et_indexer/var_query/$',
        et_indexer.views.VariableOccurrenceList.as_view(),
        name='et_indexer_view_varquery'),

    url(r'.*', pdata_app.views.view_home, name='home'),
]

urlpatterns += staticfiles_urlpatterns()
