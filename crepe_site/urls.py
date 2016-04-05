from django.conf.urls import patterns, include, url
from django.contrib.staticfiles.urls import staticfiles_urlpatterns

#from crepe_app import models

from django.contrib import admin
admin.autodiscover()

urlpatterns = patterns('',
    # Examples:
    # url(r'^$', 'crepe_site.views.home', name='home'),
    # url(r'^blog/', include('blog.urls')),
    url(r'^admin/', include(admin.site.urls)),
    url(r'^files/$', 'crepe_app.views.view_files'),
    url(r'^chains/$', 'crepe_app.views.view_chains'),
    url(r'.*', 'crepe_app.views.view_home'),
)

urlpatterns += staticfiles_urlpatterns()