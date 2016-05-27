from django.contrib import admin
from solo.admin import SingletonModelAdmin

from crepe_app.models import *

# Register your models here.
admin.site.register(Settings, SingletonModelAdmin)

for mymodel in (Chain, Dataset, ProcessStage, Status, ProcessStageInChain,
           File, Symlink, Event, Checksum):
    admin.site.register(mymodel)
