from django.contrib import admin

# Register your models here.
from crepe_app.models import *

for mymodel in (Chain, Dataset, ProcessStage, Status, ProcessStageInChain,
           File, Symlink, Event, Checksum, Settings):
    admin.site.register(mymodel)
