from django.contrib import admin

# Register your models here.

#From et_indexer.models import model_names
from et_indexer.models import *

for m in model_names:
    admin.site.register(eval(m))