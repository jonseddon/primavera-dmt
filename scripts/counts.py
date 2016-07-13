import django
django.setup()

from pdata_app.models import *

for cls in (Dataset, Chain, Status):
    print cls.__name__
    print cls.objects.count()
