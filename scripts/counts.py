import django
django.setup()

from crepe_app.models import *

for cls in (Dataset, Chain, Status):
    print cls.__name__
    print cls.objects.count()
