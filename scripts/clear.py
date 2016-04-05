import django
django.setup()

from crepe_app.models import *
from crepe_app.models import __all__ as all_class_names

classes = [eval(cls_name) for cls_name in all_class_names]
 
for cls in classes:
    print cls.__name__
    cls.objects.all().delete()

print "Deleted all!"
