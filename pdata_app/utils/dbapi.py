from django.utils import timezone

from pdata_app.models import *
from vocabs import *


def insert(cls, **props):
    obj = cls(**props)
    obj.save()
    return obj


def match_one(cls, **props):
    match = cls.objects.filter(**props)
    if match.count() == 1:
        return match.first()
    return False


def get_or_create(cls, **props):
    obj, created = cls.objects.get_or_create(**props)
    return obj


def exists(cls, **props):
    if cls.objects.filter(**props):
        return True
    else:
        return False


def get_checksum(data_file, checksum_type="MD5"):
    return data_file.checksum_set.get(checksum_type=checksum_type).checksum_value


def count(cls):
    return cls.objects.count()


def is_paused():
    return Settings.objects.get().is_paused