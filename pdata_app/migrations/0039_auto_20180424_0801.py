# -*- coding: utf-8 -*-
# Generated by Django 1.11 on 2018-04-24 08:01
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('pdata_app', '0038_remove_esgfdataset_drs_id'),
    ]

    operations = [
        migrations.AlterField(
            model_name='esgfdataset',
            name='status',
            field=models.CharField(choices=[(b'CREATED', b'CREATED'), (b'SUBMITTED', b'SUBMITTED'), (b'AT_CEDA', b'AT_CEDA'), (b'PUBLISHED', b'PUBLISHED'), (b'REJECTED', b'REJECTED'), (b'NEEDS_FIX', b'NEEDS_FIX'), (b'FILES_MISSING', b'FILES_MISSING'), (b'NOT_ON_DISK', b'NOT_ON_DISK')], default=b'CREATED', max_length=20, verbose_name=b'Status'),
        ),
    ]
