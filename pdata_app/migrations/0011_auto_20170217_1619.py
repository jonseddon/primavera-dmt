# -*- coding: utf-8 -*-
# Generated by Django 1.10 on 2017-02-17 16:19
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('pdata_app', '0010_remove_retrievalrequest_lotus_job'),
    ]

    operations = [
        migrations.AlterField(
            model_name='datafile',
            name='size',
            field=models.BigIntegerField(verbose_name=b'File size'),
        ),
    ]
