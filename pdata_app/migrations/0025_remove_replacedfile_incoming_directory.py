# -*- coding: utf-8 -*-
# Generated by Django 1.10 on 2017-12-14 11:52
from __future__ import unicode_literals

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('pdata_app', '0024_replacedfile'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='replacedfile',
            name='incoming_directory',
        ),
    ]
