# -*- coding: utf-8 -*-
# Generated by Django 1.11 on 2019-05-02 12:24
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('pdata_app', '0044_auto_20190502_1224'),
    ]

    operations = [
        migrations.AlterField(
            model_name='datafile',
            name='incoming_name',
            field=models.CharField(max_length=200,
                                   verbose_name='Original file name',),
            preserve_default=False,
        ),

    ]
