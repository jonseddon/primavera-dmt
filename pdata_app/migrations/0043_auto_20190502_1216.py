# -*- coding: utf-8 -*-
# Generated by Django 1.11 on 2019-05-02 12:16
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('pdata_app', '0042_auto_20190403_1315'),
    ]

    operations = [
        migrations.AddField(
            model_name='datafile',
            name='incoming_name',
            field=models.CharField(max_length=200, verbose_name='Original file name', null=True,),
            preserve_default=False,
        ),
        migrations.AlterField(
            model_name='settings',
            name='base_output_dir',
            field=models.CharField(default='/gws/nopw/j04/primavera5/stream1', max_length=300, verbose_name='Base directory for retrieved files'),
        ),
        migrations.AlterField(
            model_name='settings',
            name='current_stream1_dir',
            field=models.CharField(default='/gws/nopw/j04/primavera4/stream1', max_length=300, verbose_name='The directory that retrievals are currently being retrieved to.'),
        ),
    ]
