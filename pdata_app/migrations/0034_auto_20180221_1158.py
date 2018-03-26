# -*- coding: utf-8 -*-
# Generated by Django 1.11 on 2018-02-21 11:58
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('pdata_app', '0033_auto_20180216_1637'),
    ]

    operations = [
        migrations.AlterField(
            model_name='observationfile',
            name='long_name',
            field=models.CharField(blank=True, max_length=500, null=True, verbose_name=b'Long name'),
        ),
        migrations.AlterField(
            model_name='observationfile',
            name='standard_name',
            field=models.CharField(blank=True, max_length=500, null=True, verbose_name=b'Standard name'),
        ),
        migrations.AlterField(
            model_name='observationfile',
            name='var_name',
            field=models.CharField(blank=True, max_length=200, null=True, verbose_name=b'Var name'),
        ),
    ]