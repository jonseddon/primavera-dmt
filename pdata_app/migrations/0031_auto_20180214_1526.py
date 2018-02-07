# -*- coding: utf-8 -*-
# Generated by Django 1.11 on 2018-02-14 15:26
from __future__ import unicode_literals

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('pdata_app', '0030_auto_20180207_0909'),
    ]

    operations = [
        migrations.AddField(
            model_name='observationdataset',
            name='doi',
            field=models.URLField(blank=True, null=True, verbose_name=b'DOI'),
        ),
        migrations.AddField(
            model_name='observationdataset',
            name='reference',
            field=models.CharField(blank=True, max_length=4000, null=True, verbose_name=b'Reference'),
        ),
        migrations.AlterField(
            model_name='observationfile',
            name='directory',
            field=models.CharField(blank=True, max_length=200, null=True, verbose_name=b'Directory'),
        ),
        migrations.AlterField(
            model_name='observationfile',
            name='incoming_directory',
            field=models.CharField(max_length=500, verbose_name=b'Incoming directory'),
        ),
        migrations.AlterField(
            model_name='observationfile',
            name='obs_set',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='pdata_app.ObservationDataset', verbose_name=b'Obs Set'),
        ),
    ]
