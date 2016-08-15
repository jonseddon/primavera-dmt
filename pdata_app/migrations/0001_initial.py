# -*- coding: utf-8 -*-
# Generated by Django 1.9.7 on 2016-08-15 11:19
from __future__ import unicode_literals

from django.db import migrations, models
import django.db.models.deletion
import django.utils.timezone


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='CEDADataset',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('catalogue_url', models.URLField(verbose_name=b'CEDA Catalogue URL')),
                ('directory', models.CharField(max_length=500, verbose_name=b'Directory')),
                ('doi', models.URLField(blank=True, null=True, verbose_name=b'DOI')),
            ],
            options={
                'abstract': False,
            },
        ),
        migrations.CreateModel(
            name='Checksum',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('checksum_value', models.CharField(max_length=200)),
                ('checksum_type', models.CharField(choices=[(b'SHA256', b'SHA256'), (b'MD5', b'MD5'), (b'ADLER32', b'ADLER32')], max_length=20)),
            ],
        ),
        migrations.CreateModel(
            name='ClimateModel',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('short_name', models.CharField(max_length=100)),
                ('full_name', models.CharField(max_length=300)),
            ],
        ),
        migrations.CreateModel(
            name='DataFile',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=200, verbose_name=b'File name')),
                ('incoming_directory', models.CharField(max_length=500, verbose_name=b'Incoming directory')),
                ('directory', models.CharField(max_length=500, verbose_name=b'Current directory')),
                ('size', models.BigIntegerField(verbose_name=b'File size (bytes)')),
                ('frequency', models.CharField(choices=[(b'ann', b'ann'), (b'mon', b'mon'), (b'day', b'day'), (b'6hr', b'6hr'), (b'3hr', b'3hr'), (b'1hr', b'1hr'), (b'subhr', b'subhr'), (b'fx', b'fx')], max_length=20, verbose_name=b'Time frequency')),
                ('rip_code', models.CharField(max_length=20, verbose_name=b'RIP code')),
                ('start_time', models.DateTimeField(blank=True, null=True, verbose_name=b'Start time')),
                ('end_time', models.DateTimeField(blank=True, null=True, verbose_name=b'End time')),
                ('ceda_download_url', models.URLField(blank=True, null=True, verbose_name=b'CEDA Download URL')),
                ('ceda_opendap_url', models.URLField(blank=True, null=True, verbose_name=b'CEDA OpenDAP URL')),
                ('esgf_download_url', models.URLField(blank=True, null=True, verbose_name=b'ESGF Download URL')),
                ('esgf_opendap_url', models.URLField(blank=True, null=True, verbose_name=b'ESGF OpenDAP URL')),
                ('online', models.BooleanField(default=True, verbose_name=b'Is the file online?')),
                ('tape_url', models.URLField(blank=True, null=True, verbose_name=b'Tape URL')),
                ('ceda_dataset', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, to='pdata_app.CEDADataset')),
                ('climate_model', models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, to='pdata_app.ClimateModel')),
            ],
            options={
                'verbose_name': 'Data File',
            },
        ),
        migrations.CreateModel(
            name='DataIssue',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('issue', models.CharField(max_length=500, verbose_name=b'Issue reported')),
                ('reporter', models.CharField(max_length=60, verbose_name=b'Reporter')),
                ('date_time', models.DateTimeField(default=django.utils.timezone.now, verbose_name=b'Date and time of report')),
                ('data_file', models.ManyToManyField(to='pdata_app.DataFile')),
            ],
        ),
        migrations.CreateModel(
            name='DataRequest',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('frequency', models.CharField(choices=[(b'ann', b'ann'), (b'mon', b'mon'), (b'day', b'day'), (b'6hr', b'6hr'), (b'3hr', b'3hr'), (b'1hr', b'1hr'), (b'subhr', b'subhr'), (b'fx', b'fx')], max_length=20, verbose_name=b'Time frequency')),
                ('start_time', models.DateTimeField(verbose_name=b'Start time')),
                ('end_time', models.DateTimeField(verbose_name=b'End time')),
                ('climate_model', models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, to='pdata_app.ClimateModel')),
            ],
        ),
        migrations.CreateModel(
            name='DataSubmission',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('status', models.CharField(choices=[(b'EXPECTED', b'EXPECTED'), (b'ARRIVED', b'ARRIVED'), (b'VALIDATED', b'VALIDATED'), (b'ARCHIVED', b'ARCHIVED'), (b'PUBLISHED', b'PUBLISHED')], default=b'EXPECTED', max_length=20, verbose_name=b'Status')),
                ('incoming_directory', models.CharField(max_length=500, verbose_name=b'Incoming Directory')),
                ('directory', models.CharField(max_length=500, verbose_name=b'Main Directory')),
            ],
            options={
                'abstract': False,
            },
        ),
        migrations.CreateModel(
            name='ESGFDataset',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('drs_id', models.CharField(max_length=500, verbose_name=b'DRS Dataset Identifier')),
                ('version', models.CharField(max_length=20, verbose_name=b'Version')),
                ('directory', models.CharField(max_length=500, verbose_name=b'Directory')),
                ('thredds_url', models.URLField(blank=True, null=True, verbose_name=b'THREDDS Download URL')),
                ('ceda_dataset', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, to='pdata_app.CEDADataset')),
                ('data_submission', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, to='pdata_app.DataSubmission')),
            ],
            options={
                'verbose_name': 'ESGF Dataset',
            },
        ),
        migrations.CreateModel(
            name='Experiment',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('short_name', models.CharField(max_length=100)),
                ('full_name', models.CharField(max_length=300)),
            ],
        ),
        migrations.CreateModel(
            name='Institute',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('short_name', models.CharField(max_length=100)),
                ('full_name', models.CharField(max_length=300)),
            ],
        ),
        migrations.CreateModel(
            name='Project',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('short_name', models.CharField(max_length=100)),
                ('full_name', models.CharField(max_length=300)),
            ],
        ),
        migrations.CreateModel(
            name='Settings',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('is_paused', models.BooleanField(default=False)),
            ],
            options={
                'verbose_name': 'Settings',
            },
        ),
        migrations.CreateModel(
            name='Variable',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('var_id', models.CharField(max_length=100, verbose_name=b'Variable Id')),
                ('units', models.CharField(max_length=100, verbose_name=b'Units')),
                ('long_name', models.CharField(blank=True, max_length=100, null=True, verbose_name=b'Long Name')),
                ('standard_name', models.CharField(blank=True, max_length=100, null=True, verbose_name=b'CF Standard Name')),
            ],
        ),
        migrations.CreateModel(
            name='VariableRequest',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('table_name', models.CharField(max_length=30)),
                ('long_name', models.CharField(max_length=200)),
                ('units', models.CharField(max_length=200)),
                ('var_name', models.CharField(max_length=30)),
                ('standard_name', models.CharField(max_length=100)),
                ('cell_methods', models.CharField(max_length=200)),
                ('positive', models.CharField(blank=True, max_length=20, null=True)),
                ('variable_type', models.CharField(choices=[(b'real', b'real'), (b'None', b'None'), (b'', b'')], max_length=20)),
                ('dimensions', models.CharField(max_length=200)),
                ('cmor_name', models.CharField(max_length=20)),
                ('modeling_realm', models.CharField(max_length=20)),
                ('frequency', models.CharField(choices=[(b'ann', b'ann'), (b'mon', b'mon'), (b'day', b'day'), (b'6hr', b'6hr'), (b'3hr', b'3hr'), (b'1hr', b'1hr'), (b'subhr', b'subhr'), (b'fx', b'fx')], max_length=200)),
                ('cell_measures', models.CharField(max_length=200)),
                ('uid', models.CharField(max_length=200)),
            ],
        ),
        migrations.AddField(
            model_name='datarequest',
            name='experiment',
            field=models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, to='pdata_app.Experiment'),
        ),
        migrations.AddField(
            model_name='datarequest',
            name='institute',
            field=models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, to='pdata_app.Institute'),
        ),
        migrations.AddField(
            model_name='datarequest',
            name='variable',
            field=models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, to='pdata_app.Variable'),
        ),
        migrations.AddField(
            model_name='datarequest',
            name='variable_request',
            field=models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, to='pdata_app.VariableRequest'),
        ),
        migrations.AddField(
            model_name='datafile',
            name='data_submission',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='pdata_app.DataSubmission'),
        ),
        migrations.AddField(
            model_name='datafile',
            name='esgf_dataset',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, to='pdata_app.ESGFDataset'),
        ),
        migrations.AddField(
            model_name='datafile',
            name='experiment',
            field=models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, to='pdata_app.Experiment'),
        ),
        migrations.AddField(
            model_name='datafile',
            name='project',
            field=models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, to='pdata_app.Project'),
        ),
        migrations.AddField(
            model_name='datafile',
            name='variable',
            field=models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, to='pdata_app.Variable'),
        ),
        migrations.AddField(
            model_name='checksum',
            name='data_file',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='pdata_app.DataFile'),
        ),
        migrations.AlterUniqueTogether(
            name='esgfdataset',
            unique_together=set([('drs_id', 'version')]),
        ),
        migrations.AlterUniqueTogether(
            name='datafile',
            unique_together=set([('name', 'directory')]),
        ),
    ]
