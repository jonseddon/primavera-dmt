# -*- coding: utf-8 -*-
# Generated by Django 1.10 on 2017-02-22 11:50
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('pdata_app', '0013_auto_20170220_1312'),
    ]

    operations = [
        migrations.AddField(
            model_name='settings',
            name='contact_user_id',
            field=models.CharField(default=b'jseddon', max_length=20, verbose_name=b'Contact User ID'),
        ),
    ]
