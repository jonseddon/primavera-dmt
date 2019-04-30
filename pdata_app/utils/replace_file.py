"""
replace_file.py - a function to move DataFile entries to a ReplacedFile entry.
"""
from __future__ import unicode_literals, division, absolute_import

import logging.config
from pdata_app.models import Checksum, DataFile, ReplacedFile
from pdata_app.utils.dbapi import get_or_create

logger = logging.getLogger(__name__)


def replace_files(queryset):
    """
    Move DataFile entries from `queryset` to a ReplacedFile type

    :param django.db.models.query.QuerySet queryset: the DataFile objects to
        move.
    :raises TypeError: if a queryset entry is not a DataFile.
    :raises ValueError: if unable to create a ReplacedFile object
    """
    num_files_moved = 0

    for datafile in queryset:
        if not isinstance(datafile, DataFile):
            raise TypeError('queryset entries must be of type DataFile')

        checksum = datafile.checksum_set.first()
        replacement_file = ReplacedFile.objects.create(
            name=datafile.name,
            incoming_directory=datafile.incoming_directory,
            size=datafile.size,
            version=datafile.version,
            project=datafile.project,
            institute=datafile.institute,
            climate_model=datafile.climate_model,
            activity_id=datafile.activity_id,
            experiment=datafile.experiment,
            variable_request=datafile.variable_request,
            data_request=datafile.data_request,
            frequency=datafile.frequency,
            rip_code=datafile.rip_code,
            grid=datafile.grid,
            start_time=datafile.start_time,
            end_time=datafile.end_time,
            time_units=datafile.time_units,
            calendar=datafile.calendar,
            data_submission=datafile.data_submission,
            tape_url=datafile.tape_url,
            checksum_value=checksum.checksum_value if checksum else None,
            checksum_type=checksum.checksum_type if checksum else None
        )

        if replacement_file:
            datafile.delete()
            num_files_moved += 1
        else:
            raise ValueError('No ReplacedFile object created for {}.'.
                             format(datafile))

    logger.debug('{} files moved.'.format(num_files_moved))


def restore_files(queryset):
    """
    Move ReplacedFile entries from `queryset` to a DataFile type

    :param django.db.models.query.QuerySet queryset: the DataFile objects to
        move.
    :raises TypeError: if a queryset entry is not a ReplacedFile.
    :raises ValueError: if unable to create a DataFile object
    """
    num_files_restored = 0

    for rep_file in queryset:
        if not isinstance(rep_file, ReplacedFile):
            raise TypeError('queryset entries must be of type ReplacedFile')

        data_file = DataFile.objects.create(
            name=rep_file.name,
            incoming_directory=rep_file.incoming_directory,
            directory=None,
            size=rep_file.size,
            tape_size=None,
            version=rep_file.version,
            project=rep_file.project,
            institute=rep_file.institute,
            climate_model=rep_file.climate_model,
            activity_id=rep_file.activity_id,
            experiment=rep_file.experiment,
            variable_request=rep_file.variable_request,
            data_request=rep_file.data_request,
            frequency=rep_file.frequency,
            rip_code=rep_file.rip_code,
            grid=rep_file.grid,
            start_time=rep_file.start_time,
            end_time=rep_file.end_time,
            time_units=rep_file.time_units,
            calendar=rep_file.calendar,
            data_submission=rep_file.data_submission,
            online=False,
            tape_url=rep_file.tape_url
        )

        if data_file:
            checksum = get_or_create(Checksum, data_file=data_file,
                                     checksum_value=rep_file.checksum_value,
                                     checksum_type=rep_file.checksum_type)
            rep_file.delete()
            num_files_restored += 1
        else:
            raise ValueError('No DataFile object created for {}.'.
                             format(rep_file))

    logger.debug('{} files moved.'.format(num_files_restored))
