"""
attribute_update.py - classes to update the values of files' attributes and
keep the files, directory structure and DMT consistent.
"""
from __future__ import unicode_literals, division, absolute_import
from abc import ABCMeta, abstractmethod
import logging
import os
import six

from pdata_app.models import ClimateModel
from pdata_app.utils.common import (construct_drs_path, construct_filename,
                                    get_gws)

logger = logging.getLogger(__name__)


@six.add_metaclass(ABCMeta)
class DmtUpdate(object):
    """
    Abstract base class for all DMT metadata updates.
    """
    def __init__(self, datafile, new_value):
        """
        Initialise the class

        :param pdata_apps.models.DataFile datafile: the file to update
        :param str new_value: the new value to apply
        """
        self.datafile = datafile
        self.new_value = new_value
        self.new_filename = None
        self.new_directory = None

    def update(self):
        """
        Update everything.
        """
        self._update_attribute()
        self.datafile.refresh_from_db()
        self._update_filename()
        self._update_directory()

    @abstractmethod
    def _update_attribute(self):
        """
        Update the attribute in the database.
        """
        pass

    def _update_filename(self):
        """
        Update the file's name in the database.
        """
        self.new_filename = construct_filename(self.datafile)
        self.datafile.name = self.new_filename
        self.datafile.save()

    def _update_directory(self):
        """
        Update the file's directory.
        """
        self.new_directory = os.path.join(get_gws(self.datafile.directory),
                                          construct_drs_path(self.datafile))
        self.datafile.directory = self.new_directory
        self.datafile.save()


class SourceIdUpdate(DmtUpdate):
    """
    Update a DataFile's source_id (climate model).
    """
    def __init__(self, datafile, new_value):
        """
        Initialise the class
        """
        super(SourceIdUpdate, self).__init__(datafile, new_value)

    def _update_attribute(self):
        """
        Update the source_id
        """
        new_source_id = ClimateModel.objects.get(short_name=self.new_value)
        self.datafile.climate_model = new_source_id
        self.datafile.save()


class VariantLabelUpdate(DmtUpdate):
    """
    Update a DataFile's variant_label (rip_code).
    """
    def __init__(self, datafile, new_value):
        """
        Initialise the class
        """
        super(VariantLabelUpdate, self).__init__(datafile, new_value)

    def _update_attribute(self):
        """
        Update the variant label
        """
        self.datafile.rip_code = self.new_value
        self.datafile.save()
