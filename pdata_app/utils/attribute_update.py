"""
attribute_update.py - classes to update the values of files' attributes and
keep the files, directory structure and DMT consistent.
"""
from __future__ import unicode_literals, division, absolute_import
from abc import ABCMeta, abstractmethod
import logging
import os
import re
import six

import django

from pdata_app.models import ClimateModel, DataRequest, Settings
from pdata_app.utils.common import (construct_drs_path, construct_filename,
                                    get_gws, delete_drs_dir, is_same_gws,
                                    run_ncatted)

logger = logging.getLogger(__name__)

# The top-level directory to write output data to
BASE_OUTPUT_DIR = Settings.get_solo().base_output_dir


class AttributeUpdateError(Exception):
    """
    Base class for all custom exceptions
    """
    pass


class FileOfflineError(AttributeUpdateError):
    """
    Raised when a file is marked as offline in the DMT
    """
    def __init__(self, directory, filename):
        message = '{} is offline'.format(os.path.join(directory, filename))
        Exception.__init__(self, message)


class FileNotOnDiskError(AttributeUpdateError):
    """
    Raised when a file is not found on disk
    """
    def __init__(self, directory, filename):
        message = '{} was not found on disk'.format(os.path.join(directory,
                                                                 filename))
        Exception.__init__(self, message)


class SymLinkIsFileError(AttributeUpdateError):
    """
    Raised when a file is found when a symbolic link was expected.
    """
    def __init__(self, filepath):
        message = ("{} was expected to be a symbolic link but isn't.".
            format(filepath))
        Exception.__init__(self, message)


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
        self.old_filename = self.datafile.name
        self.old_directory = self.datafile.directory
        self.old_sym_link_dir = os.path.join(BASE_OUTPUT_DIR,
                                             construct_drs_path(self.datafile))
        self.new_filename = None
        self.new_directory = None

        # The name and value of the data_request attribute being modified
        self.data_req_attribute_name = None
        self.data_req_attribute_value = None

        # The destination data_request
        self.new_dreq = None

    def update(self):
        """
        Update everything.
        """
        self._find_new_dreq()
        self._check_available()
        self._update_database_attribute()
        self._update_file_attribute()
        self._update_filename()
        self._update_directory()
        self._rename_file()
        self._move_dreq()

    def _find_new_dreq(self):
        """
        Find the new data request. If it can't be find the data request (or
        there are multiple ones) then Django will raise an exception so that we
        don't make any changes to the files or DB.
        """
        if self.data_req_attribute_name is None:
            raise NotImplementedError("data_req_attribute_name hasn't been "
                                      "set.")
        if self.data_req_attribute_value is None:
            raise NotImplementedError("data_req_attribute_value hasn't been "
                                      "set.")

        # the default values from the existing data request
        dreq_dict = {
            'project': self.datafile.data_request.project,
            'institute': self.datafile.data_request.institute,
            'climate_model': self.datafile.data_request.climate_model,
            'experiment': self.datafile.data_request.experiment,
            'variable_request': self.datafile.data_request.variable_request,
            'rip_code': self.datafile.data_request.rip_code
        }
        # overwrite with the new value
        dreq_dict[self.data_req_attribute_name] = self.data_req_attribute_value

        # find the data request
        self.new_dreq = DataRequest.objects.get(**dreq_dict)

    def _check_available(self):
        """
        Check that the file is online in the DMT and can be found in its
        specified location on disk.

        :raises FileOfflineError: if file does not have a status of online in
            the DMT.
        :raises FileNotOnDiskError: if the file is not found on disk.
        """
        if not self.datafile.online:
            raise FileOfflineError(self.old_directory, self.old_filename)

        if not os.path.exists(os.path.join(self.old_directory,
                                           self.old_filename)):
            raise FileNotOnDiskError(self.old_directory, self.old_filename)

    @abstractmethod
    def _update_database_attribute(self):
        """
        Update the attribute in the database.
        """
        pass

    @abstractmethod
    def _update_file_attribute(self):
        """
        Update the metadata attribute in the file.
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

    def _rename_file(self):
        """
        Rename the file on disk and move to its new directory. Update the link
        from the primary directory.
        """
        if not os.path.exists(self.new_directory):
            os.makedirs(self.new_directory)

        os.rename(os.path.join(self.old_directory, self.old_filename),
                  os.path.join(self.new_directory, self.new_filename))

        # check for empty directory
        if not os.listdir(self.old_directory):
            delete_drs_dir(self.old_directory)

        # Update the symbolic link if required
        if not is_same_gws(self.old_directory, BASE_OUTPUT_DIR):
            old_link_path = os.path.join(self.old_sym_link_dir,
                                         self.old_filename)
            if os.path.lexists(old_link_path):
                if not os.path.islink(old_link_path):
                    logger.error("{} exists and isn't a symbolic link.".
                                 format(old_link_path))
                    raise SymLinkIsFileError(old_link_path)
                else:
                    # it is a link so remove it
                    os.remove(old_link_path)
                    # check for empty directory
                    if not os.listdir(self.old_sym_link_dir):
                        delete_drs_dir(self.old_sym_link_dir)

            new_link_dir = os.path.join(BASE_OUTPUT_DIR,
                                        construct_drs_path(self.datafile))
            if not os.path.exists(new_link_dir):
                os.makedirs(new_link_dir)
            os.symlink(os.path.join(self.new_directory, self.new_filename),
                       os.path.join(new_link_dir, self.new_filename))

    def _move_dreq(self):
        """
        Move the data file to the new data request
        """
        self.datafile.data_request = self.new_dreq
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
        self.data_req_attribute_name = 'climate_model'
        self.data_req_attribute_value = ClimateModel.objects.get(
            short_name=self.new_value
        )

    def _update_database_attribute(self):
        """
        Update the source_id
        """
        new_source_id = ClimateModel.objects.get(short_name=self.new_value)
        self.datafile.climate_model = new_source_id
        self.datafile.save()

    def _update_file_attribute(self):
        """
        Update the source_id and make the same change in the further_info_url.
        """
        # source_id
        run_ncatted(self.datafile.directory, self.datafile.name,
                    'source_id', 'global', 'c', self.new_value, False)

        # further_info_url
        further_info_url = ('https://furtherinfo.es-doc.org/{}.{}.{}.{}.none.{}'.
                            format(self.datafile.project.short_name,
                                   self.datafile.institute.short_name,
                                   self.new_value,
                                   self.datafile.experiment.short_name,
                                   self.datafile.rip_code))
        run_ncatted(self.datafile.directory, self.datafile.name,
                    'further_info_url', 'global', 'c', further_info_url, False)


class VariantLabelUpdate(DmtUpdate):
    """
    Update a DataFile's variant_label (rip_code).
    """
    def __init__(self, datafile, new_value):
        """
        Initialise the class
        """
        super(VariantLabelUpdate, self).__init__(datafile, new_value)
        self.data_req_attribute_name = 'rip_code'
        self.data_req_attribute_value = self.new_value

    def _update_database_attribute(self):
        """
        Update the variant label
        """
        self.datafile.rip_code = self.new_value
        self.datafile.save()

    def _update_file_attribute(self):
        """
        Update the variant_label and make the same change in its constituent
        parts and the further_info_url.
        """
        # variant_label
        run_ncatted(self.datafile.directory, self.datafile.name,
                    'variant_label', 'global', 'c', self.new_value, False)

        # indexes
        ripf = re.match('^r(\d+)i(\d+)p(\d+)f(\d+)$', self.new_value)
        run_ncatted(self.datafile.directory, self.datafile.name,
                    'realization_index', 'global', 's', int(ripf.group(1)),
                    False)
        run_ncatted(self.datafile.directory, self.datafile.name,
                    'initialization_index', 'global', 's', int(ripf.group(2)),
                    False)
        run_ncatted(self.datafile.directory, self.datafile.name,
                    'physics_index', 'global', 's', int(ripf.group(3)),
                    False)
        run_ncatted(self.datafile.directory, self.datafile.name,
                    'forcing_index', 'global', 's', int(ripf.group(4)),
                    False)

        # further_info_url
        further_info_url = ('https://furtherinfo.es-doc.org/{}.{}.{}.{}.none.{}'.
                            format(self.datafile.project.short_name,
                                   self.datafile.institute.short_name,
                                   self.datafile.climate_model.short_name,
                                   self.datafile.experiment.short_name,
                                   self.new_value))
        run_ncatted(self.datafile.directory, self.datafile.name,
                    'further_info_url', 'global', 'c', further_info_url, False)
