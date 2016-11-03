import re

import cf_units

from django.db import models
from django.utils import timezone
from solo.models import SingletonModel
from django.db.models import PROTECT, SET_NULL, CASCADE
from django.core.exceptions import ValidationError

from vocabs import (STATUS_VALUES, FREQUENCY_VALUES, ONLINE_STATUS,
    CHECKSUM_TYPES, VARIABLE_TYPES, CALENDARS)

model_names = ['Project', 'Institute', 'ClimateModel', 'Experiment',
               'DataSubmission', 'DataFile', 'ESGFDataset', 'CEDADataset',
               'DataRequest', 'DataIssue', 'Checksum', 'Settings',
               'VariableRequest']
__all__ = model_names


class Settings(SingletonModel):
    """
    Global settings for the app (that can be changed within the app
    """
    is_paused = models.BooleanField(default=False, null=False)
    standard_time_units = models.CharField(max_length=50,
        verbose_name='Standard Time Units', default='days since 1950-01-01')

    class Meta:
        verbose_name = "Settings"

    def __unicode__(self):
        return u"App Settings"


class Project(models.Model):
    """
    A project
    """
    # RFK Relationships
    # DataFile

    short_name = models.CharField(max_length=100, null=False,
        blank=False, unique=True)
    full_name = models.CharField(max_length=300, null=False, blank=False)

    def __unicode__(self):
        return self.short_name


class Institute(models.Model):
    """
    An institute
    """
    # RFK Relationships
    # DataRequest

    short_name = models.CharField(max_length=100, null=False,
        blank=False, unique=True)
    full_name = models.CharField(max_length=300, null=False, blank=False)

    def __unicode__(self):
        return self.short_name

# TODO Should we have Individual in here???


class ClimateModel(models.Model):
    """
    A climate model
    """
    # RFK Relationships
    # DataFile
    # DataRequest

    short_name = models.CharField(max_length=100, null=False,
        blank=False, unique=True)
    full_name = models.CharField(max_length=300, null=False, blank=False)

    def __unicode__(self):
        return self.short_name


class Experiment(models.Model):
    """
    An experiment
    """
    # RFK Relationships
    # DataFile
    # DataRequest

    short_name = models.CharField(max_length=100, null=False,
        blank=False, unique=True)
    full_name = models.CharField(max_length=300, null=False, blank=False)

    def __unicode__(self):
        return self.short_name


class VariableRequest(models.Model):
    """
    A variable requested in the CMIP6 data request
    """
    table_name = models.CharField(max_length=30, null=False, blank=False)
    long_name = models.CharField(max_length=200, null=False, blank=False)
    units = models.CharField(max_length=200, null=False, blank=False)
    var_name = models.CharField(max_length=30, null=False, blank=False)
    standard_name = models.CharField(max_length=100, null=False, blank=False)
    cell_methods = models.CharField(max_length=200, null=False, blank=False)
    positive = models.CharField(max_length=20, null=True, blank=True)
    variable_type = models.CharField(max_length=20, choices=VARIABLE_TYPES.items(), null=False, blank=False)
    dimensions = models.CharField(max_length=200, null=False, blank=False)
    cmor_name = models.CharField(max_length=20, null=False, blank=False)
    modeling_realm = models.CharField(max_length=20, null=False, blank=False)
    frequency = models.CharField(max_length=200, choices=FREQUENCY_VALUES.items(), null=False, blank=False)
    cell_measures = models.CharField(max_length=200, null=False, blank=False)
    uid = models.CharField(max_length=200, null=False, blank=False)

    def __unicode__(self):
        return 'VariableRequest: {} ({})'.format(self.cmor_name, self.table_name)


class DataFileAggregationBase(models.Model):
    """
    An abstract base class for datasets containing many files.

    Includes a number of convenience methods to calculate
    aggregations of properties.
    """
    class Meta:
        abstract = True

    def _file_aggregation(self, field_name):
        records = [getattr(datafile, field_name) for datafile in self.get_data_files()]
        # Return unique sorted set of records
        return sorted(set(records))

    def get_data_files(self):
        return self.datafile_set.all()

    def project(self):
        return self._file_aggregation("project")

    def climate_model(self):
        return self._file_aggregation("climate_model")

    def frequency(self):
        return self._file_aggregation("frequency")

    def variables(self):
        return self._file_aggregation("variable_request")

    def get_tape_urls(self):
        return self._file_aggregation("tape_url")

    def get_file_versions(self):
        return self._file_aggregation("version")

    def get_data_issues(self):
        records = []
        for datafile in self.get_data_files():
            records.extend(datafile.dataissue_set.all())

        records = list(set(records))
        records.sort(key=lambda di: di.date_time, reverse=True)
        return records

    def assign_data_issue(self, issue_text, reporter, date_time=None, time_units=None):
        """
        Creates a DataIssue and attaches it to all related DataFile records.
        """
        date_time = date_time or timezone.now()
        data_issue, _tf = DataIssue.objects.get_or_create(issue=issue_text,
            reporter=reporter, date_time=date_time, time_units=time_units)
        data_issue.save()

        data_files = self.get_data_files()
        data_issue.data_file.add(*data_files)

    def start_time(self):
        std_units = Settings.get_solo().standard_time_units

        start_times = self.datafile_set.values_list('start_time', 'time_units',
            'calendar')

        if not start_times:
            return None

        std_times = [
            (_standardise_time_unit(time, unit, std_units, cal), cal)
            for time, unit, cal in start_times
        ]

        none_values_removed = [(std_time, cal)
                               for std_time, cal in std_times
                               if std_time is not None]

        earliest_time, calendar = min(none_values_removed, key=lambda x: x[0])

        earliest_obj = cf_units.num2date(earliest_time, std_units, calendar)

        return earliest_obj

    def end_time(self):
        std_units = Settings.get_solo().standard_time_units

        end_times = self.datafile_set.values_list('end_time', 'time_units',
            'calendar')

        if not end_times:
            return None

        std_times = [
            (_standardise_time_unit(time, unit, std_units, cal), cal)
            for time, unit, cal in end_times
        ]

        latest_time, calendar = max(std_times, key=lambda x: x[0])

        latest_obj = cf_units.num2date(latest_time, std_units, calendar)

        return latest_obj

    def online_status(self):
        """
        Checks aggregation of online status of all DataFiles.
        Returns one of:
            ONLINE_STATUS.online
            ONLINE_STATUS.offline
            ONLINE_STATUS.partial
        """
        files_online = self.datafile_set.filter(online=True).count()
        files_offline = self.datafile_set.filter(online=False).count()

        if files_offline:
            if files_online:
                return ONLINE_STATUS.partial
            else:
                return ONLINE_STATUS.offline
        else:
            return ONLINE_STATUS.online


class DataSubmission(DataFileAggregationBase):
    """
    A directory containing a directory tree of data files copied to the
    platform.
    """

    # RFK relationships:
    # ESGFDatasets: ESGFDataset
    # DataFiles:        DataFile

    # Dynamically aggregated from DataFile information:
    # project
    # climate_model
    # frequency
    # variables
    # data_issues
    # start_time
    # end_time

    status = models.CharField(max_length=20, choices=STATUS_VALUES.items(), verbose_name='Status',
                              default=STATUS_VALUES.EXPECTED, blank=False, null=False)
    incoming_directory = models.CharField(max_length=500, verbose_name='Incoming Directory', blank=False, null=False)
    # Current directory
    directory = models.CharField(max_length=500, verbose_name='Current Directory', blank=False, null=False)
    user = models.CharField(max_length=100, blank=False, null=False)
    date_submitted = models.DateTimeField(auto_now_add=True, null=False, blank=False)

    def __unicode__(self):
        return "Data Submission: %s" % self.directory


class CEDADataset(DataFileAggregationBase):
    """
    A CEDA Dataset - a collection of ESGF Datasets that are held in the
    CEDA Data catalogue (http://catalogue.ceda.ac.uk) with their own metadata
    records.
    """
    # RFK relationships:
    # ESGFDataset

    # Dynamically aggregated from DataFile information:
    # project
    # climate_model
    # frequency
    # variables
    # data_issues
    # start_time
    # end_time

    catalogue_url = models.URLField(verbose_name="CEDA Catalogue URL", blank=False, null=False)
    directory = models.CharField(max_length=500, verbose_name="Directory", blank=False, null=False)

    # The CEDA Dataset might have a DOI
    doi = models.URLField(verbose_name="DOI", blank=True, null=True)

    def __unicode__(self):
        return "CEDA Dataset: %s" % self.catalogue_url


class ESGFDataset(DataFileAggregationBase):
    """
    An ESGF Dataset - a collection of files with an identifier.

    This model uses the Directory Reference Syntax (DRS) dataset which consists of:
        * drs_id - string representing facets of the DRS, e.g. "a.b.c.d"
        * version - string representing the version, e.g. "v20160312"
        * directory - string representing the directory containing the actual data files.
    And a method:
        * get_full_id: Returns full DRS Id made up of drsId version as: `self.drs_id`.`self.version`.
    """
    class Meta:
        unique_together = ('drs_id', 'version')
        verbose_name = "ESGF Dataset"

    # RFK relationships:
    # files: DataFile

    # Dynamically aggregated from DataFile information:
    # project
    # climate_model
    # frequency
    # variables
    # data_issues
    # start_time
    # end_time

    drs_id = models.CharField(max_length=500, verbose_name='DRS Dataset Identifier', blank=False, null=False)
    version = models.CharField(max_length=20, verbose_name='Version', blank=False, null=False)
    directory = models.CharField(max_length=500, verbose_name='Directory', blank=False, null=False)

    thredds_url = models.URLField(verbose_name="THREDDS Download URL", blank=True, null=True)

    # Each ESGF Dataset will be part of one CEDADataset
    ceda_dataset = models.ForeignKey(CEDADataset, blank=True, null=True,
        on_delete=SET_NULL)

    # Each ESGF Dataset will be part of one submission
    data_submission = models.ForeignKey(DataSubmission, blank=True, null=True,
        on_delete=SET_NULL)

    def get_full_id(self):
        """
        Return full DRS Id made up of drsId version as: drs_id.version
        """
        return "%s.%s" % (self.drs_id, self.version)

    def clean(self, *args, **kwargs):
        if not re.match(r"^v\d+$", self.version):
            raise ValidationError('Version must begin with letter "v" followed '
                                  'by a number (date).')

        if not self.directory.startswith("/"):
            raise ValidationError('Directory must begin with "/" because it is '
                                  'a full directory path.')

        if self.directory.endswith("/"):
            self.directory = self.directory.rstrip("/")

        super(ESGFDataset, self).save(*args, **kwargs)

    def __unicode__(self):
        """
        Returns full DRS Id.
        """
        return self.get_full_id()


class DataRequest(models.Model):
    """
    A Data Request for a given set of inputs
    """

    project = models.ForeignKey(Project, null=False, on_delete=PROTECT)
    institute = models.ForeignKey(Institute, null=False, on_delete=PROTECT)
    climate_model = models.ForeignKey(ClimateModel, null=False,
        on_delete=PROTECT)
    experiment = models.ForeignKey(Experiment, null=False, on_delete=PROTECT)
    variable_request = models.ForeignKey(VariableRequest, null=False,
        on_delete=PROTECT)
    rip_code = models.CharField(max_length=20, verbose_name="RIP code",
        null=True, blank=True)
    start_time = models.FloatField(verbose_name="Start time", null=False, blank=False)
    end_time = models.FloatField(verbose_name="End time", null=False, blank=False)
    time_units = models.CharField(verbose_name='Time units', max_length=50, null=False, blank=False)
    calendar = models.CharField(verbose_name='Calendar', max_length=20,
        null=False, blank=False, choices=CALENDARS.items())

    def start_date_string(self):
        """Return a string containing the start date"""
        dto = cf_units.num2date(self.start_time, self.time_units, self.calendar)
        return dto.strftime('%Y-%m-%d')

    def end_date_string(self):
        """Return a string containing the end date"""
        dto = cf_units.num2date(self.end_time, self.time_units, self.calendar)
        return dto.strftime('%Y-%m-%d')


class DataFile(models.Model):
    """
    A data file
    """

    # RFK relationships:
    # checksums: Checksum - multiple is OK

    # ManyToMany relationships:
    # DataIssues: DataIssue - multiple is OK

    name = models.CharField(max_length=200, verbose_name="File name", null=False, blank=False)
    incoming_directory = models.CharField(max_length=500, verbose_name="Incoming directory", null=False, blank=False)

    # This is where the datafile is now
    directory = models.CharField(max_length=500, verbose_name="Current directory", null=False, blank=False)
    size = models.BigIntegerField(null=False, verbose_name="File size (bytes)")

    # This is the file's version
    version = models.CharField(max_length=10, verbose_name='File Version',
                               null=True, blank=True)

    # Scientific metadata
    project = models.ForeignKey(Project, null=False, on_delete=PROTECT)
    institute = models.ForeignKey(Institute, null=False, on_delete=PROTECT)
    climate_model = models.ForeignKey(ClimateModel, null=False, on_delete=PROTECT)
    experiment = models.ForeignKey(Experiment, null=False, on_delete=PROTECT)
    variable_request = models.ForeignKey(VariableRequest, null=False, on_delete=PROTECT)
    frequency = models.CharField(max_length=20, choices=FREQUENCY_VALUES.items(),
        verbose_name="Time frequency", null=False, blank=False)
    rip_code = models.CharField(max_length=20, verbose_name="RIP code",
        null=False, blank=False)

    # DateTimes are allowed to be null/blank because some fields (such as orography)
    # are time-independent
    start_time = models.FloatField(verbose_name="Start time", null=True, blank=True)
    end_time = models.FloatField(verbose_name="End time", null=True, blank=True)
    time_units = models.CharField(verbose_name='Time units', max_length=50, null=True, blank=True)
    calendar = models.CharField(verbose_name='Calendar', max_length=20,
        null=True, blank=True, choices=CALENDARS.items())

    data_submission = models.ForeignKey(DataSubmission, null=False, blank=False,
        on_delete=CASCADE)

    # These Dataset fields might only be known after processing so they can be null/blank
    esgf_dataset = models.ForeignKey(ESGFDataset, null=True, blank=True,
        on_delete=SET_NULL)
    ceda_dataset = models.ForeignKey(CEDADataset, null=True, blank=True,
        on_delete=SET_NULL)

    # URLs will not be known at start so can be blank
    ceda_download_url = models.URLField(verbose_name="CEDA Download URL", null=True, blank=True)
    ceda_opendap_url = models.URLField(verbose_name="CEDA OpenDAP URL", null=True, blank=True)

    esgf_download_url = models.URLField(verbose_name="ESGF Download URL", null=True, blank=True)
    esgf_opendap_url = models.URLField(verbose_name="ESGF OpenDAP URL", null=True, blank=True)

    # Tape status
    online = models.BooleanField(default=True, verbose_name="Is the file online?", null=False, blank=False)
    tape_url = models.CharField(verbose_name="Tape URL", max_length=200, null=True, blank=True)

    def __unicode__(self):
        return "%s (Directory: %s)" % (self.name, self.directory)

    class Meta:
        unique_together = ('name', 'directory')
        verbose_name = "Data File"


class DataIssue(models.Model):
    """
    A recorded issue with a DataFile

    NOTE: You can have multiple data issues related to a single DataFile
    NOTE: Aggregation is used to associate a DataIssue with an ESGFDataset, CEDADataset or DataSubmission
    """
    issue = models.CharField(max_length=500, verbose_name="Issue reported", null=False, blank=False)
    reporter = models.CharField(max_length=60, verbose_name="Reporter", null=False, blank=False)
    date_time = models.FloatField(verbose_name="Date and time of report", null=False, blank=False)
    time_units = models.CharField(verbose_name='Time units', max_length=50, null=False, blank=False)
    calendar = models.CharField(verbose_name='Calendar', max_length=20,
        null=False, blank=False, choices=CALENDARS.items())

    # DataFile that the Data Issue corresponds to
    data_file = models.ManyToManyField(DataFile)

    def __unicode__(self):
        return "Data Issue (%s): %s (%s)" % (
            cf_units.num2date(self.date_time, self.time_units, self.calendar).
            strftime('%Y-%m-%d %H:%M:%S'), self.issue, self.reporter)

    def date_time_string(self):
        """Return a string containing the issue date and time"""
        dto = cf_units.num2date(self.date_time, self.time_units, self.calendar)
        return dto.strftime('%Y-%m-%d %H:%M:%S')


class Checksum(models.Model):
    """
    A checksum
    """
    data_file = models.ForeignKey(DataFile, null=False, blank=False,
        on_delete=CASCADE)
    checksum_value = models.CharField(max_length=200, null=False, blank=False)
    checksum_type = models.CharField(max_length=20, choices=CHECKSUM_TYPES.items(), null=False,
                                     blank=False)

    def __unicode__(self):
        return "%s: %s (%s)" % (self.checksum_type, self.checksum_value,
                                self.data_file.name)


def _standardise_time_unit(time_float, time_unit, standard_unit, calendar):
    """
    Standardise a floating point time in one time unit by returning the
    corresponding time in the `standard_unit`. The original value is returned if
    it is already in the `standard_unit`. None is returned if the `time_float`
    is None.

    :param float time_float:
    :param str time_unit:
    :param str standard_unit:
    :returns: A floating point representation of the old time in `new_unit`
    """
    if time_float is None:
        return None

    if time_unit == standard_unit:
        return time_float

    date_time = cf_units.num2date(time_float, time_unit, calendar)
    corrected_time = cf_units.date2num(date_time, standard_unit, calendar)

    return corrected_time
