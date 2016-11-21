from __future__ import unicode_literals
from django.db import models

# Create your models here.

model_names = ['Variable', 'Datafile', 'VariableOccurrence',
               'VariableAttributeLink', 'Attribute', 'DatafileAttributeLink',
               'DatafileNote', 'VariableNote']


class Variable(models.Model):
    """
    Class to hold information on a single netCDF variable;
      * var_name: Char, required
      * standard_name: char, optional
      * long_name: char, optional
      * var_type: char, required
      * units: char, default = "1"

    Other attributes to go in VariableAttribute
    """
    class Meta:
        verbose_name = 'Variable'

    var_name = models.CharField(max_length=200, null=False, blank=False)
    standard_name = models.CharField(max_length=200, null=True, blank=True)
    long_name = models.CharField(max_length=200, null=True, blank=True)
    var_type = models.CharField(max_length=200, null=False, blank=False)
    units = models.CharField(max_length=200, default="1", null=True, blank=False)
    attributes_list = models.CharField(max_length=2000, null=True, blank=False)

    def __unicode__(self):
        outstr = "\n"
        for i in ['var_name', 'standard_name', 'long_name', 'var_type', 'units', 'attributes_list']:
            outstr += "   %s : %s\n" % (i, getattr(self, i))
        return outstr


class Datafile(models.Model):
    """
    Class to hold information on individual data files:
     * original_location: char, required
     * batch_id: int, optional
     * status: char, optional
     * original_owner: char, required
     * workspace: char, optional
     * date_scanned: date, auto_now_add
     * date_archived: date, optional
     * file_size: int, required
     * file_checksum: char, optional
     * file_checksum_type: char, optional
     * file_format: char, required
    """
    class Meta:
        verbose_name = 'Data File'

    original_location = models.CharField(max_length=200, null=False,
                                         blank=False, unique=True,
                                         verbose_name='Original Location')
    batch_id = models.IntegerField(null=True, blank=True,
                                   verbose_name='Batch ID')
    status = models.CharField(max_length=200, null=True, blank=True,
                              verbose_name='Status')
    original_owner = models.CharField(max_length=200, null=False, blank=False,
                                      verbose_name='Original Owner')
    workspace = models.CharField(max_length=200, null=True, blank=True,
                                 verbose_name='Workspace')
    date_scanned = models.DateTimeField(auto_now_add=True,
                                        null=False, blank=False,
                                        verbose_name='Date Scanned')
    date_archived = models.DateTimeField(null=True, blank=True,
                                         verbose_name='Date Archived')
    file_size = models.IntegerField(null=False, blank=False,
                                    verbose_name='File Size')
    file_checksum = models.CharField(max_length=50, null=True, blank=True,
                                     verbose_name='File Checksum')
    file_checksum_type = models.CharField(max_length=50, null=True, blank=True,
                                          verbose_name='File Checksum Type')
    file_format = models.CharField(max_length=40, null=False, blank=False,
                                   verbose_name='File Format')

    def __unicode__(self):
        outstr = "\n"
        for i in ['original_location', 'batch_id', 'status', 'original_owner', 'workspace', 'date_scanned', 'date_archived', 'file_size', 'file_checksum', 'file_checksum_type', 'file_format']:
            outstr += "   %s : %s\n" % (i, getattr(self, i))
        return outstr


class VariableOccurrence(models.Model):
    """
    Link a variable with a file and optionally include information about the data in the file
    * data_file: FK
    * variable: FK
    * max_val: float, optional
    * min_val: float, optional
    * shape: char, optional
    """
    class Meta:
        verbose_name = 'Variable Occurrence'

    data_file = models.ForeignKey('Datafile', verbose_name='Data File')
    variable = models.ForeignKey('Variable', verbose_name='Variable')
    max_val = models.FloatField(null=True, blank=True,
                                verbose_name='Maximum Value')
    min_val = models.FloatField(null=True, blank=True,
                                verbose_name='Minimum Value')
    shape = models.CharField(max_length=50, null=True, blank=True,
                             verbose_name='Shape')

    def __unicode__(self):
        outstr = "\n"
        for i in ['data_file', 'variable', 'max_val', 'min_val', 'shape']:
            outstr += "   %s : %s\n" % (i, getattr(self, i))
        return outstr


class Attribute(models.Model):
    """
    Attribute information
    * attribute_name: char, required
    * attribute_type: char, required
    * attribute_value: char, required
    """
    class Meta:
        verbose_name = 'Attribute'

    attribute_name = models.CharField(max_length=200, null=False, blank=False)
    attribute_type = models.CharField(max_length=200, null=False, blank=False)
    attribute_value = models.CharField(max_length=200, null=False, blank=False)

    def __unicode__(self):
        outstr = "\n"
        for i in ['attribute_name', 'attribute_type', 'attribute_value']:
            outstr += "   %s : %s\n" % (i, getattr(self, i))
        return outstr


class VariableAttributeLink(models.Model):
    """
    Link a variable with an attribute
    * variable: FK
    * attribute: FK
    """
    class Meta:
        verbose_name = 'Variable-Attribute Link'

    var_occurrence = models.ForeignKey('VariableOccurrence')
    attribute = models.ForeignKey('Attribute')

    def __unicode__(self):
        outstr = "\n"
        for i in ['variable', 'attribute']:
            outstr += "   %s : %s\n" % (i, getattr(self, i))
        return outstr


class DatafileAttributeLink(models.Model):
    """
    Link a data file with an attribute (for global attributes)
    * data_file: FK
    * attribute: FK
    """
    class Meta:
        verbose_name = 'Data File-Attribute Link'

    data_file = models.ForeignKey('Datafile')
    attribute = models.ForeignKey('Attribute')

    def __unicode__(self):
        outstr = "\n"
        for i in ['data_file', 'attribute']:
            outstr += "   %s : %s\n" % (i, getattr(self, i))
        return outstr


class DatafileNote(models.Model):
    """
    A class to annotate data files, e.g. "corrupted"
    * data_file: FK
    * data_file_note: char, required
    * note_author: char, required
    * note_datetime: datetime, auto_now_add
    """
    class Meta:
        verbose_name = 'Data File Note'

    data_file = models.ForeignKey('Datafile')
    data_file_note = models.CharField(max_length=2000, null=False, blank=False)
    note_author = models.CharField(max_length=50, null=False, blank=False)
    note_datetime = models.DateTimeField(auto_now_add=True, null=False, blank=False)

    def __unicode__(self):
        outstr = "\n"
        for i in ['data_file', 'data_file_note', 'note_author', 'note_datetime']:
            outstr += "   %s : %s\n" % (i, getattr(self, i))
        return outstr


class VariableNote(models.Model):
    """
    A class to annotate variables
    * variable: FK
    * variable_note: char, required
    * note_author: char, required
    * note_datetime: datetime, auto_now_add
    """
    class Meta:
        verbose_name = 'Variable Note'

    variable = models.ForeignKey('Variable')
    variable_note = models.CharField(max_length=2000, null=False, blank=False)
    note_datetime = models.DateTimeField(auto_now_add=True, null=False, blank=False)
    note_author = models.CharField(max_length=50, null=False, blank=False)

    def __unicode__(self):
        outstr = "\n"
        for i in ['variable', 'variable_note', 'note_author', 'note_datetime']:
            outstr += "   %s : %s\n" % (i, getattr(self, i))
        return outstr
