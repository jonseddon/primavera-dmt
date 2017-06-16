import sys
import os
from pwd import getpwuid
sys.path.append('/usr/local/sci/lib/python2.7/site-packages/')
sys.path.append('/usr/lib/python2.7/site-packages/')
import netCDF4
import numpy as np
import django
django.setup()
from et_indexer.models import *

# The following are variable attributes which are included
# in the variable entry. All others are dealt with as attributes.
VAR_ATTS = ['standard_name', 'long_name', 'units']


def _typestr(x):
    '''
    return a text string describing the type of the object supplied. allow for numpy and intrinsic variable types
    '''
    if isinstance(x, np.number):
        t = str(x.dtype)
        return t
    else:
        t = str(type(x))
        return t.split("'")[1]


class NoDatafileError(Exception):
    def __init__(self, filename):
        self.filename = filename

    def __str__(self):
        return "No entry in Datafile for %s." % self.filename


def FileSizeError(Exception):
    def __init__(self, et_size, db_size):
        self.et_size = et_size
        self.db_size = db_size

    def __str__(self):
        return "File sizes differ:\n\tET: %s\n\tDB: %s" % (self.et_size, self.db_size)


class NetCDFIndexedFile(object):
    def __init__(self, filename):
        '''
        Build an object from the file name without processing it.
        Check that the true path is supplied otherwise raise exception
        Arguments:
          * filename:
            file to process/represent
        '''
        realfname = os.path.realpath(filename)
        if filename != realfname:
            raise Exception("The true path of the file must be supplied. Please resolve links: (%s -> %s)" % (filename, realfname))

        self.filename = filename
        self.nf = None
        self.datafile = None
        self.datafile_attribute_links = []
        self.datafile_variable_links = []

    def process(self):
        '''
        Build the entries in the database for the file
        '''
        self.nf = netCDF4.Dataset(self.filename)

        self._build_datafile_entry()
        self._build_global_attr()
        self._build_variables()

    def retrieve(self):
        '''
        Attempt to retrieve the entry from the database otherwise raise exception
        '''
        data_files = Datafile.objects.filter(original_location=self.filename)

        if len(data_files) < 1:
            raise NoDatafileError(self.filename)
        elif len(data_files) > 1:
            raise Exception("Multiple entries in Datafile for file %s:\n%s" % (
                self.filename, data_files))

        self.datafile = data_files[0]
        self.datafile_attribute_links = DatafileAttributeLink.objects.filter(data_file=self.datafile)
        self.datafile_variable_links = VariableOccurrence.objects.filter(data_file=self.datafile)

    def _build_datafile_entry(self):
        '''
        Given a file name and the netCDF4 Dataset object build the entry for the
        Datafile table and return the output from the get_or_create call
        (object, isnew)
        '''
        f = self.filename
        f_owner = getpwuid(os.stat(f).st_uid).pw_name

        file_entry = {'original_location': f,
                      'file_size': os.path.getsize(f),
                      'original_owner': f_owner,
                      'file_format': self.nf.file_format, }

        df, isnew = Datafile.objects.get_or_create(**file_entry)

        self.datafile = df

    def _build_global_attr(self):
        '''
        Build entries in the Attributes table for each global attribute, and
        link them to the datafile using the DatafileAttributeLink table.
        '''

        for att in self.nf.ncattrs():
            v = self.nf.getncattr(att)

            if isinstance(v, basestring):
                attribute_entry = {'attribute_name': att,
                                   'attribute_type': _typestr(v),
                                   'attribute_value': v.encode('utf-8'), }
            else:
                attribute_entry = {'attribute_name': att,
                                   'attribute_type': _typestr(v),
                                   'attribute_value': str(v), }
            attribute, isnew = Attribute.objects.get_or_create(**attribute_entry)

            file_attribute_link = {'data_file': self.datafile,
                                   'attribute': attribute, }
            df_att_link, isnew = DatafileAttributeLink.objects.get_or_create(
                **file_attribute_link)
            self.datafile_attribute_links.append(df_att_link)

    def _build_variables(self):
        '''
        Build the required Variable, Attribute and VariableAttributeLink entries.
        '''

        def _build_variable_attributes(ncvar, var_occurrence, **kwargs):
            '''
            Given a netCDF4 variable and the VariableOccurrence instance create
            all the entries in the attribute and VariableAttributeLink tables
            '''
            variable_attributes = []
            variable_attribute_links = []
            atts = ncvar.ncattrs()
            for a in atts:
                if a in VAR_ATTS:
                    continue
                attr_entry = {'attribute_name': a,
                              'attribute_type': _typestr(ncvar.getncattr(a)),
                              'attribute_value': ncvar.getncattr(a), }
                attribute, isnew = Attribute.objects.get_or_create(**attr_entry)
                variable_attributes.append(attribute)
                link, isnew = VariableAttributeLink.objects.get_or_create(
                    var_occurrence=var_occurrence, attribute=attribute)
                variable_attribute_links.append(link)
            return variable_attributes, variable_attribute_links

        for k, v in self.nf.variables.items():
            atts = v.ncattrs()

            # build the variable
            variable_entry = {'var_name': k, }

            for a in VAR_ATTS:
                if a in v.ncattrs():
                    variable_entry.update({a: v.getncattr(a)})

            variable_entry['var_type'] = str(v.dtype)

            attributes_list = [i for i in atts if i not in VAR_ATTS]
            variable_entry['attributes_list'] = " ".join(attributes_list)

            variable, isnew = Variable.objects.get_or_create(**variable_entry)

            # build information for the link between the variable and the data file
            shapestr = ", ".join(["%s:%s" % (d, s) for d, s in zip(v.dimensions, v.shape)])
            datafile_variable_link_entry = {'variable': variable,
                                            'data_file': self.datafile,
                                            'shape': shapestr, }

            var_occur, isnew = VariableOccurrence.objects.get_or_create(
                **datafile_variable_link_entry)
            self.datafile_variable_links.append(var_occur)

            # build the VariableOccurrece attributes
            var_atts, var_att_links = _build_variable_attributes(v, var_occur)

    def destroy(self):

        for dvl in self.datafile_variable_links:
            dvl.delete()

        for attl in self.datafile_attribute_links:
            attl.delete()

        self.datafile.delete()

    def checksum(self, checksummer=None, overwrite=False, quiet=False):
        '''
        Given a filename and the path to a check summing tool update the
        file_checksum elements of the Datafile table.
        '''

        if self.datafile.file_checksum is not None:
            if overwrite:
                if not quiet:
                    print "Overwriting checksum for file %s" % self.filename
                    print "\toriginal value:", self.datafile.file_checksum
            else:
                raise Exception("File %s already has checksum %s" % (self.filename, self.datafile.file_checksum))

        import subprocess

        cmd = [checksummer, self.filename]
        output = subprocess.check_output(cmd)

        chksum, filename = output.split()

        self.datafile.file_checksum = chksum
        if not quiet:
            print "\t     new value:", self.datafile.file_checksum

        self.datafile.file_checksum_type = checksummer.split("/")[-1]
        self.datafile.save()

    def calculate_variable_summary(self):
        '''
        populate the max and min value entries in the
        datafile_variable_links table
        '''
        if not self.nf:
            self.nf = netCDF4.Dataset(self.filename)

        for df_v_link in self.datafile_variable_links:
            var_name = df_v_link.variable.var_name
            var = self.nf.variables[var_name]
            df_v_link.max_val = var[...].max()
            df_v_link.min_val = var[...].min()
            df_v_link.save()

    def update_from_et(self, batch_id=None, file_size=None, state=None, date_archived=None, workspace=None, overwrite=False):
        '''
        Update the Datafile entry with information extracted from the Elastic table system
        Keywords:
         * batch_id,file_size, state, date_archived:
           information extracted from ET html reports
         * workspace:
           workspace the data is archived under
         * overwrite:
           overwrite the file_size (USE WITH CARE)

        a list of messages describing the changes made is returned
        '''
        def _make_change(val, name):
            df_val = getattr(self.datafile, name)
            msg = None
            if df_val != val:
                filename = self.datafile.original_location
                msg = "File %s, %s: %s -> %s" % (filename, name, df_val, val)
                setattr(self.datafile, name, val)
            return [msg]

        messages = []
        messages += _make_change(batch_id, "batch_id")

        if self.datafile.file_size != file_size:
            if not overwrite:
                raise FileSizeError(file_size, self.datafile.file_size)
            else:
                messages += _make_change(file_size, "file_size")

        messages += _make_change(state, "status")
        messages += _make_change(date_archived, "date_archived")
        messages += _make_change(workspace, "workspace")

        self.datafile.save()

        return [m for m in messages if m is not None]

if __name__ == '__main__':
    pass
