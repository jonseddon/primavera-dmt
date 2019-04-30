"""
common.py - several functions that are used throughout the pdata_app
"""
from __future__ import unicode_literals, division, absolute_import

import datetime
import logging
import os
import random
import re
from subprocess import check_output, CalledProcessError
from six.moves import zip_longest
from tempfile import gettempdir

from iris.time import PartialDateTime
import netcdftime
import cf_units

from django.db.models import Sum


PAUSE_FILES = {
    'et:': '/gws/nopw/j04/primavera5/.tape_pause/pause_et',
    'moose:': '/gws/nopw/j04/primavera5/.tape_pause/pause_moose',
}

logger = logging.getLogger(__name__)


def safe_strftime(dt, format):
    """
    datetime.strftime without >1900 restriction.

    Edited from http://stackoverflow.com/questions/1526170/formatting-date-string-in-python-for-dates-prior-to-1900

    """
    if dt.year >= 1900:
        return dt.strftime(format)


    assert dt.year < 1900
    factor = (1900 - dt.year - 1) // 400 + 1
    future_year = dt.year + factor * 400
    assert future_year > 1900

    # Just check the format doesn't contain markers
    assert not ('[@' in format or '@]' in format)

    if '%c' in format or '%x' in format:
        raise ValueError("'%c', '%x' produce unreliable results for year < 1900")

    mark_format = format.replace('%Y', '[@%Y@]')
    from_replace = '[@%d@]' % future_year
    to_replace = '%d' % dt.year

    result = dt.replace(year=future_year).strftime(mark_format)
    result = result.replace(from_replace, to_replace)

    return result


def md5(fpath):
    return _checksum('md5sum', fpath)


def sha256(fpath):
    return _checksum('sha256sum', fpath)


def adler32(fpath):
    return _checksum('adler32', fpath)


def _checksum(checksum_method, file_path):
    """
    Runs program `checksum_method` on `file_path` and returns the result or
    None if running the program was unsuccessful.

    :param str command:
    :param str file_path:
    :return: the checksum or None if it cannot be calculated
    """
    try:
        # shell=True is not a security risk here. The input has previously been
        # checked and this is only called if file_path has been confirmed as
        # being a valid file
        ret_val = check_output("{} '{}'".format(checksum_method, file_path),
                                shell=True).decode('utf-8')
        # split on white space and return the first part
        checksum = ret_val.split()[0]
    except (CalledProcessError, OSError):
        checksum = None

    return checksum


def make_partial_date_time(date_string):
    """
    Convert the fields in `date_string` into a PartialDateTime object. Formats
    that are known about are:

    YYYMM
    YYYYMMDD

    :param str date_string: The date string to process
    :returns: An Iris PartialDateTime object containing as much information as
        could be deduced from date_string
    :rtype: iris.time.PartialDateTime
    :raises ValueError: If the string is not in a known format.
    """
    if len(date_string) == 6:
        pdt_str = PartialDateTime(year=int(date_string[0:4]),
            month=int(date_string[4:6]))
    elif len(date_string) == 8:
        pdt_str = PartialDateTime(year=int(date_string[0:4]),
            month=int(date_string[4:6]), day=int(date_string[6:8]))
    else:
        raise ValueError('Unknown date string format')

    return pdt_str


def calc_last_day_in_month(year, month, calendar):
    """
    Calculate the last day of the specified month using the calendar given.

    :param str year: The year
    :param str month: The month
    :param str calendar: The calendar to use, which must be supported by
        cf_units
    :returns: The last day of the specified month
    :rtype: int
    """
    ref_units = 'days since 1969-07-21'

    if month == 12:
        start_next_month_obj = netcdftime.datetime(year + 1, 1, 1)
    else:
        start_next_month_obj = netcdftime.datetime(year, month + 1, 1)

    start_next_month = cf_units.date2num(start_next_month_obj, ref_units,
        calendar)

    end_this_month = cf_units.num2date(start_next_month - 1, ref_units,
        calendar)

    return end_this_month.day


def pdt2num(pdt, time_units, calendar, start_of_period=True):
    """
    Convert an Iris PartialDateTime object into a Python datetime object. If
    the day of the month is not specified in `pdt` then it is set as 1 in the
    output unless `start_of_period` isn't True, when 30 is used as the day of
    the month (because a 360 day calendar is assumed).

    :param iris.time.PartialDateTime pdt: The partial date time to convert
    :param str time_units: The units used in this time
    :param str calendar:
    :param bool start_of_period: If true and no day is specified then day is
        set to 1, otherwise day is set to 30.
    :returns: A float representation of `pdt` relative to `time_units`
    :raises ValueError: if the PartialDateTime object does not have all of the
        required attributes set.
    """
    datetime_attrs = {}

    compulsory_attrs = ['year', 'month']

    for attr in compulsory_attrs:
        attr_value = getattr(pdt, attr)
        if attr_value:
            datetime_attrs[attr] = attr_value
        else:
            msg = '{} must be defined in: {}'.format(attr, pdt)
            raise ValueError(msg)

    if pdt.day:
        datetime_attrs['day'] = pdt.day
    else:
        if start_of_period:
            datetime_attrs['day'] = 1
        else:
            datetime_attrs['day'] = calc_last_day_in_month(pdt.year, pdt.month,
                calendar)

    optional_attrs = ['hour', 'minute', 'second', 'microsecond']
    for attr in optional_attrs:
        attr_value = getattr(pdt, attr)
        if attr_value:
            datetime_attrs[attr] = attr_value

    dt_obj = netcdftime.datetime(**datetime_attrs)

    return cf_units.date2num(dt_obj, time_units, calendar)


def list_files(directory, suffix='.nc'):
    """
    Return a list of all the files with the specified suffix in the submission
    directory structure and sub-directories.

    :param str directory: The root directory of the submission
    :param str suffix: The suffix of the files of interest
    :returns: A list of absolute filepaths
    """
    nc_files = []

    dir_files = os.listdir(directory)
    for filename in dir_files:
        file_path = os.path.join(directory, filename)
        if os.path.isdir(file_path):
            nc_files.extend(list_files(file_path, suffix))
        elif file_path.endswith(suffix):
            nc_files.append(file_path)

    return nc_files


def ilist_files(directory, suffix='.nc'):
    """
    Return an iterator of all the files with the specified suffix in the
    submission directory structure and sub-directories.

    :param str directory: The root directory of the submission
    :param str suffix: The suffix of the files of interest
    :returns: A list of absolute filepaths
    """
    dir_files = os.listdir(directory)
    for filename in dir_files:
        file_path = os.path.join(directory, filename)
        if os.path.isdir(file_path):
            for ifile in ilist_files(file_path):
                yield ifile
        elif file_path.endswith(suffix):
            yield file_path


def get_temp_filename(suffix):
    """
    Generates a random filename and returns this as a string. The filename is
    in the form tempXXXXX.YY where XXXXX is a random five digit integer. The
    filename is appended to the path of the system's temporary directory.

    :param str suffix: The suffix to append to the filename
    :returns: The random file path
    """
    number = random.randint(0, 99999)
    zero_padded = str(number).zfill(5)
    filename = 'temp' + zero_padded + '.' + suffix

    return os.path.join(gettempdir(), filename)


def standardise_time_unit(time_float, time_unit, standard_unit, calendar):
    """
    Standardise a floating point time in one time unit by returning the
    corresponding time in the `standard_unit`. The original value is returned if
    it is already in the `standard_unit`. None is returned if the `time_float`
    is None.

    :param float time_float: The time to change
    :param str time_unit: The original time's units
    :param str standard_unit: The new unit
    :returns: A floating point representation of the old time in
        `standard_unit`
    """
    if (time_float is None or time_unit is None or
            standard_unit is None or calendar is None):
        return None

    if time_unit == standard_unit:
        return time_float

    date_time = cf_units.num2date(time_float, time_unit, calendar)
    corrected_time = cf_units.date2num(date_time, standard_unit, calendar)

    return corrected_time


def is_same_gws(path1, path2):
    """
    Check that two paths both start with the same group workspace name.

    It's assumed that both paths are both in either the old-style or both
    in the new-style format.

    :param str path1: The first path
    :param str path2: The second path
    :returns: True if both paths are in the same group workspace
    :raises RuntimeError: if the paths aren't recognised
    """
    if path1.startswith('/group_workspaces'):
        gws_pattern = r'^/group_workspaces/jasmin2/primavera\d'
    elif path1.startswith('/gws'):
        gws_pattern = r'^/gws/nopw/j04/primavera\d'
    else:
        raise ValueError('path1 format is not a recognised group workspace '
                         'pattern: {}'.format(path1))
    gws1 = re.match(gws_pattern, path1)
    gws2 = re.match(gws_pattern, path2)

    if not gws1:
        msg = 'Cannot determine group workspace name from {}'.format(path1)
        raise RuntimeError(msg)
    if not gws2:
        msg = 'Cannot determine group workspace name from {}'.format(path2)
        raise RuntimeError(msg)

    return True if gws1.group(0) == gws2.group(0) else False


def get_gws(path):
    """
    Find the group workspace path at the start of `path`.

    :param str path:
    :returns: the path identified
    :rtype: str
    :raises RuntimeError: if the the path isn't a gws path
    """
    gws_pattern = r'^/group_workspaces/jasmin2/primavera\d/stream1'
    gws = re.match(gws_pattern, path)

    if not gws:
        msg = 'Cannot determine group workspace name from {}'.format(path)
        raise RuntimeError(msg)

    return gws.group(0)


def construct_drs_path(data_file):
    """
    Make the CMIP6 DRS directory path for the specified file.

    :param pdata_app.models.DataFile data_file: the file
    :returns: A string containing the DRS directory structure
    """
    return os.path.join(
        data_file.project.short_name,
        data_file.activity_id.short_name,
        data_file.institute.short_name,
        data_file.climate_model.short_name,
        data_file.experiment.short_name,
        data_file.rip_code,
        data_file.variable_request.table_name,
        data_file.variable_request.cmor_name,
        data_file.grid,
        data_file.version
    )


def construct_filename(data_file):
    """
    Calculate the CMIP6 filename for the specified file.

    The grid code cannot be determined using any other method and so is taken
    from the existing file name.

    :param pdata_app.models.DataFile data_file: the file
    :returns: A string containing the file's name
    :raises NotImplementedError: if the frequency isn't known
    """
    components = [
        data_file.variable_request.cmor_name,
        data_file.variable_request.table_name,
        data_file.climate_model.short_name,
        data_file.experiment.short_name,
        data_file.rip_code,
        data_file.grid,
    ]

    if data_file.frequency != 'fx':
        start_str = construct_time_string(data_file.start_time,
                                          data_file.time_units,
                                          data_file.calendar,
                                          data_file.frequency)
        end_str = construct_time_string(data_file.end_time,
                                        data_file.time_units,
                                        data_file.calendar,
                                        data_file.frequency)
        components.append(start_str + '-' + end_str)

    return '_'.join(components) + '.nc'


def construct_time_string(time_point, time_units, calendar, frequency):
    """
    Calculate the time string to the appropriate resolution for use in CMIP6
    filenames according to http://goo.gl/v1drZl

    :param float time_point: the start time
    :param str time_units: the time's units
    :param str calendar: the time's calendar
    :param str frequency: the variables' frequnecy string
    :returns: the time point
    :rtype: str
    :raises NotImplementedError: if the frequency isn't known
    """
    formats = {
        'ann': '%Y',
        'mon': '%Y%m',
        'day': '%Y%m%d',
        '6hr': '%Y%m%d%H%M',
        '3hr': '%Y%m%d%H%M',
        '1hr': '%Y%m%d%H%M',
    }

    try:
        time_fmt = formats[frequency]
    except KeyError:
        msg = 'No time format known for frequency string {}'.format(frequency)
        raise NotImplementedError(msg)

    datetime = netcdftime.num2date(time_point, time_units, calendar)

    return datetime.strftime(time_fmt)


def get_request_size(data_reqs, start_year, end_year,
                     online=False, offline=False):
    """
    Find the size in bytes of the files in the data requests between the years
    specified.

    :param Iterable data_reqs: an iterable of data requests to get the size
        of. This is typically a Django queryset or a list.
    :param int start_year: the first year of the range to find.
    :param int end_year: the final year of the range to find.
    :param bool online: show the size of only files that are currently online
    :param bool offline: show the size of only files that are currently offline
    :rtype: int
    :return: The size in bytes of the retrieval request.
    :raises ValueError: if both online and offline are set
    """
    if online and offline:
        msg = 'online and offline arguments cannot both be True'
        raise ValueError(msg)

    request_sizes = []
    for req in data_reqs:
        if online:
            all_files = req.datafile_set.filter(online=True)
        elif offline:
            all_files = req.datafile_set.filter(online=False)
        else:
            all_files = req.datafile_set.all()

        filtered_files = date_filter_files(all_files, start_year, end_year)
        if filtered_files:
            req_size = filtered_files.aggregate(Sum('size'))['size__sum']
            if req_size is None:
                req_size = 0
        else:
            req_size = 0
        request_sizes.append(req_size)

    return sum(request_sizes)


def date_filter_files(data_files, start_year, end_year):
    """
    Filter a set of data file model objects and return those that lie between
    or include the 1st January in the start year and the last day of the end
    year. The data files are assumed to be from a common source and so they all
    have the same time_units and calendar.

    :param django.db.models.query.QuerySet data_files: the data files to
        filter by date
    :param int start_year: the first year of the range to find.
    :param int end_year: the final year of the range to find.
    :returns: the filtered files
    :rtype: django.db.models.query.QuerySet
    """
    if data_files:
        time_units = data_files[0].time_units
        calendar = data_files[0].calendar
    else:
        return None

    if start_year is not None and time_units and calendar:
        start_float = cf_units.date2num(
            datetime.datetime(start_year, 1, 1), time_units,
            calendar
        )
    else:
        start_float = None
    if end_year is not None and time_units and calendar:
        end_float = cf_units.date2num(
            datetime.datetime(end_year + 1, 1, 1), time_units,
            calendar
        )
    else:
        end_float = None

    timeless_files = data_files.filter(start_time__isnull=True)

    if start_float is not None and end_float is not None:
        between_files = (data_files.exclude(start_time__isnull=True).
                         filter(start_time__gte=start_float,
                                end_time__lt=end_float))
        start_straddle = (data_files.exclude(start_time__isnull=True).
                          filter(start_time__lt=start_float,
                                 end_time__gt=start_float))
        end_straddle = (data_files.exclude(start_time__isnull=True).
                        filter(start_time__lt=end_float,
                               end_time__gt=end_float))
        data_files = between_files | start_straddle | end_straddle
    else:
        data_files = data_files.exclude(start_time__isnull=True)

    return (timeless_files | data_files).distinct()


def delete_drs_dir(directory, mip_eras=('PRIMAVERA', 'CMIP6')):
    """
    Delete the directory specified and any empty parent directories until
    one of the mip_eras values is found at the top of the DRS structure.
    It is assumed that the directory has already been confirmed as being
    empty.

    :param str directory: The directory to delete.
    :param tuple mip_eras: The possible values of mip_era at the top of the
        DRS structure. Directories above these values are not deleted.
    """
    try:
        os.rmdir(directory)
    except OSError as exc:
        logger.error('Unable to delete directory {}. {}.'.format(directory,
                                                                 exc.strerror))
        return

    parent_dir = os.path.dirname(directory)
    if (not os.listdir(parent_dir) and
            not directory.endswith(mip_eras)):
        # parent is empty so delete it
        delete_drs_dir(parent_dir)


def grouper(iterable, n):
    """
    Group an iterable into chunks of length `n` with the final chunk containing
    any remaining values and so possibly being less than length n.

    Taken from:
    https://docs.python.org/3.6/library/itertools.html#itertools-recipes

    :param Iterable iterable: the input iterable to chunk
    :param int n: return chunks of this length
    :returns: iterables of length n
    :rtype: Iterable
    """
    args = [iter(iterable)] * n
    for chunk in zip_longest(*args):
        yield filter(lambda x: x is not None, chunk)


def directories_spanned(data_req):
    """
    Find all of the directories containing files from the specified data
    request.

    :param pdata_app.models.DataRequest data_req: the data request to query
    :return: a list of dictionaries containing the directories containing data
        for the specified data request
    """
    dirs_list = []

    for data_dir in data_req.directories():
        # ignore Nones
        if data_dir:
            dfs = data_req.datafile_set.filter(directory=data_dir)
            dirs_list.append({
                'dir_name': data_dir,
                'num_files': dfs.count(),
                'dir_size': dfs.aggregate(Sum('size'))['size__sum']
            })

    dirs_list.sort(key=lambda dd: dd['dir_name'])

    return dirs_list
