"""
common.py - several functions that are used throughout the pdata_app
"""
import os
import random
from subprocess import check_output, CalledProcessError
from tempfile import gettempdir

from iris.time import PartialDateTime
import cf_units


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
        ret_val = check_output('{} {}'.format(checksum_method, file_path),
                                shell=True)
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
        start_next_month_obj = cf_units.netcdftime.datetime(year + 1, 1, 1)
    else:
        start_next_month_obj = cf_units.netcdftime.datetime(year, month + 1, 1)

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

    dt_obj = cf_units.netcdftime.datetime(**datetime_attrs)

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
            nc_files.extend(list_files(file_path))
        elif file_path.endswith(suffix):
            nc_files.append(file_path)

    return nc_files


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
