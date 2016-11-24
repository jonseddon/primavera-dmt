from subprocess import check_output, CalledProcessError

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
