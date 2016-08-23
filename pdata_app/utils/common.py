import commands

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
    return commands.getoutput("md5sum %s" % fpath).split()[0]

def sha256(fpath):
    return commands.getoutput("sha256sum %s" % fpath).split()[0]

def adler32(fpath):
    return commands.getoutput("adler32 %s" % fpath).split()[0]
