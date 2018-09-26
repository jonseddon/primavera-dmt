"""
check_debug.py

This is designed to be called by Travis. Import settings and check that
`DEBUG` has been set to `False`. Fail if it hasn't.
"""
from __future__ import absolute_import, print_function
import sys
from pdata_site import settings

def main():
    """ Main function """
    if settings.DEBUG is False:
        sys.exit(0)
    else:
        print('ERROR: DEBUG in settings.py is not False!')
        sys.exit(1)


if __name__ == '__main__':
    main()
