# BSD Licence
# Copyright (c) 2010, Science & Technology Facilities Council (STFC)
# All rights reserved.
#
# See the LICENSE file in the source distribution of this software for
# the full license text.

from setuptools import setup, find_packages
import sys, os

version = '0.1.0'

setup(name='pdata_site',
      version=version,
      description='PRIMAVERA Data Management Tool',
      url='https://github.com/agstephens/primavera-dmt',
      packages=find_packages(),
      include_package_data=True,
      package_data={'pdata_app':['templates/*.html',
                                 'templates/pdata_app/*.html']}
      )
