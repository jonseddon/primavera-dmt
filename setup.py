# BSD Licence
# Copyright (c) 2010, Science & Technology Facilities Council (STFC)
# All rights reserved.
#
# See the LICENSE file in the source distribution of this software for
# the full license text.

from setuptools import setup, find_packages
import sys, os

version = '0.1.0'

setup(name='crepe_site',
      version=version,
      description='CEDA Dataset Pipeline App',
      url='https://github.com/cedadev/crepe',
      packages=find_packages(),
      include_package_data=True,
      )
