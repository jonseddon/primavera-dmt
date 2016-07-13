#!/usr/bin/env python

"""
Usage:

    create_chain.py <chain_name> <stage_1> [<stage_2> ... <stage_n>]
"""
import os
import sys

import django

os.environ["DJANGO_SETTINGS_MODULE"] = "pdata_site.settings"
django.setup()

from pdata_app.utils.dbapi import create_chain
from pdata_app.utils.common import get_controller_class


def make_chain(name, *steps):
    controllers = []

    for controller_name in steps:
        try:
            controller = get_controller_class(controller_name)
        except:
            raise Exception("Cannot find controller with name: '%s'" % controller_name)

        controllers.append(controller)

    chain = create_chain(name, controllers)
    print "Created chain: %s" % chain.name


if __name__ == "__main__":
    args = [arg.lower() for arg in sys.argv[1:]]
    make_chain(*args)
