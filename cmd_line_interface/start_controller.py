#!/usr/bin/env python

import os
import sys

import django

os.environ["DJANGO_SETTINGS_MODULE"] = "crepe_site.settings"
django.setup()

from crepe_app.utils.common import get_controller_class


def run_controller(name):
    try:
        controller = get_controller_class(controller_name)()
    except:
        raise Exception("Cannot find controller with name: '%s'" % name)

    controller.start()

if __name__ == "__main__":

    controller_name = sys.argv[1].lower()
    run_controller(controller_name)
