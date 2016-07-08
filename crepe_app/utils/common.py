import os
import commands

from apps import all_controllers
from crepe_app.models import model_names
from crepe_app.models import *

def get_controller_class(name):
    mod_path, mod_name = [path.split(".") for path in all_controllers if path.startswith(name)][0]
    path = "apps.%s" % mod_path
    mod = __import__(path, fromlist=[''])
    cls = getattr(mod, mod_name)
    return cls

def get_missing_file_list(dr, files):
    "Return a list of files from list files that are NOT in directory dr."
    fpaths = [os.path.join(dr, f.name) for f in files]
    return [os.path.basename(fpath) for fpath in fpaths if not os.path.isfile(fpath)]

def md5(fpath):
    return commands.getoutput("md5sum %s" % fpath).split()[0]

def get_class_by_name(model_name):
    try:
        matched_model = [name for name in model_names if name.lower() == model_name.lower()][0]
        model = eval(matched_model)
        assert hasattr(model, "objects")
    except:
        raise Exception("Model '%s' not recognised." % model_name)

    return model


def os_path_join(a, b, sep="/"):
    return a + sep + b

def as_bool(item, nofail=False):
    """
    Interpret ``item`` as a boolean value and return that. If cannot do so then raise
    an exception unless ``nofail`` is set to True; in which case return Nones.

    :param item: any type that the string representation of can be "true" or "false"
                 case-insensitive.
    :param nofail: boolean to instruct whether to raise an Exception if cannot convert
                   to a boolean.
    :return: True or False
    """
    if str(item).lower() in ("true", "false"):
        return eval(str(item).title())

    if nofail: return None

    raise Exception("Cannot convert '%s' to boolean type." % str(item))