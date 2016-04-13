import os
import commands

from apps import all_controllers

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

def os_path_join(a, b, sep="/"):
    return a + sep + b