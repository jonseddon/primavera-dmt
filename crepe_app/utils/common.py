from apps import all_controllers

def get_controller_class(name):
    mod_path, mod_name = [path.split(".") for path in all_controllers if path.startswith(name)][0]
    path = "apps.%s" % mod_path
    mod = __import__(path, fromlist=[''])
    cls = getattr(mod, mod_name)
    return cls