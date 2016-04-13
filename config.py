# Hand rolled config to start with
config = {
    "CMIP6-MOHC":
        {"incoming_dir": "./test_data/incoming",
         "archive_dir":  "./test_data/archive",
         "esgf_dir":     "./test_data/esgf"},
    "log_level": "INFO",
}

def get_dir_from_scheme(scheme, dir_type):
    return config[scheme][dir_type]