# Hand rolled config to start with
config = {
    "CMIP6-MOHC":
        {"incoming_dir": "./test_datasets/incoming",
         "archive_dir":  "./test_datasets/archive",
         "esgf_dir":     "./test_datasets/esgf"}
}

def get_dir_from_scheme(scheme, dir_type):
    return config[scheme][dir_type]