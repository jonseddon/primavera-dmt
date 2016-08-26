"""
test_dataset.py
===============

Defines a class to identify test datasets and defines
a set of test datasets to use.
"""

# Standard library imports
import os

class DatasetForTests(object):

    INCOMING_DIR = "./test_data/incoming"

    def __init__(self, id, files, bad_file=-1, simulate_action="CANNOT PUBLISH"):
        self.id = id
        self.files = files
        self.corrupt_index = bad_file
        self.simulate_error = simulate_action

    def _get_var_id(self, fpath):
        return os.path.basename(fpath).split("_")[0]

    def create_test_files(self):
        "Creates dummy files in incoming."
        if not os.path.isdir(self.INCOMING_DIR):
            os.makedirs(self.INCOMING_DIR)

        for (indx, fname) in enumerate(self.files):

            fpath = os.path.join(self.INCOMING_DIR, fname)
            # print "Writing test file: %s" % fpath
            content = self._get_var_id(fname)
            if indx == self.corrupt_index:
                content = "RUBBISH"

            with open(fpath, 'w') as f:
                f.write(content)


# Define datasets
# d1 - 3 good files
d1 = DatasetForTests("cmip5.output1.MOHC.HadGEM2-ES.abrupt4xCO2.day.atmos.cfDay.r1i1p1.v20120114",
                     ("rsds_cfDay_HadGEM2-ES_abrupt4xCO2_r1i1p1_19910101-19911230.nc",
                      "rsds_cfDay_HadGEM2-ES_abrupt4xCO2_r1i1p1_19920101-19921230.nc",
                      "rsds_cfDay_HadGEM2-ES_abrupt4xCO2_r1i1p1_19930101-19931230.nc"))

# d2 - 2 good files, 1 bad file
d2 = DatasetForTests("cmip5.output1.IPSL.IPSL-CM5A-LR.historical.day.atmos.day.r1i1p1.v20140430",
                     ("tas_day_IPSL-CM5A-LR_historical_r1i1p1_18590101-18591230.nc",
                      "tas_day_IPSL-CM5A-LR_historical_r1i1p1_18600101-18601230.nc",
                      "tas_day_IPSL-CM5A-LR_historical_r1i1p1_18610101-18611230.nc"),
                     bad_file=1)

# d3 - 3 good files, but IOError raised during Ingest process
d3 = DatasetForTests("cmip5.output1.IPSL.IPSL-CM5A-LR.amip4K.day.atmos.day.r1i1p1.v20140430",
                     ("tasmax_day_IPSL-CM5A-LR_amip4K_r1i1p1_18590101-18591230.nc",
                      "tasmax_day_IPSL-CM5A-LR_amip4K_r1i1p1_18600101-18601230.nc",
                      "tasmax_day_IPSL-CM5A-LR_amip4K_r1i1p1_18610101-18611230.nc"),
                     simulate_action="PUBLICATION FAILS")

# d4 - 3 good files, but withdraw afterwards
d4 = DatasetForTests("cmip5.output1.MOHC.HadGEM2-ES.rcp45.day.atmos.cfDay.r1i1p1.v20120114",
                     ("rsds_cfDay_HadGEM2-ES_rcp45_r1i1p1_19910101-19911230.nc",
                      "rsds_cfDay_HadGEM2-ES_rcp45_r1i1p1_19920101-19921230.nc",
                      "rsds_cfDay_HadGEM2-ES_rcp45_r1i1p1_19930101-19931230.nc"),
                     simulate_action="WITHDRAW ON SUCCESS")

# d5 - 3 good files, but withdraw during ingest
d5 = DatasetForTests("cmip5.output1.MOHC.HadGEM2-ES.rcp45.day.atmos.cfDay.r1i1p1.v20120114",
                     ("ps_cfDay_HadGEM2-ES_rcp45_r1i1p1_19910101-19911230.nc",
                      "ps_cfDay_HadGEM2-ES_rcp45_r1i1p1_19920101-19921230.nc",
                      "ps_cfDay_HadGEM2-ES_rcp45_r1i1p1_19930101-19931230.nc"),
                     simulate_action="WITHDRAW DURING INGEST")

d6 = DatasetForTests("cmip5.output1.IPSL.IPSL-CM5A-LR.abrupt4xCO2.day.atmos.cfDay.r1i1p1.v20120114",
                     ("rsds_cfDay_IPSL-CM5A-LR_abrupt4xCO2_r1i1p1_19910101-19911230.nc",
                      "rsds_cfDay_IPSL-CM5A-LR_abrupt4xCO2_r1i1p1_19920101-19921230.nc",
                      "rsds_cfDay_IPSL-CM5A-LR_abrupt4xCO2_r1i1p1_19930101-19931230.nc"))

# d7 - Same as d3 but files provided by MOHC
d7 = DatasetForTests("cmip5.output1.MOHC.HadGEM2-ES.amip4K.day.atmos.day.r1i1p1.v20140430",
                     ("tasmax_day_HadGEM2-ES_amip4K_r1i1p1_18590101-18591230.nc",
                      "tasmax_day_HadGEM2-ES_amip4K_r1i1p1_18600101-18601230.nc",
                      "tasmax_day_HadGEM2-ES_amip4K_r1i1p1_18610101-18611230.nc"),
                     simulate_action="PUBLICATION FAILS")


class DataSubmissionForTests(DatasetForTests):

    INCOMING_DIR = "./test_data/submission"

test_data_submission = DataSubmissionForTests(None,
                        tuple(list(d3.files) + list(d5.files) + list(d6.files) + list(d7.files))
                   )
