"""
common.py

Common code used in several tests
"""
import django
django.setup()

from django.contrib.auth.models import User

from pdata_app import models
from pdata_app.utils import dbapi
from vocabs.vocabs import STATUS_VALUES, FREQUENCY_VALUES, VARIABLE_TYPES


def make_example_files(parent_obj):
    """
    Create some common test data. Attach the items that tests need to refer-to
    to the parent object. Other items will just exist in the database.

    :param parent_obj: the parent object
    """
    project = dbapi.get_or_create(models.Project, short_name='t',
                                  full_name='test')
    clim_mod = dbapi.get_or_create(models.ClimateModel, short_name='t',
                                   full_name='test')
    clim_mod_2 = dbapi.get_or_create(models.ClimateModel,
                                     short_name='better-model',
                                     full_name='better-model')
    institute = dbapi.get_or_create(models.Institute, short_name='MOHC',
                                    full_name='Met Office Hadley Centre')
    expt = dbapi.get_or_create(models.Experiment, short_name='t',
                               full_name='test')
    vble1 = dbapi.get_or_create(models.VariableRequest,
                                table_name='Amon',
                                long_name='very descriptive', units='1',
                                var_name='var1',
                                standard_name='var_name',
                                cell_methods='time:mean',
                                positive='optimistic',
                                variable_type=VARIABLE_TYPES['real'],
                                dimensions='massive', cmor_name='var1',
                                modeling_realm='atmos',
                                frequency=FREQUENCY_VALUES['ann'],
                                cell_measures='', uid='123abc')
    vble2 = dbapi.get_or_create(models.VariableRequest,
                                table_name='Amon',
                                long_name='very descriptive', units='1',
                                var_name='var2',
                                standard_name='var_name',
                                cell_methods='time:mean',
                                positive='optimistic',
                                variable_type=VARIABLE_TYPES['real'],
                                dimensions='massive', cmor_name='var2',
                                modeling_realm='atmos',
                                frequency=FREQUENCY_VALUES['ann'],
                                cell_measures='', uid='123abc')
    parent_obj.dreq1 = dbapi.get_or_create(models.DataRequest, project=project,
                                           institute=institute,
                                           climate_model=clim_mod, experiment=expt,
                                           variable_request=vble1,
                                           rip_code='r1i1p1f1',
                                           request_start_time=0.0,
                                           request_end_time=23400.0,
                                           time_units='days since 1950-01-01',
                                           calendar='360_day')
    parent_obj.dreq2 = dbapi.get_or_create(models.DataRequest, project=project,
                                           institute=institute,
                                           climate_model=clim_mod, experiment=expt,
                                           variable_request=vble2,
                                           rip_code='r1i1p1f1',
                                           request_start_time=0.0,
                                           request_end_time=23400.0,
                                           time_units='days since 1950-01-01',
                                           calendar='360_day')
    parent_obj.dreq3 = dbapi.get_or_create(models.DataRequest, project=project,
                                           institute=institute,
                                           climate_model=clim_mod_2,
                                           experiment=expt,
                                           variable_request=vble1,
                                           rip_code='r1i1p1f1',
                                           request_start_time=0.0,
                                           request_end_time=23400.0,
                                           time_units='days since 1950-01-01',
                                           calendar='360_day')
    parent_obj.dreq4 = dbapi.get_or_create(models.DataRequest, project=project,
                                           institute=institute,
                                           climate_model=clim_mod, experiment=expt,
                                           variable_request=vble1,
                                           rip_code='r1i2p3f4',
                                           request_start_time=0.0,
                                           request_end_time=23400.0,
                                           time_units='days since 1950-01-01',
                                           calendar='360_day')
    parent_obj.esgf_dataset = dbapi.get_or_create(models.ESGFDataset,
                                                  status='PUBLISHED',
                                                  version='v12345678',
                                                  data_request=parent_obj.dreq1)
    parent_obj.user = dbapi.get_or_create(User, username='fred')
    act_id = dbapi.get_or_create(models.ActivityId,
                                 short_name='HighResMIP',
                                 full_name='High Resolution Model Intercomparison Project')
    dsub = dbapi.get_or_create(models.DataSubmission,
                               status=STATUS_VALUES['EXPECTED'],
                               incoming_directory='/some/dir',
                               directory='/some/dir', user=parent_obj.user)
    parent_obj.data_file1 = dbapi.get_or_create(models.DataFile, name='test1',
                                     incoming_name='test1',
                                     incoming_directory='/some/dir',
                                     directory='/some/dir1', size=1,
                                     project=project,
                                     climate_model=clim_mod,
                                     institute=institute,
                                     experiment=expt,
                                     variable_request=vble1,
                                     data_request=parent_obj.dreq1,
                                     activity_id=act_id, frequency='ann',
                                     rip_code='r1i1p1', grid='gn',
                                     data_submission=dsub, online=True,
                                     start_time=0,  # 1950-01-01 00:00:00
                                     end_time=3600,  # 1960-01-01 00:00:00
                                     calendar='360_day',
                                     time_units='days since 1950-01-01',
                                     version='v12345678')
    data_file4 = dbapi.get_or_create(models.DataFile, name='test4',
                                     incoming_name='test4',
                                     incoming_directory='/some/dir',
                                     directory='/some/dir2', size=4,
                                     project=project,
                                     climate_model=clim_mod,
                                     institute=institute,
                                     experiment=expt,
                                     variable_request=vble1,
                                     data_request=parent_obj.dreq1,
                                     activity_id=act_id, frequency='ann',
                                     rip_code='r1i1p1',
                                     data_submission=dsub, online=False,
                                     start_time=7200,  # 1970-01-01 00:00:00
                                     end_time=10800,  # 1980-01-01 00:00:00
                                     calendar='360_day',
                                     time_units='days since 1950-01-01')
    data_file8 = dbapi.get_or_create(models.DataFile, name='test8',
                                     incoming_name='test8',
                                     incoming_directory='/some/dir',
                                     directory='/some/dir2', size=8,
                                     project=project,
                                     climate_model=clim_mod,
                                     institute=institute,
                                     experiment=expt,
                                     variable_request=vble1,
                                     data_request=parent_obj.dreq1,
                                     activity_id=act_id, frequency='ann',
                                     rip_code='r1i1p1',
                                     data_submission=dsub, online=False,
                                     start_time=10800,  # 1980-01-01 00:00:00
                                     end_time=14400,  # 1990-01-01 00:00:00
                                     calendar='360_day',
                                     time_units='days since 1950-01-01')
    data_file2 = dbapi.get_or_create(models.DataFile, name='test2',
                                     incoming_name='test2',
                                     incoming_directory='/some/dir',
                                     directory='/some/dir', size=2,
                                     project=project,
                                     climate_model=clim_mod,
                                     institute=institute,
                                     experiment=expt,
                                     variable_request=vble2,
                                     data_request=parent_obj.dreq2,
                                     activity_id=act_id, frequency='ann',
                                     rip_code='r1i1p1',
                                     data_submission=dsub, online=True)
