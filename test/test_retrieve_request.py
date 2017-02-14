"""
test_validate_data_submission.py - unit tests for validate_data_submission.py
"""
import mock

import django
django.setup()

from django.test import TestCase

from pdata_app.utils.dbapi import get_or_create
from pdata_app.models import (Project, Institute, ClimateModel, ActivityId,
                              Experiment, VariableRequest, DataRequest,
                              RetrievalRequest, DataFile, DataSubmission)
from vocabs.vocabs import (CALENDARS, FREQUENCY_VALUES, STATUS_VALUES,
                           VARIABLE_TYPES)

from scripts.retrieve_request import _check_same_gws, main


class TestCheckSameGws(TestCase):
    def test_same(self):
        path1 = '/group_workspaces/jasmin2/primavera1/some/dir'
        path2 = '/group_workspaces/jasmin2/primavera1/another/dir'

        self.assertTrue(_check_same_gws(path1, path2))

    def test_diff(self):
        path1 = '/group_workspaces/jasmin2/primavera1/some/dir'
        path2 = '/group_workspaces/jasmin2/primavera2/some/dir'

        self.assertFalse(_check_same_gws(path1, path2))

    def test_bad_path(self):
        path1 = 'primavera1/some/dir'
        path2 = '/group_workspaces/jasmin2/primavera2/some/dir'

        self.assertRaisesRegexp(RuntimeError, 'Cannot determine group '
            'workspace name from primavera1/some/dir', _check_same_gws,
            path1, path2)

    def test_slightly_bad_path(self):
        path1 = '/group_workspaces/jasmin2/primavera2/some/dir'
        path2 = '/group_workspaces/jasmin1/primavera1/some/dir'

        self.assertRaisesRegexp(RuntimeError, 'Cannot determine group '
            'workspace name from /group_workspaces/jasmin1/primavera1/some/dir',
            _check_same_gws, path1, path2)


class TestIntegrationTests(TestCase):
    """Integration tests run through the unittest framework and mock"""
    def setUp(self):
        # mock any external calls
        patch = mock.patch('scripts.retrieve_request._run_command')
        self.mock_run_cmd = patch.start()
        self.addCleanup(patch.stop)

        patch = mock.patch('scripts.retrieve_request.os.link')
        self.mock_link = patch.start()
        self.addCleanup(patch.stop)

        patch = mock.patch('scripts.retrieve_request.os.mkdir')
        self.mock_mkdir = patch.start()
        self.addCleanup(patch.stop)

        patch = mock.patch('scripts.retrieve_request.os.path.exists')
        self.mock_exists = patch.start()
        self.addCleanup(patch.stop)

        patch = mock.patch('scripts.retrieve_request.shutil.copyfile')
        self.mock_copyfile = patch.start()
        self.addCleanup(patch.stop)

        # create the necessary DB objects
        proj = get_or_create(Project, short_name="CMIP6", full_name="6th "
            "Coupled Model Intercomparison Project")
        climate_model = get_or_create(ClimateModel, short_name="MY-MODEL",
            full_name="Really good model")
        institute = get_or_create(Institute, short_name='MOHC',
            full_name='Met Office Hadley Centre')
        act_id = get_or_create(ActivityId, short_name='HighResMIP',
            full_name='High Resolution Model Intercomparison Project')
        experiment = get_or_create(Experiment, short_name="experiment",
            full_name="Really good experiment")
        var = get_or_create(VariableRequest, table_name='my-table',
            long_name='very descriptive', units='1', var_name='my-var',
            standard_name='var-name', cell_methods='time: mean',
            positive='optimistic', variable_type=VARIABLE_TYPES['real'],
            dimensions='massive', cmor_name='my-var', modeling_realm='atmos',
            frequency=FREQUENCY_VALUES['ann'], cell_measures='', uid='123abc')
        dreq = get_or_create(DataRequest, project=proj,
            institute=institute, climate_model=climate_model,
            experiment=experiment, variable_request=var, rip_code='r1i1p1f1',
            request_start_time=0.0, request_end_time=23400.0,
            time_units='days since 1950-01-01', calendar='360_day')
        incoming_directory = '/gws/MOHC/MY-MODEL/incoming/v12345678'
        dsub = get_or_create(DataSubmission, status=STATUS_VALUES['VALIDATED'],
            incoming_directory=incoming_directory,
            directory=incoming_directory,
            user='fred')
        df1 = get_or_create(
            DataFile,
            name='file_one.nc',
            incoming_directory=incoming_directory,
            directory=None,
            size=1,
            project=proj,
            climate_model=climate_model,
            experiment=experiment,
            institute=institute,
            variable_request=var, data_request=dreq,
            frequency=FREQUENCY_VALUES['ann'],
            activity_id=act_id,
            rip_code='r1i1p1f1',
            online=False,
            start_time=0.,
            end_time=359.,
            time_units='days since 1950-01-01',
            calendar=CALENDARS['360_day'],
            version='v12345678', tape_url='et:1234',
            data_submission=dsub)
        self.ret_req = get_or_create(RetrievalRequest, requester='bob')
        self.ret_req.data_request.add(dreq)
        self.ret_req.save()

    def test_simplest(self):
        class ArgparseNamespace(object):
            retrieval_id = 1

        ns = ArgparseNamespace()

        # main(ns)


