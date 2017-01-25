"""
test_validate_data_submission.py - unit tests for validate_data_submission.py
"""
from django.test import TestCase
import mock

import iris
from iris.tests.stock import realistic_3d
from iris.time import PartialDateTime

from scripts.validate_data_submission import (
    _check_start_end_times, _check_contiguity, _check_data_point,
    identify_filename_metadata, FileValidationError, update_database_submission)
from pdata_app.models import DataSubmission
from pdata_app.utils.dbapi import get_or_create


class TestIdentifyFilenameMetadata(TestCase):
    @mock.patch('scripts.validate_data_submission.os.path.getsize')
    def setUp(self, mock_getsize):
        mock_getsize.return_value = 1234

        filename = 'clt_Amon_Monty_historical_r1i1p1_185912-188411.nc'

        self.metadata = identify_filename_metadata(filename,
                                                   file_format='CMIP5')

    def test_cmor_name(self):
        self.assertEqual(self.metadata['cmor_name'], 'clt')

    def test_table(self):
        self.assertEqual(self.metadata['table'], 'Amon')

    def test_climate_model(self):
        self.assertEqual(self.metadata['climate_model'], 'Monty')

    def test_experiment(self):
        self.assertEqual(self.metadata['experiment'], 'historical')

    def test_rip_code(self):
        self.assertEqual(self.metadata['rip_code'], 'r1i1p1')

    def test_start_date(self):
        self.assertEqual(self.metadata['start_date'],
            PartialDateTime(year=1859, month=12))

    def test_end_date(self):
        self.assertEqual(self.metadata['end_date'],
            PartialDateTime(year=1884, month=11))

    @mock.patch('scripts.validate_data_submission.logger')
    def test_bad_date_format(self, mock_logger):
        filename = 'clt_Amon_Monty_historical_r1i1p1_1859-1884.nc'
        self.assertRaises(FileValidationError,
            identify_filename_metadata, filename, file_format='CMIP5')


class TestUpdateDatabaseSubmission(TestCase):
    def setUp(self):
        self.ds1 = get_or_create(DataSubmission, incoming_directory='/dir1',
            directory='/dir1', user='primavera')
        self.ds2 = get_or_create(DataSubmission, incoming_directory='/dir1',
            directory='/dir1', user='someone')
        self.ds3 = get_or_create(DataSubmission, incoming_directory='/dir2',
            directory='/dir2', user='primavera')

    def test_unique_submission(self):
        update_database_submission({}, self.ds3)
        self.ds3.refresh_from_db()
        self.assertEqual(self.ds3.status, 'VALIDATED')


class TestCheckStartEndTimes(TestCase):
    def setUp(self):
        self.cube = realistic_3d()

        self.metadata_1 = {'basename': 'file.nc',
            'start_date': PartialDateTime(year=2014, month=12),
            'end_date': PartialDateTime(year=2014, month=12)}
        self.metadata_2 = {'basename': 'file.nc',
            'start_date': PartialDateTime(year=2014, month=11),
            'end_date': PartialDateTime(year=2014, month=12)}
        self.metadata_3 = {'basename': 'file.nc',
            'start_date': PartialDateTime(year=2013, month=12),
            'end_date': PartialDateTime(year=2014, month=12)}
        self.metadata_4 = {'basename': 'file.nc',
            'start_date': PartialDateTime(year=2014, month=12),
            'end_date': PartialDateTime(year=2015, month=9)}

        # mock logger to prevent it displaying messages on screen
        patch = mock.patch('scripts.validate_data_submission.logger')
        self.mock_logger = patch.start()
        self.addCleanup(patch.stop)

    def test_equals(self):
        self.assertTrue(_check_start_end_times(self.cube, self.metadata_1))

    def test_fails_start_month(self):
        self.assertRaises(FileValidationError, _check_start_end_times,
            self.cube, self.metadata_2)

    def test_fails_start_year(self):
        self.assertRaises(FileValidationError, _check_start_end_times,
            self.cube, self.metadata_3)

    def test_fails_end(self):
        self.assertRaises(FileValidationError, _check_start_end_times,
            self.cube, self.metadata_4)


class TestCheckContiguity(TestCase):
    def setUp(self):
        # mock logger to prevent it displaying messages on screen
        patch = mock.patch('scripts.validate_data_submission.logger')
        self.mock_logger = patch.start()
        self.addCleanup(patch.stop)

        self.good_cube = realistic_3d()
        self.good_cube.coord('time').guess_bounds()

        cubes = iris.cube.CubeList([self.good_cube[:2], self.good_cube[4:]])
        self.bad_cube = cubes.concatenate_cube()

    def test_contiguous(self):
        self.assertTrue(
            _check_contiguity(self.good_cube, {'basename': 'file.nc'}))

    def test_not_contiguous(self):
        self.assertRaises(FileValidationError, _check_contiguity,
            self.bad_cube, {'basename': 'file.nc'})


class TestCheckDataPoint(TestCase):
    def test_todo(self):
        # TODO: figure put how to test this function
        pass
