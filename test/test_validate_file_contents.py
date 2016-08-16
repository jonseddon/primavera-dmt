"""
test_validate_data_submission.py - unit tests for validate_data_submission.py
"""
import datetime
from django.test import TestCase
import mock

import iris
from iris.tests.stock import realistic_3d
from iris.time import PartialDateTime

from scripts.validate_data_submission import (_compare_dates,
    _check_start_end_times, _check_contiguity, _check_data_point,
    identify_filename_metadata, FileValidationError)


class TestIdentifyFilenameMetadata(TestCase):
    @mock.patch('scripts.validate_data_submission.os.path.getsize')
    def setUp(self, mock_getsize):
        mock_getsize.return_value = 1234

        filename = 'clt_Amon_HadGEM2-ES_historical_r1i1p1_185912-188411.nc'

        self.metadata = identify_filename_metadata(filename)

    def test_cmor_name(self):
        self.assertEqual(self.metadata['cmor_name'], 'clt')

    def test_table(self):
        self.assertEqual(self.metadata['table'], 'Amon')

    def test_climate_model(self):
        self.assertEqual(self.metadata['climate_model'], 'HadGEM2-ES')

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


class TestCompareDates(TestCase):
    def setUp(self):
        self.pdt = PartialDateTime(year=1978, month=7)
        self.dt1 = datetime.datetime(1978, 7, 19, 0, 43, 17)
        self.dt2 = datetime.datetime(1982, 8, 23, 18, 11, 59)

    def test_equals(self):
        self.assertTrue(_compare_dates(self.pdt, self.dt1))

    def test_not_equals(self):
        self.assertFalse(_compare_dates(self.pdt, self.dt2))


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
