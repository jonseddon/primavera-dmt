"""
test_validate_data_submission.py - unit tests for validate_data_submission.py
"""
import datetime
from django.test import TestCase
import mock

import iris
from iris.tests.stock import realistic_3d
from iris.time import PartialDateTime
import pytz

from scripts.validate_data_submission import (_check_start_end_times,
    _check_contiguity, _check_data_point, identify_filename_metadata,
    FileValidationError, _make_partial_date_time, _pdt_to_datetime)


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

    @mock.patch('scripts.validate_data_submission.logger')
    def test_bad_date_format(self, mock_logger):
        filename = 'clt_Amon_HadGEM2-ES_historical_r1i1p1_1859-1884.nc'
        self.assertRaises(FileValidationError,
            identify_filename_metadata, filename)


class TestPdtToDatetime(TestCase):
    def test_year_month_day(self):
        pdt = PartialDateTime(2016, 8, 22)
        actual = _pdt_to_datetime(pdt)

        expected = datetime.datetime(2016, 8, 22, 0, 0, tzinfo=pytz.UTC)

        self.assertEqual(actual, expected)

    def test_year_month(self):
        pdt = PartialDateTime(2016, 8)
        actual = _pdt_to_datetime(pdt)

        expected = datetime.datetime(2016, 8, 1, 0, 0, tzinfo=pytz.UTC)

        self.assertEqual(actual, expected)

    @mock.patch('scripts.validate_data_submission.logger')
    def test_year(self, mock_logger):
        # logger mocked to prevent error from appearing on screen
        pdt = PartialDateTime(2016)
        self.assertRaises(ValueError, _pdt_to_datetime, pdt)


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


class TestMakePartialDateTime(TestCase):
    def test_yyyymm(self):
        expected = PartialDateTime(year=2014, month=8)
        actual = _make_partial_date_time('201408')
        self.assertEqual(actual, expected)

    def test_yyyymmdd(self):
        expected = PartialDateTime(year=2014, month=8, day=1)
        actual = _make_partial_date_time('20140801')
        self.assertEqual(actual, expected)

    def test_yyyy(self):
        self.assertRaises(ValueError, _make_partial_date_time, '2014')
