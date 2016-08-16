"""
test_validate_data_submission.py - unit tests for validate_data_submission.py
"""
import datetime
from django.test import TestCase
import mock

from iris.tests.stock import realistic_3d
from iris.time import PartialDateTime

from scripts.validate_data_submission import (_compare_dates,
    _check_start_end_times, _check_contiguity, _check_data_point,
    FileValidationError)


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

        self.metadata_1 = {
            'start_date': PartialDateTime(year=2014, month=12),
            'end_date': PartialDateTime(year=2014, month=12)}
        self.metadata_2 = {
            'start_date': PartialDateTime(year=2014, month=11),
            'end_date': PartialDateTime(year=2014, month=12)}
        self.metadata_3 = {
            'start_date': PartialDateTime(year=2013, month=12),
            'end_date': PartialDateTime(year=2014, month=12)}
        self.metadata_4 = {
            'start_date': PartialDateTime(year=2015, month=11),
            'end_date': PartialDateTime(year=2014, month=12)}

        # mock logger to prevent logger displaying messages on screen
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
