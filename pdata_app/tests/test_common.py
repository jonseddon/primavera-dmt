"""
test_common.py - unit tests for pdata_app.utils.common.py
"""
from iris.time import PartialDateTime
from numpy.testing import assert_almost_equal

from django.test import TestCase

from pdata_app.utils.common import (make_partial_date_time,
                                    standardise_time_unit,
                                    calc_last_day_in_month, pdt2num)

class TestMakePartialDateTime(TestCase):
    def test_yyyymm(self):
        expected = PartialDateTime(year=2014, month=8)
        actual = make_partial_date_time('201408')
        self.assertEqual(actual, expected)

    def test_yyyymmdd(self):
        expected = PartialDateTime(year=2014, month=8, day=1)
        actual = make_partial_date_time('20140801')
        self.assertEqual(actual, expected)

    def test_yyyy(self):
        self.assertRaises(ValueError, make_partial_date_time, '2014')


class TestCalcLastDayInMonth(TestCase):
    def test_31_days(self):
        actual = calc_last_day_in_month(2016, 10, calendar='gregorian')

        self.assertEqual(actual, 31)

    def test_30_days(self):
        actual = calc_last_day_in_month(2016, 10, calendar='360_day')

        self.assertEqual(actual, 30)

    def test_360_day_february(self):
        actual = calc_last_day_in_month(2016, 2, calendar='360_day')

        self.assertEqual(actual, 30)

    def test_gregorian_february(self):
        actual = calc_last_day_in_month(2015, 2, calendar='gregorian')

        self.assertEqual(actual, 28)

    def test_gregorian_leap_year(self):
        actual = calc_last_day_in_month(2016, 2, calendar='gregorian')

        self.assertEqual(actual, 29)

    def test_360_day_december(self):
        actual = calc_last_day_in_month(2016, 12, calendar='360_day')

        self.assertEqual(actual, 30)

    def test_gregorian_december(self):
        actual = calc_last_day_in_month(2016, 12, calendar='gregorian')

        self.assertEqual(actual, 31)


class TestPdt2Num(TestCase):
    def test_year_month_day(self):
        pdt = PartialDateTime(2016, 8, 22)
        actual = pdt2num(pdt, 'days since 2016-08-20', 'gregorian')

        expected = 2.

        self.assertEqual(actual, expected)

    def test_year_month(self):
        pdt = PartialDateTime(2016, 8)
        actual = pdt2num(pdt, 'days since 2016-08-20', 'gregorian')

        expected = -19.

        self.assertEqual(actual, expected)

    def test_year_month_end_period(self):
        pdt = PartialDateTime(2016, 8)
        actual = pdt2num(pdt, 'days since 2016-08-20', 'gregorian',
                         start_of_period=False)

        expected = 11.

        self.assertEqual(actual, expected)

    def test_year_month_end_period_360_day(self):
        pdt = PartialDateTime(2016, 8)
        actual = pdt2num(pdt, 'days since 2016-08-20', '360_day',
                         start_of_period=False)

        expected = 10.

        self.assertEqual(actual, expected)

    def test_date_time(self):
        pdt = PartialDateTime(2016, 8, 22, 14, 42, 11)
        actual = pdt2num(pdt, 'days since 2016-08-20', 'gregorian')

        expected = 2.6126273148148148

        assert_almost_equal(actual, expected)

    def test_year(self):
        pdt = PartialDateTime(2016)
        self.assertRaises(ValueError, pdt2num, pdt, 'days since 2016-08-20',
            'gregorian')


class TestStandardiseTimeUnit(TestCase):
    """
    Test _standardise_time_unit()
    """
    def test_same_units(self):
        time_unit = 'days since 2000-01-01'
        time_num = 3.14159

        actual = standardise_time_unit(time_num, time_unit, time_unit, '360_day')
        assert_almost_equal(actual, time_num)

    def test_different_units(self):
        old_unit = 'days since 2000-01-01'
        new_unit = 'days since 2000-02-01'
        time_num = 33.14159

        actual = standardise_time_unit(time_num, old_unit, new_unit, '360_day')
        expected = 3.14159
        assert_almost_equal(actual, expected, decimal=5)

    def test_none(self):
        time_unit = 'days since 2000-01-01'
        self.assertIsNone(standardise_time_unit(None, time_unit,
                                                time_unit, '360_day'))
