"""
test_common.py - unit tests for pdata_app.utils.common.py
"""
from __future__ import unicode_literals, division, absolute_import
import six

from iris.time import PartialDateTime
from numpy.testing import assert_almost_equal

from django.contrib.auth.models import User
from django.test import TestCase

from pdata_app import models
from pdata_app.utils.common import (make_partial_date_time,
                                    standardise_time_unit,
                                    calc_last_day_in_month, pdt2num,
                                    is_same_gws, get_request_size,
                                    date_filter_files, grouper)
from pdata_app.utils import dbapi
from vocabs.vocabs import STATUS_VALUES, FREQUENCY_VALUES, VARIABLE_TYPES


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


class TestIsSameGws(TestCase):
    def test_same(self):
        path1 = '/group_workspaces/jasmin2/primavera1/some/dir'
        path2 = '/group_workspaces/jasmin2/primavera1/another/dir'

        self.assertTrue(is_same_gws(path1, path2))

    def test_diff(self):
        path1 = '/group_workspaces/jasmin2/primavera1/some/dir'
        path2 = '/group_workspaces/jasmin2/primavera2/some/dir'

        self.assertFalse(is_same_gws(path1, path2))

    def test_new_same(self):
        path1 = '/gws/nopw/j04/primavera1/some/dir'
        path2 = '/gws/nopw/j04/primavera1/another/dir'

        self.assertTrue(is_same_gws(path1, path2))

    def test_new_diff(self):
        path1 = '/gws/nopw/j04/primavera1/some/dir'
        path2 = '/gws/nopw/j04/primavera5/another/dir'

        self.assertFalse(is_same_gws(path1, path2))

    def test_bad_path(self):
        path1 = 'primavera1/some/dir'
        path2 = '/group_workspaces/jasmin2/primavera2/some/dir'

        six.assertRaisesRegex(self, ValueError,
                              'path1 format is not a recognised group workspace'
                              ' pattern: primavera1/some/dir', is_same_gws,
                                path1, path2)

    def test_slightly_bad_path(self):
        path1 = '/group_workspaces/jasmin2/primavera2/some/dir'
        path2 = '/group_workspaces/jasmin1/primavera1/some/dir'

        six.assertRaisesRegex(self, RuntimeError, 'Cannot determine group '
            'workspace name from /group_workspaces/jasmin1/primavera1/some/dir',
                                is_same_gws, path1, path2)


class TestGetRequestSize(TestCase):
    def setUp(self):
        _make_example_files(self)

    def test_all_files(self):
        rreq = dbapi.get_or_create(models.RetrievalRequest,
                                   requester=self.user,
                                   start_year=1950, end_year=2000)
        rreq.data_request.add(self.dreq1)
        rreq.save()
        rreq.data_request.add(self.dreq2)
        rreq.save()

        self.assertEqual(15, get_request_size(rreq.data_request.all(),
                                             rreq.start_year, rreq.end_year))

    def test_dates(self):
        rreq = dbapi.get_or_create(models.RetrievalRequest,
                                    requester=self.user,
                                    start_year=1950, end_year=1975)
        rreq.data_request.add(self.dreq1)
        rreq.save()
        rreq.data_request.add(self.dreq2)
        rreq.save()

        self.assertEqual(7, get_request_size(rreq.data_request.all(),
                                             rreq.start_year, rreq.end_year))

    def test_online(self):
        rreq = dbapi.get_or_create(models.RetrievalRequest,
                                    requester=self.user,
                                    start_year=1950, end_year=2014)
        rreq.data_request.add(self.dreq1)
        rreq.save()
        rreq.data_request.add(self.dreq2)
        rreq.save()

        self.assertEqual(3, get_request_size(rreq.data_request.all(),
                         rreq.start_year, rreq.end_year, online=True))

    def test_offline(self):
        rreq = dbapi.get_or_create(models.RetrievalRequest,
                                    requester=self.user,
                                    start_year=1950, end_year=2014)
        rreq.data_request.add(self.dreq1)
        rreq.save()
        rreq.data_request.add(self.dreq2)
        rreq.save()

        self.assertEqual(12, get_request_size(rreq.data_request.all(),
                                             rreq.start_year, rreq.end_year,
                                             offline=True))

    def test_both_options(self):
        rreq = dbapi.get_or_create(models.RetrievalRequest,
                                    requester=self.user,
                                    start_year=1950, end_year=2014)
        rreq.data_request.add(self.dreq1)
        rreq.save()
        rreq.data_request.add(self.dreq2)
        rreq.save()

        self.assertRaises(ValueError, get_request_size,
                          rreq.data_request.all(), rreq.start_year,
                          rreq.end_year, online= True, offline=True)

    def test_no_files(self):
        rreq = dbapi.get_or_create(models.RetrievalRequest,
                                    requester=self.user,
                                    start_year=2001, end_year=2014)
        rreq.data_request.add(self.dreq1)
        rreq.save()
        rreq.data_request.add(self.dreq2)
        rreq.save()

        self.assertEqual(0, get_request_size(rreq.data_request.all(),
                                             rreq.start_year, rreq.end_year,
                                             offline=True))

    def test_manual_list(self):
        self.assertEqual(15, get_request_size([self.dreq1, self.dreq2],
                                             1950, 2000))


class TestDateFilterFiles(TestCase):
    def setUp(self):
        _make_example_files(self)

    def test_entirely_contains(self):
        data_files =  models.DataFile.objects.all()
        self.assertEqual(['test1', 'test2'],
                         _assertable(date_filter_files(data_files,
                                                       1949, 1961)))

    def test_end_spans(self):
        data_files =  models.DataFile.objects.all()
        self.assertEqual(['test1', 'test2', 'test4'],
                         _assertable(date_filter_files(data_files,
                                                       1949, 1975)))

    def test_start_spans(self):
        data_files =  models.DataFile.objects.all()
        self.assertEqual(['test2', 'test4', 'test8'],
                         _assertable(date_filter_files(data_files,
                                                       1965, 1995)))

    def test_both_span(self):
        data_files =  models.DataFile.objects.all()
        self.assertEqual(['test1', 'test2', 'test4', 'test8'],
                         _assertable(date_filter_files(data_files,
                                                       1955, 1987)))

    def test_subset_entirely(self):
        data_files =  models.DataFile.objects.filter(online=False)
        self.assertEqual(['test4', 'test8'],
                         _assertable(date_filter_files(data_files,
                                                       1952, 1992)))

    def test_subset_spans(self):
        data_files =  models.DataFile.objects.filter(online=False)
        self.assertEqual(['test4', 'test8'],
                         _assertable(date_filter_files(data_files,
                                                       1975, 1985)))


class TestGrouper(TestCase):
    def test_exact_multiple(self):
        actual = [list(chunk) for chunk in grouper(range(8), 4)]
        expected = [[0, 1, 2, 3], [4, 5, 6, 7]]
        self.assertEqual(actual, expected)

    def test_non_exact_multiple(self):
        actual = [list(chunk) for chunk in grouper(range(10), 4)]
        expected = [[0, 1, 2, 3], [4, 5, 6, 7], [8, 9]]
        self.assertEqual(actual, expected)

    def test_n_is_one(self):
        actual = [list(chunk) for chunk in grouper(range(3), 1)]
        expected = [[0], [1], [2]]
        self.assertEqual(actual, expected)

    def test_other_type(self):
        actual = [list(chunk) for chunk in grouper(list(range(10)), 4)]
        expected = [[0, 1, 2, 3], [4, 5, 6, 7], [8, 9]]
        self.assertEqual(actual, expected)


def _make_example_files(parent_obj):
    """
    Create some common test data. Attach the items that tests need to refer-to
    to the parent object. Other items will just exist in the database.

    :param parent_obj: the parent object
    """
    project = dbapi.get_or_create(models.Project, short_name='t',
                                  full_name='test')
    clim_mod = dbapi.get_or_create(models.ClimateModel, short_name='t',
                                   full_name='test')
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
                                dimensions='massive', cmor_name='var1',
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
    parent_obj.user = dbapi.get_or_create(User, username='fred')
    act_id = dbapi.get_or_create(models.ActivityId,
                                 short_name='HighResMIP',
                                 full_name='High Resolution Model Intercomparison Project')
    dsub = dbapi.get_or_create(models.DataSubmission,
                               status=STATUS_VALUES['EXPECTED'],
                               incoming_directory='/some/dir',
                               directory='/some/dir', user=parent_obj.user)
    data_file1 = dbapi.get_or_create(models.DataFile, name='test1',
                                     incoming_directory='/some/dir',
                                     directory='/some/dir', size=1,
                                     project=project,
                                     climate_model=clim_mod,
                                     institute=institute,
                                     experiment=expt,
                                     variable_request=vble1,
                                     data_request=parent_obj.dreq1,
                                     activity_id=act_id, frequency='t',
                                     rip_code='r1i1p1',
                                     data_submission=dsub, online=True,
                                     start_time=0,  # 1950-01-01 00:00:00
                                     end_time=3600,  # 1960-01-01 00:00:00
                                     calendar='360_day',
                                     time_units='days since 1950-01-01')
    data_file4 = dbapi.get_or_create(models.DataFile, name='test4',
                                     incoming_directory='/some/dir',
                                     directory='/some/dir', size=4,
                                     project=project,
                                     climate_model=clim_mod,
                                     institute=institute,
                                     experiment=expt,
                                     variable_request=vble1,
                                     data_request=parent_obj.dreq1,
                                     activity_id=act_id, frequency='t',
                                     rip_code='r1i1p1',
                                     data_submission=dsub, online=False,
                                     start_time=7200,  # 1970-01-01 00:00:00
                                     end_time=10800,  # 1980-01-01 00:00:00
                                     calendar='360_day',
                                     time_units='days since 1950-01-01')
    data_file8 = dbapi.get_or_create(models.DataFile, name='test8',
                                     incoming_directory='/some/dir',
                                     directory='/some/dir', size=8,
                                     project=project,
                                     climate_model=clim_mod,
                                     institute=institute,
                                     experiment=expt,
                                     variable_request=vble1,
                                     data_request=parent_obj.dreq1,
                                     activity_id=act_id, frequency='t',
                                     rip_code='r1i1p1',
                                     data_submission=dsub, online=False,
                                     start_time=10800,  # 1980-01-01 00:00:00
                                     end_time=10800,  # 1990-01-01 00:00:00
                                     calendar='360_day',
                                     time_units='days since 1950-01-01')
    data_file2 = dbapi.get_or_create(models.DataFile, name='test2',
                                     incoming_directory='/some/dir',
                                     directory='/some/dir', size=2,
                                     project=project,
                                     climate_model=clim_mod,
                                     institute=institute,
                                     experiment=expt,
                                     variable_request=vble2,
                                     data_request=parent_obj.dreq2,
                                     activity_id=act_id, frequency='t',
                                     rip_code='r1i1p1',
                                     data_submission=dsub, online=True)


def _assertable(queryset, list_item='name'):
    """
    From a Django queryset return a list of one attribute that can be passed
    to an assert statement. The default model attribute to compare is name.

    :param django.db.models.query.QuerySet queryset:
    :returns: A list of the specified attrributes from each element of the
        queryset.
    :rtype: list
    """
    return list(queryset.order_by(list_item).values_list(list_item, flat=True))
