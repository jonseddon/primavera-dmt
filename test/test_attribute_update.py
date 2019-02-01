"""
test_attribute_update.py

Test of attribute_update.py
"""
import io
import mock

import django
django.setup()

from django.test import TestCase

from pdata_app.models import ClimateModel, DataFile
from pdata_app.tests.common import make_example_files

from scripts.attribute_update import SourceIdUpdate, VariantLabelUpdate
from scripts.attribute_update import main as att_update_main


class TestSourceIdUpdate(TestCase):
    """Test scripts.attribute_update.SourceIdUpdate"""
    def setUp(self):
        make_example_files(self)
        self.test_file = DataFile.objects.get(name='test1')
        _make_files_realistic()
        self.test_file.refresh_from_db()
        self.desired_source_id = 'better-model'
        ClimateModel.objects.create(short_name=self.desired_source_id)

        # mock sys.stdout to prevent unwanted output but also allow
        # testing of output
        patch = mock.patch('sys.stdout', new_callable=io.StringIO)
        self.mock_stdout = patch.start()
        self.addCleanup(patch.stop)


    def test_source_id_updated(self):
        updater = SourceIdUpdate(self.test_file, self.desired_source_id)
        updater.update()
        self.test_file.refresh_from_db()
        self.assertEqual(self.test_file.climate_model.short_name,
                         self.desired_source_id)

    def test_filename_updated(self):
        updater = SourceIdUpdate(self.test_file, self.desired_source_id)
        updater.update()
        self.test_file.refresh_from_db()
        desired_filename = 'var1_Amon_better-model_t_r1i1p1_gn_1950-1960.nc'
        self.assertEqual(self.test_file.name, desired_filename)

    def test_directory_updated(self):
        updater = SourceIdUpdate(self.test_file, self.desired_source_id)
        updater.update()
        self.test_file.refresh_from_db()
        desired_dir = ('/group_workspaces/jasmin2/primavera9/stream1/t/'
                       'HighResMIP/MOHC/better-model/t/r1i1p1/Amon/var1/gn/'
                       'v12345678')
        self.assertEqual(self.test_file.directory, desired_dir)

    def test_report_results(self):
        updater = SourceIdUpdate(self.test_file, self.desired_source_id)
        updater.update()
        actual = self.mock_stdout.getvalue()
        expected = ('{"filename": "var1_Amon_better-model_t_r1i1p1_gn_1950-'
                    '1960.nc", "directory": "/group_workspaces/jasmin2/'
                    'primavera9/stream1/t/HighResMIP/MOHC/better-model/t/'
                    'r1i1p1/Amon/var1/gn/v12345678"}\n')
        self.assertEqual(actual, expected)


class TestVariantLabelUpdate(TestCase):
    """Test scripts.attribute_update.VariantLabelUpdate"""
    def setUp(self):
        make_example_files(self)
        self.test_file = DataFile.objects.get(name='test1')
        _make_files_realistic()
        self.test_file.refresh_from_db()
        self.desired_variant_label = 'r9i9p9f9'

        # mock sys.stdout to prevent unwanted output but also allow
        # testing of output
        patch = mock.patch('sys.stdout', new_callable=io.StringIO)
        self.mock_stdout = patch.start()
        self.addCleanup(patch.stop)

    def test_variant_label_updated(self):
        updater = VariantLabelUpdate(self.test_file, self.desired_variant_label)
        updater.update()
        self.test_file.refresh_from_db()
        self.assertEqual(self.test_file.rip_code, self.desired_variant_label)

    def test_filename_updated(self):
        updater = VariantLabelUpdate(self.test_file, self.desired_variant_label)
        updater.update()
        self.test_file.refresh_from_db()
        desired_filename = 'var1_Amon_t_t_r9i9p9f9_gn_1950-1960.nc'
        self.assertEqual(self.test_file.name, desired_filename)

    def test_directory_updated(self):
        updater = VariantLabelUpdate(self.test_file, self.desired_variant_label)
        updater.update()
        self.test_file.refresh_from_db()
        desired_dir = ('/group_workspaces/jasmin2/primavera9/stream1/t/'
                       'HighResMIP/MOHC/t/t/r9i9p9f9/Amon/var1/gn/v12345678')
        self.assertEqual(self.test_file.directory, desired_dir)

    def test_report_results(self):
        updater = VariantLabelUpdate(self.test_file, self.desired_variant_label)
        updater.update()
        actual = self.mock_stdout.getvalue()
        expected = ('{"filename": "var1_Amon_t_t_r9i9p9f9_gn_1950-1960.nc", '
                    '"directory": "/group_workspaces/jasmin2/primavera9/'
                    'stream1/t/HighResMIP/MOHC/t/t/r9i9p9f9/Amon/var1/gn/'
                    'v12345678"}\n')
        self.assertEqual(actual, expected)


class TestIntegration(TestCase):
    """Test scripts.attribute_update.main"""
    def setUp(self):
        make_example_files(self)
        self.test_file = DataFile.objects.get(name='test1')
        _make_files_realistic()
        self.test_file.refresh_from_db()

        # mock sys.stdout and stderr to prevent unwanted output but also allow
        # testing of output
        patch = mock.patch('sys.stdout', new_callable=io.StringIO)
        self.mock_stdout = patch.start()
        self.addCleanup(patch.stop)

        patch = mock.patch('sys.stderr', new_callable=io.StringIO)
        self.mock_stderr = patch.start()
        self.addCleanup(patch.stop)

    def test_variant_label(self):
        new_var_label = 'r1i1p2f1'
        class ArgparseNamespace(object):
            file_path = '/group_workspaces/jasmin2/primavera9/stream1/path/var_table_model_expt_varlab_gn_1-2.nc'
            attribute_name = 'variant_label'
            new_value = new_var_label
        ns = ArgparseNamespace()

        att_update_main(ns)
        self.test_file.refresh_from_db()
        self.assertEqual(self.test_file.rip_code, new_var_label)

    def test_climate_model(self):
        new_model = 'bestest-ever-model'
        ClimateModel.objects.create(short_name=new_model)
        class ArgparseNamespace(object):
            file_path = '/group_workspaces/jasmin2/primavera9/stream1/path/var_table_model_expt_varlab_gn_1-2.nc'
            attribute_name = 'source_id'
            new_value = new_model
        ns = ArgparseNamespace()

        att_update_main(ns)
        self.test_file.refresh_from_db()
        self.assertEqual(self.test_file.climate_model.short_name, new_model)

    def test_some_other_option(self):
        class ArgparseNamespace(object):
            file_path = '/group_workspaces/jasmin2/primavera9/stream1/path/var_table_model_expt_varlab_gn_1-2.nc'
            attribute_name = 'other-option'
            new_value = 'new-model'
        ns = ArgparseNamespace()

        self.assertRaises(SystemExit, att_update_main, ns)

def _make_files_realistic():
    """
    Update the files in the database created by make_example_files() to make
    them realistic enough to run these tests.
    """
    datafile = DataFile.objects.get(name='test1')
    datafile.name = 'var_table_model_expt_varlab_gn_1-2.nc'
    datafile.directory = '/group_workspaces/jasmin2/primavera9/stream1/path'
    datafile.save()

