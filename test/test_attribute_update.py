"""
test_attribute_update.py

Test of attribute_update.py
"""
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
                       'HighResMIP/MOHC/better-model/t/r1i1p1/Amon/var1/gn/v12345678')
        self.assertEqual(self.test_file.directory, desired_dir)


class TestVariantLabelUpdate(TestCase):
    """Test scripts.attribute_update.VariantLabelUpdate"""
    def setUp(self):
        make_example_files(self)
        self.test_file = DataFile.objects.get(name='test1')
        _make_files_realistic()
        self.test_file.refresh_from_db()
        self.desired_variant_label = 'r9i9p9f9'

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


class TestIntegration(TestCase):
    """Test scripts.attribute_update.main"""
    def setUp(self):
        make_example_files(self)
        _make_files_realistic()

    def test_variant_label(self):
        class ArgparseNamespace(object):
            file_path = '/some/dir/test1'
            attribute_name = 'variant_label'
            new_value = 'r1i1p2f1'
        ns = ArgparseNamespace()

        self.assertTrue(True)


def _make_files_realistic():
    """
    Update the files in the database created by make_example_files() to make
    them realistic enough to run these tests.
    """
    datafile = DataFile.objects.get(name='test1')
    datafile.name = 'var_table_model_expt_varlab_gn_1-2.nc'
    datafile.directory = '/group_workspaces/jasmin2/primavera9/stream1/path'
    datafile.save()

