"""
test_attribute_update.py

Test of attribute_update.py
"""
try:
    from unittest import mock
except ImportError:
    import mock

import django
django.setup()

from django.test import TestCase

from pdata_app.models import ClimateModel, DataFile, Settings
from pdata_app.tests.common import make_example_files

from pdata_app.utils.attribute_update import (SourceIdUpdate,
                                              VariantLabelUpdate,
                                              FileOfflineError,
                                              FileNotOnDiskError)
import pdata_app.utils.attribute_update
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

        mock.patch.object(pdata_app.utils.attribute_update, 'BASE_OUTPUT_DIR',
                          return_value = '/gws/nopw/j04/primavera5/stream1')

    def test_online(self):
        self.test_file.online = False
        self.test_file.save()
        updater = SourceIdUpdate(self.test_file, self.desired_source_id)
        self.assertRaises(FileOfflineError, updater.update)

    def test_not_on_disk(self):
        updater = SourceIdUpdate(self.test_file, self.desired_source_id)
        self.assertRaises(FileNotOnDiskError, updater.update)

    @mock.patch('pdata_app.utils.attribute_update.DmtUpdate._check_available')
    @mock.patch('pdata_app.utils.attribute_update.DmtUpdate._rename_file')
    def test_source_id_updated(self, mock_reanme, mock_available):
        updater = SourceIdUpdate(self.test_file, self.desired_source_id)
        updater.update()
        self.test_file.refresh_from_db()
        self.assertEqual(self.test_file.climate_model.short_name,
                         self.desired_source_id)

    @mock.patch('pdata_app.utils.attribute_update.DmtUpdate._check_available')
    @mock.patch('pdata_app.utils.attribute_update.DmtUpdate._rename_file')
    def test_filename_updated(self, mock_reanme, mock_available):
        updater = SourceIdUpdate(self.test_file, self.desired_source_id)
        updater.update()
        self.test_file.refresh_from_db()
        desired_filename = 'var1_Amon_better-model_t_r1i1p1_gn_1950-1960.nc'
        self.assertEqual(self.test_file.name, desired_filename)

    @mock.patch('pdata_app.utils.attribute_update.DmtUpdate._check_available')
    @mock.patch('pdata_app.utils.attribute_update.DmtUpdate._rename_file')
    def test_directory_updated(self, mock_reanme, mock_available):
        updater = SourceIdUpdate(self.test_file, self.desired_source_id)
        updater.update()
        self.test_file.refresh_from_db()
        desired_dir = ('/gws/nopw/j04/primavera9/stream1/t/'
                       'HighResMIP/MOHC/better-model/t/r1i1p1/Amon/var1/gn/'
                       'v12345678')
        self.assertEqual(self.test_file.directory, desired_dir)

    @mock.patch('pdata_app.utils.attribute_update.DmtUpdate._check_available')
    @mock.patch('pdata_app.utils.attribute_update.os.makedirs')
    @mock.patch('pdata_app.utils.attribute_update.os.rename')
    @mock.patch('pdata_app.utils.attribute_update.os.listdir')
    @mock.patch('pdata_app.utils.attribute_update.delete_drs_dir')
    @mock.patch('pdata_app.utils.attribute_update.os.symlink')
    @mock.patch('pdata_app.utils.attribute_update.os.path.lexists')
    @mock.patch('pdata_app.utils.attribute_update.os.path.islink')
    @mock.patch('pdata_app.utils.attribute_update.os.remove')
    def test_renaming(self, mock_rm, mock_islink, mock_lexists, mock_symlink,
                      mock_del_drs_dir, mock_ls, mock_rename, mock_mkdirs,
                      mock_available):
        mock_islink.return_vale = True
        mock_lexists.return_vale = True
        mock_ls.return_value = False
        updater = SourceIdUpdate(self.test_file, self.desired_source_id)
        updater.update()
        mock_mkdirs.assert_has_calls([
            mock.call('/gws/nopw/j04/primavera9/stream1/t/HighResMIP/MOHC/'
                      'better-model/t/r1i1p1/Amon/var1/gn/v12345678'),
            mock.call('/gws/nopw/j04/primavera5/stream1/t/HighResMIP/MOHC/'
                      'better-model/t/r1i1p1/Amon/var1/gn/v12345678')
        ])
        mock_rename.assert_called_with(
            '/gws/nopw/j04/primavera9/stream1/path/'
            'var1_table_model_expt_varlab_gn_1-2.nc',
            '/gws/nopw/j04/primavera9/stream1/t/HighResMIP/MOHC/better-model/'
            't/r1i1p1/Amon/var1/gn/v12345678/'
            'var1_Amon_better-model_t_r1i1p1_gn_1950-1960.nc'
        )

    @mock.patch('pdata_app.utils.attribute_update.DmtUpdate._check_available')
    @mock.patch('pdata_app.utils.attribute_update.os.makedirs')
    @mock.patch('pdata_app.utils.attribute_update.os.rename')
    @mock.patch('pdata_app.utils.attribute_update.os.listdir')
    @mock.patch('pdata_app.utils.attribute_update.delete_drs_dir')
    @mock.patch('pdata_app.utils.attribute_update.os.symlink')
    @mock.patch('pdata_app.utils.attribute_update.os.path.lexists')
    @mock.patch('pdata_app.utils.attribute_update.os.path.islink')
    @mock.patch('pdata_app.utils.attribute_update.os.remove')
    def test_sym_link(self, mock_rm, mock_islink, mock_lexists, mock_symlink,
                      mock_del_drs_dir, mock_ls, mock_rename, mock_mkdirs,
                      mock_available):
        mock_islink.return_vale = True
        mock_lexists.return_vale = True
        mock_ls.return_value = False
        updater = SourceIdUpdate(self.test_file, self.desired_source_id)
        updater.update()
        mock_rm.assert_called_once_with(
            '/gws/nopw/j04/primavera5/stream1/t/HighResMIP/MOHC/t/t/r1i1p1/'
            'Amon/var1/gn/v12345678/var1_table_model_expt_varlab_gn_1-2.nc'
        )
        mock_mkdirs.assert_has_calls([
            mock.call('/gws/nopw/j04/primavera9/stream1/t/HighResMIP/MOHC/'
                      'better-model/t/r1i1p1/Amon/var1/gn/v12345678'),
            mock.call('/gws/nopw/j04/primavera5/stream1/t/HighResMIP/MOHC/'
                      'better-model/t/r1i1p1/Amon/var1/gn/v12345678')
        ])
        mock_symlink.assert_called_with(
            '/gws/nopw/j04/primavera9/stream1/t/HighResMIP/MOHC/better-model/'
            't/r1i1p1/Amon/var1/gn/v12345678/'
            'var1_Amon_better-model_t_r1i1p1_gn_1950-1960.nc',
            '/gws/nopw/j04/primavera5/stream1/t/HighResMIP/MOHC/better-model/'
            't/r1i1p1/Amon/var1/gn/v12345678/'
            'var1_Amon_better-model_t_r1i1p1_gn_1950-1960.nc'

        )

    @mock.patch('pdata_app.utils.attribute_update.DmtUpdate._check_available')
    @mock.patch('pdata_app.utils.attribute_update.os.makedirs')
    @mock.patch('pdata_app.utils.attribute_update.os.rename')
    @mock.patch('pdata_app.utils.attribute_update.os.listdir')
    @mock.patch('pdata_app.utils.attribute_update.delete_drs_dir')
    @mock.patch('pdata_app.utils.attribute_update.os.symlink')
    @mock.patch('pdata_app.utils.attribute_update.os.path.lexists')
    @mock.patch('pdata_app.utils.attribute_update.os.path.islink')
    @mock.patch('pdata_app.utils.attribute_update.os.remove')
    def test_sym_link_not_required(self, mock_rm, mock_islink, mock_lexists,
                                   mock_symlink, mock_del_drs_dir, mock_ls,
                                   mock_rename, mock_mkdirs, mock_available):
        mock_islink.return_vale = True
        mock_lexists.return_vale = True
        mock_ls.return_value = False

        self.test_file.directory = '/gws/nopw/j04/primavera5/stream1/path'
        self.test_file.save()

        updater = SourceIdUpdate(self.test_file, self.desired_source_id)
        updater.update()
        mock_mkdirs.assert_called_once_with(
            '/gws/nopw/j04/primavera5/stream1/t/HighResMIP/MOHC/better-model/'
            't/r1i1p1/Amon/var1/gn/v12345678'
        )
        mock_rename.assert_called_with(
            '/gws/nopw/j04/primavera5/stream1/path/'
            'var1_table_model_expt_varlab_gn_1-2.nc',
            '/gws/nopw/j04/primavera5/stream1/t/HighResMIP/MOHC/better-model/'
            't/r1i1p1/Amon/var1/gn/v12345678/'
            'var1_Amon_better-model_t_r1i1p1_gn_1950-1960.nc'
        )
        mock_symlink.assert_not_called()


class TestVariantLabelUpdate(TestCase):
    """Test scripts.attribute_update.VariantLabelUpdate"""
    def setUp(self):
        make_example_files(self)
        self.test_file = DataFile.objects.get(name='test1')
        _make_files_realistic()
        self.test_file.refresh_from_db()
        self.desired_variant_label = 'r9i9p9f9'

    @mock.patch('pdata_app.utils.attribute_update.DmtUpdate._check_available')
    @mock.patch('pdata_app.utils.attribute_update.DmtUpdate._rename_file')
    def test_variant_label_updated(self, mock_reanme, mock_available):
        updater = VariantLabelUpdate(self.test_file, self.desired_variant_label)
        updater.update()
        self.test_file.refresh_from_db()
        self.assertEqual(self.test_file.rip_code, self.desired_variant_label)

    @mock.patch('pdata_app.utils.attribute_update.DmtUpdate._check_available')
    @mock.patch('pdata_app.utils.attribute_update.DmtUpdate._rename_file')
    def test_filename_updated(self, mock_reanme, mock_available):
        updater = VariantLabelUpdate(self.test_file, self.desired_variant_label)
        updater.update()
        self.test_file.refresh_from_db()
        desired_filename = 'var1_Amon_t_t_r9i9p9f9_gn_1950-1960.nc'
        self.assertEqual(self.test_file.name, desired_filename)

    @mock.patch('pdata_app.utils.attribute_update.DmtUpdate._check_available')
    @mock.patch('pdata_app.utils.attribute_update.DmtUpdate._rename_file')
    def test_directory_updated(self, mock_reanme, mock_available):
        updater = VariantLabelUpdate(self.test_file, self.desired_variant_label)
        updater.update()
        self.test_file.refresh_from_db()
        desired_dir = ('/gws/nopw/j04/primavera9/stream1/t/'
                       'HighResMIP/MOHC/t/t/r9i9p9f9/Amon/var1/gn/v12345678')
        self.assertEqual(self.test_file.directory, desired_dir)


# class TestIntegration(TestCase):
#     """Test scripts.attribute_update.main"""
#     def setUp(self):
#         make_example_files(self)
#         self.test_file = DataFile.objects.get(name='test1')
#         _make_files_realistic()
#         self.test_file.refresh_from_db()
#
#     def test_variant_label(self):
#         new_var_label = 'r1i1p2f1'
#
#         class ArgparseNamespace(object):
#             file_path = ('/gws/nopw/j04/primavera9/stream1/path/var_table_'
#                          'model_expt_varlab_gn_1-2.nc')
#             attribute_name = 'variant_label'
#             new_value = new_var_label
#         ns = ArgparseNamespace()
#
#         att_update_main(ns)
#         self.test_file.refresh_from_db()
#         self.assertEqual(self.test_file.rip_code, new_var_label)
#
#     def test_climate_model(self):
#         new_model = 'bestest-ever-model'
#         ClimateModel.objects.create(short_name=new_model)
#
#         class ArgparseNamespace(object):
#             file_path = ('/gws/nopw/j04/primavera9/stream1/path/var_table_'
#                          'model_expt_varlab_gn_1-2.nc')
#             attribute_name = 'source_id'
#             new_value = new_model
#         ns = ArgparseNamespace()
#
#         att_update_main(ns)
#         self.test_file.refresh_from_db()
#         self.assertEqual(self.test_file.climate_model.short_name, new_model)
#
#     @mock.patch('scripts.attribute_update.logger')
#     def test_some_other_option(self, mock_logger):
#
#         class ArgparseNamespace(object):
#             file_path = ('/gws/nopw/j04/primavera9/stream1/path/var_table_'
#                          'model_expt_varlab_gn_1-2.nc')
#             attribute_name = 'other-option'
#             new_value = 'new-model'
#         ns = ArgparseNamespace()
#
#         self.assertRaises(SystemExit, att_update_main, ns)


def _make_files_realistic():
    """
    Update the files in the database created by make_example_files() to make
    them realistic enough to run these tests.
    """
    datafile = DataFile.objects.get(name='test1')
    datafile.name = 'var1_table_model_expt_varlab_gn_1-2.nc'
    datafile.directory = '/gws/nopw/j04/primavera9/stream1/path'
    datafile.save()
