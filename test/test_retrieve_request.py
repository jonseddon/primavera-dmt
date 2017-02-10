"""
test_validate_data_submission.py - unit tests for validate_data_submission.py
"""
from django.test import TestCase

from scripts.retrieve_request import _check_same_gws


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
