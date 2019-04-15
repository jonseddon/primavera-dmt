"""
test_esgf_utils.py - unit tests for pdata_app.utils.esgf_utils.py
"""
from __future__ import unicode_literals, division, absolute_import

from django.test import TestCase
from pdata_app.utils.esgf_utils import add_data_request, parse_rose_stream_name
from .test_common import make_example_files


class TestParseRoseStreamName(TestCase):
    def test_string_parsed(self):
        rose_task_name = 'HadGEM3-GC31-LM_highresSST-present_r1i1p1f1_Amon_psl'
        output_dict = parse_rose_stream_name(rose_task_name)
        expected = {
            'source_id': 'HadGEM3-GC31-LM',
            'experiment_id': 'highresSST-present',
            'variant_label': 'r1i1p1f1',
            'table_id': 'Amon',
            'cmor_name': 'psl'
        }
        self.assertDictEqual(output_dict, expected)


class TestAddDataRequest(TestCase):
    def setUp(self):
        make_example_files(self)

    def test_dreq_added(self):
        input_dict = {
            'source_id': 't',
            'experiment_id': 't',
            'variant_label': 'r1i1p1f1',
            'table_id': 'Amon',
            'cmor_name': 'var1'
        }
        expected = input_dict.copy()
        expected['data_req'] = self.dreq1
        add_data_request(input_dict)
        self.assertEqual(input_dict, expected)
