"""
common.py - several functions that are used throughout the pdata_app
"""
from __future__ import unicode_literals, division, absolute_import
import logging

import django
from pdata_app.models import DataRequest

logger = logging.getLogger(__name__)


def add_data_request(stream_cmpts):
    """
    Using the dictionary of components of the stream name, find the
    corresponding pdata_app.models.DataRequest and add this to the
    dictionary.

    :param dict stream_cmpts: The components of the dataset stream.
    """
    try:
        stream_cmpts['data_req'] = DataRequest.objects.get(
            climate_model__short_name=stream_cmpts['source_id'],
            experiment__short_name=stream_cmpts['experiment_id'],
            rip_code=stream_cmpts['variant_label'],
            variable_request__table_name=stream_cmpts['table_id'],
            variable_request__cmor_name=stream_cmpts['cmor_name']
        )
    except django.core.exceptions.ObjectDoesNotExist:
        msg = ('Cannot find DataRequest with: climate_model__short_name={},'
               'experiment__short_name={}, variant_label={}, '
               'variable_request__table_name={}, '
               'variable_request__cmor_name={}'.format(
            stream_cmpts['source_id'], stream_cmpts['experiment_id'],
            stream_cmpts['variant_label'], stream_cmpts['table_id'],
            stream_cmpts['cmor_name']
        ))
        logger.error(msg)
        raise

    logger.debug('Found data request {}'.format(stream_cmpts['data_req']))


def parse_rose_stream_name(stream_name):
    """
    Convert the Rose stream name given to this ESGF dataset into more useful
    components.....

    :param str stream_name: The Rose stream name given to this ESGF data set.
    :returns: The dataset components from the stream name.
    :rtype: dict
    """
    cmpts = stream_name.split('_')
    cmpt_names = ['source_id', 'experiment_id', 'variant_label',
                  'table_id', 'cmor_name']

    return {cmpt_name: cmpts[index]
            for index, cmpt_name in enumerate(cmpt_names)}
