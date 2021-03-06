#!/usr/bin/env python
"""
update_dreqs_0265.py

Remove from disk and update the version of EC-Earth Lmon and LImon datasets
that have been withdrawn and will be resubmitted.
"""
import argparse
import datetime
import logging.config
import sys

import django
django.setup()
from django.contrib.auth.models import User
from pdata_app.models import DataRequest, RetrievalRequest
from pdata_app.utils.common import delete_files

__version__ = '0.1.0b1'

DEFAULT_LOG_LEVEL = logging.WARNING
DEFAULT_LOG_FORMAT = '%(levelname)s: %(message)s'

logger = logging.getLogger(__name__)


def parse_args():
    """
    Parse command-line arguments
    """
    parser = argparse.ArgumentParser(description='Add additional data requests')
    parser.add_argument('-l', '--log-level', help='set logging level to one of '
        'debug, info, warn (the default), or error')
    parser.add_argument('--version', action='version',
        version='%(prog)s {}'.format(__version__))
    args = parser.parse_args()

    return args


def main(args):
    """
    Main entry point
    """
    datasets = [
        'CMIP6.HighResMIP.EC-Earth-Consortium.EC-Earth3P.control-1950.r1i1p2f1.LImon.snd.gr.v20190906',
        'CMIP6.HighResMIP.EC-Earth-Consortium.EC-Earth3P.control-1950.r1i1p2f1.LImon.snm.gr.v20190906',
        'CMIP6.HighResMIP.EC-Earth-Consortium.EC-Earth3P.control-1950.r1i1p2f1.LImon.tsn.gr.v20190906',
        'CMIP6.HighResMIP.EC-Earth-Consortium.EC-Earth3P.control-1950.r1i1p2f1.LImon.hfdsn.gr.v20190906',
        'CMIP6.HighResMIP.EC-Earth-Consortium.EC-Earth3P.control-1950.r1i1p2f1.LImon.lwsnl.gr.v20190906',
        'CMIP6.HighResMIP.EC-Earth-Consortium.EC-Earth3P.control-1950.r1i1p2f1.LImon.sbl.gr.v20190906',
        'CMIP6.HighResMIP.EC-Earth-Consortium.EC-Earth3P.highres-future.r1i1p2f1.LImon.snm.gr.v20190421',
        'CMIP6.HighResMIP.EC-Earth-Consortium.EC-Earth3P.highres-future.r1i1p2f1.LImon.lwsnl.gr.v20190421',
        'CMIP6.HighResMIP.EC-Earth-Consortium.EC-Earth3P.highres-future.r1i1p2f1.LImon.tsn.gr.v20190421',
        'CMIP6.HighResMIP.EC-Earth-Consortium.EC-Earth3P.highres-future.r1i1p2f1.LImon.sbl.gr.v20190421',
        'CMIP6.HighResMIP.EC-Earth-Consortium.EC-Earth3P.highres-future.r1i1p2f1.LImon.hfdsn.gr.v20190421',
        'CMIP6.HighResMIP.EC-Earth-Consortium.EC-Earth3P.highres-future.r1i1p2f1.LImon.snd.gr.v20190421',
        'CMIP6.HighResMIP.EC-Earth-Consortium.EC-Earth3P.highres-future.r1i1p2f1.LImon.snc.gr.v20190421',
        'CMIP6.HighResMIP.EC-Earth-Consortium.EC-Earth3P.highres-future.r1i1p2f1.LImon.snw.gr.v20190421',
        'CMIP6.HighResMIP.EC-Earth-Consortium.EC-Earth3P.hist-1950.r1i1p2f1.LImon.sbl.gr.v20190314',
        'CMIP6.HighResMIP.EC-Earth-Consortium.EC-Earth3P.hist-1950.r1i1p2f1.LImon.snc.gr.v20190314',
        'CMIP6.HighResMIP.EC-Earth-Consortium.EC-Earth3P.hist-1950.r1i1p2f1.LImon.snm.gr.v20190314',
        'CMIP6.HighResMIP.EC-Earth-Consortium.EC-Earth3P.hist-1950.r1i1p2f1.LImon.hfdsn.gr.v20190314',
        'CMIP6.HighResMIP.EC-Earth-Consortium.EC-Earth3P.hist-1950.r1i1p2f1.LImon.snw.gr.v20190314',
        'CMIP6.HighResMIP.EC-Earth-Consortium.EC-Earth3P.hist-1950.r1i1p2f1.LImon.lwsnl.gr.v20190314',
        'CMIP6.HighResMIP.EC-Earth-Consortium.EC-Earth3P.hist-1950.r1i1p2f1.LImon.snd.gr.v20190314',
        'CMIP6.HighResMIP.EC-Earth-Consortium.EC-Earth3P.hist-1950.r1i1p2f1.LImon.tsn.gr.v20190314',
        'CMIP6.HighResMIP.EC-Earth-Consortium.EC-Earth3P.control-1950.r1i1p2f1.Lmon.tsl.gr.v20190906',
        'CMIP6.HighResMIP.EC-Earth-Consortium.EC-Earth3P.control-1950.r1i1p2f1.Lmon.mrsos.gr.v20190906',
        'CMIP6.HighResMIP.EC-Earth-Consortium.EC-Earth3P.control-1950.r1i1p2f1.Lmon.evspsblsoi.gr.v20190906',
        'CMIP6.HighResMIP.EC-Earth-Consortium.EC-Earth3P.control-1950.r1i1p2f1.Lmon.mrro.gr.v20190906',
        'CMIP6.HighResMIP.EC-Earth-Consortium.EC-Earth3P.highres-future.r1i1p2f1.Lmon.tsl.gr.v20190421',
        'CMIP6.HighResMIP.EC-Earth-Consortium.EC-Earth3P.highres-future.r1i1p2f1.Lmon.mrsos.gr.v20190421',
        'CMIP6.HighResMIP.EC-Earth-Consortium.EC-Earth3P.highres-future.r1i1p2f1.Lmon.evspsblsoi.gr.v20190421',
        'CMIP6.HighResMIP.EC-Earth-Consortium.EC-Earth3P.highres-future.r1i1p2f1.Lmon.mrso.gr.v20190421',
        'CMIP6.HighResMIP.EC-Earth-Consortium.EC-Earth3P.highres-future.r1i1p2f1.Lmon.mrro.gr.v20190421',
        'CMIP6.HighResMIP.EC-Earth-Consortium.EC-Earth3P.highres-future.r1i1p2f1.Lmon.mrros.gr.v20190421',
        'CMIP6.HighResMIP.EC-Earth-Consortium.EC-Earth3P.hist-1950.r1i1p2f1.Lmon.mrsos.gr.v20190314',
        'CMIP6.HighResMIP.EC-Earth-Consortium.EC-Earth3P.hist-1950.r1i1p2f1.Lmon.mrro.gr.v20190314',
        'CMIP6.HighResMIP.EC-Earth-Consortium.EC-Earth3P.hist-1950.r1i1p2f1.Lmon.mrso.gr.v20190314',
        'CMIP6.HighResMIP.EC-Earth-Consortium.EC-Earth3P.hist-1950.r1i1p2f1.Lmon.mrros.gr.v20190314',
        'CMIP6.HighResMIP.EC-Earth-Consortium.EC-Earth3P.hist-1950.r1i1p2f1.Lmon.tsl.gr.v20190314',
    ]

    jon = User.objects.get(username='jseddon')
    rr = RetrievalRequest.objects.create(requester=jon, start_year=1948,
                                         end_year=2051)
    time_zone = datetime.timezone(datetime.timedelta())
    rr.date_created = datetime.datetime.utcnow().replace(tzinfo=time_zone)
    rr.save()

    for dataset in datasets:
        cmpts = dataset.split('.')
        dreq = DataRequest.objects.get(
            climate_model__short_name=cmpts[3],
            experiment__short_name=cmpts[4],
            rip_code=cmpts[5],
            variable_request__table_name=cmpts[6],
            variable_request__cmor_name=cmpts[7]
        )

        logger.debug(f'{dreq} {dreq.datafile_set.all().count()} files')

        delete_files(dreq.datafile_set.all(),
                     '/gws/nopw/j04/primavera5/stream1', skip_badc=True)

        dreq.datafile_set.update(version='v20200206')

        rr.data_request.add(dreq)


if __name__ == "__main__":
    cmd_args = parse_args()

    # determine the log level
    if cmd_args.log_level:
        try:
            log_level = getattr(logging, cmd_args.log_level.upper())
        except AttributeError:
            logger.setLevel(logging.WARNING)
            logger.error('log-level must be one of: debug, info, warn or error')
            sys.exit(1)
    else:
        log_level = DEFAULT_LOG_LEVEL

    # configure the logger
    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'standard': {
                'format': DEFAULT_LOG_FORMAT,
            },
        },
        'handlers': {
            'default': {
                'level': log_level,
                'class': 'logging.StreamHandler',
                'formatter': 'standard'
            },
        },
        'loggers': {
            '': {
                'handlers': ['default'],
                'level': log_level,
                'propagate': True
            }
        }
    })

    # run the code
    main(cmd_args)
