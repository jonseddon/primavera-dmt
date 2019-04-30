#!/usr/bin/env python
"""
update_dreqs_0155.py

This file creates variable and data requests for CERFACS' contributions to WP5.
"""
from __future__ import (unicode_literals, division, absolute_import,
                        print_function)
import argparse
from datetime import datetime
import logging.config
import sys

from cf_units import date2num, CALENDAR_GREGORIAN

import django
django.setup()

from pdata_app.models import (DataRequest, VariableRequest, Experiment,
                              Institute, ClimateModel, Project, Settings,
                              ActivityId)
from pdata_app.utils.dbapi import match_one, get_or_create


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
    new_vreqs = [
        {'table_name': '6hrPlev',
         'long_name': 'Precipitation',
         'units': 'kg m-2 s-1',
         'var_name': 'pr',
         'standard_name': 'precipitation_flux',
         'cell_methods': 'area: time: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time',
         'cmor_name': 'pr',
         'modeling_realm': 'atmos',
         'frequency': '6hr',
         'cell_measures': 'area: areacella',
         'uid': '91044b3e-267c-11e7-8933-ac72891c3257'},
        {'table_name': '6hrPlev',
         'long_name': 'Geopotential Height at 1000 hPa',
         'units': 'm',
         'var_name': 'zg1000',
         'standard_name': 'geopotential_height',
         'cell_methods': 'area: time: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time p1000',
         'cmor_name': 'zg1000',
         'modeling_realm': 'atmos',
         'frequency': '6hr',
         'cell_measures': 'area: areacella',
         'uid': '8b920734-4a5b-11e6-9cd2-ac72891c3257'},
        {'table_name': '6hrPlev',
         'long_name': 'Near-Surface Air Temperature',
         'units': 'K',
         'var_name': 'tas',
         'standard_name': 'air_temperature',
         'cell_methods': 'area: time: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time height2m',
         'cmor_name': 'tas',
         'modeling_realm': 'atmos',
         'frequency': '6hr',
         'cell_measures': 'area: areacella',
         'uid': '91043914-267c-11e7-8933-ac72891c3257'},
        {'table_name': '6hrPlev',
         'long_name': 'Eastward Near-Surface Wind',
         'units': 'm s-1',
         'var_name': 'uas',
         'standard_name': 'eastward_wind',
         'cell_methods': 'area: time: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time height10m',
         'cmor_name': 'uas',
         'modeling_realm': 'atmos',
         'frequency': '6hr',
         'cell_measures': 'area: areacella',
         'uid': '91043e32-267c-11e7-8933-ac72891c3257'},
        {'table_name': '6hrPlev',
         'long_name': 'Northward Near-Surface Wind',
         'units': 'm s-1',
         'var_name': 'vas',
         'standard_name': 'northward_wind',
         'cell_methods': 'area: time: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time height10m',
         'cmor_name': 'vas',
         'modeling_realm': 'atmos',
         'frequency': '6hr',
         'cell_measures': 'area: areacella',
         'uid': '940ff494-4798-11e7-b16a-ac72891c3257'},
        {'table_name': 'AERday',
         'long_name': 'Geopotential Height at 500 hPa',
         'units': 'm',
         'var_name': 'zg500',
         'standard_name': 'geopotential_height',
         'cell_methods': 'area: time: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time p500',
         'cmor_name': 'zg500',
         'modeling_realm': 'aerosol',
         'frequency': 'day',
         'cell_measures': 'area: areacella',
         'uid': '0fabb742-817d-11e6-b80b-5404a60d96b5'},
        {'table_name': 'Eday',
         'long_name': 'Ocean Mixed Layer Thickness Defined by Sigma T',
         'units': 'm',
         'var_name': 'mlotst',
         'standard_name': 'ocean_mixed_layer_thickness_defined_by_sigma_t',
         'cell_methods': 'area: time: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time',
         'cmor_name': 'mlotst',
         'modeling_realm': 'ocean',
         'frequency': 'day',
         'cell_measures': 'area: areacella',
         'uid': '8168b848-f906-11e6-a176-5404a60d96b5'},
        {'table_name': 'Eday',
         'long_name': '20C isotherm depth',
         'units': 'm',
         'var_name': 't20d',
         'standard_name': 'depth_of_isosurface_of_sea_water_potential_temperature',
         'cell_methods': 'area: time: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time',
         'cmor_name': 't20d',
         'modeling_realm': 'ocean',
         'frequency': 'day',
         'cell_measures': 'area: areacella',
         'uid': '8b927340-4a5b-11e6-9cd2-ac72891c3257'},
        {'table_name': 'Eday',
         'long_name': 'Air Temperature',
         'units': 'K',
         'var_name': 'ta850',
         'standard_name': 'air_temperature',
         'cell_methods': 'area: time: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time p850',
         'cmor_name': 'ta850',
         'modeling_realm': 'atmos',
         'frequency': 'day',
         'cell_measures': 'area: areacella',
         'uid': '8b91f2e4-4a5b-11e6-9cd2-ac72891c3257'},
        {'table_name': 'Efx',
         'long_name': 'Grounded Ice Sheet  Area Fraction',
         'units': '%',
         'var_name': 'sftgrf',
         'standard_name': 'grounded_ice_sheet_area_fraction',
         'cell_methods': 'area: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude  typegis',
         'cmor_name': 'sftgrf',
         'modeling_realm': 'landIce',
         'frequency': 'fx',
         'cell_measures': 'area: areacella',
         'uid': 'b7f330ce-7c00-11e6-bcdf-ac72891c3257'},
        {'table_name': 'Emon',
         'long_name': 'surface altitude',
         'units': 'm',
         'var_name': 'orog',
         'standard_name': 'surface_altitude',
         'cell_methods': 'area: time: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time',
         'cmor_name': 'orog',
         'modeling_realm': 'land',
         'frequency': 'mon',
         'cell_measures': 'area: areacella',
         'uid': '7d91ee42-1ab7-11e7-8dfc-5404a60d96b5'},
        {'table_name': 'Emon',
         'long_name': 'Net Longwave Surface Radiation',
         'units': 'W m-2',
         'var_name': 'rls',
         'standard_name': 'surface_net_downward_longwave_flux',
         'cell_methods': 'area: time: mean',
         'positive': 'up',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time',
         'cmor_name': 'rls',
         'modeling_realm': 'atmos',
         'frequency': 'mon',
         'cell_measures': 'area: areacella',
         'uid': '8b922368-4a5b-11e6-9cd2-ac72891c3257'},
        {'table_name': 'Emon',
         'long_name': 'Net Shortwave Surface Radiation',
         'units': 'W m-2',
         'var_name': 'rss',
         'standard_name': 'surface_net_downward_shortwave_flux',
         'cell_methods': 'area: time: mean',
         'positive': 'down',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time',
         'cmor_name': 'rss',
         'modeling_realm': 'atmos',
         'frequency': 'mon',
         'cell_measures': 'area: areacella',
         'uid': '6f68f91c-9acb-11e6-b7ee-ac72891c3257'},
        {'table_name': 'Emon',
         'long_name': 'Vertically Averaged Sea Water Potential Temperature',
         'units': 'degC',
         'var_name': 'thetaot',
         'standard_name': 'sea_water_potential_temperature',
         'cell_methods': 'area: mean where sea time: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time',
         'cmor_name': 'thetaot',
         'modeling_realm': 'ocean',
         'frequency': 'mon',
         'cell_measures': 'area: areacello',
         'uid': '6f69f1b4-9acb-11e6-b7ee-ac72891c3257'},
        {'table_name': 'Emon',
         'long_name': 'Depth average potential temperature of upper 300m',
         'units': 'degC',
         'var_name': 'thetaot300',
         'standard_name': 'sea_water_potential_temperature',
         'cell_methods': 'area: time: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time depth300m',
         'cmor_name': 'thetaot300',
         'modeling_realm': 'ocean',
         'frequency': 'mon',
         'cell_measures': 'area: areacella',
         'uid': '8b92450a-4a5b-11e6-9cd2-ac72891c3257'},
        {'table_name': 'Emon',
         'long_name': 'Depth average potential temperature of upper 700m',
         'units': 'degC',
         'var_name': 'thetaot700',
         'standard_name': 'sea_water_potential_temperature',
         'cell_methods': 'area: time: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time depth700m',
         'cmor_name': 'thetaot700',
         'modeling_realm': 'ocean',
         'frequency': 'mon',
         'cell_measures': 'area: areacella',
         'uid': '8b924a46-4a5b-11e6-9cd2-ac72891c3257'},
        {'table_name': 'Emon',
         'long_name': 'Depth average potential temperature of upper 2000m',
         'units': 'degC',
         'var_name': 'thetaot2000',
         'standard_name': 'sea_water_potential_temperature',
         'cell_methods': 'area: time: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time depth2000m',
         'cmor_name': 'thetaot2000',
         'modeling_realm': 'ocean',
         'frequency': 'mon',
         'cell_measures': 'area: areacella',
         'uid': '8b924fa0-4a5b-11e6-9cd2-ac72891c3257'},
        {'table_name': 'Ofx',
         'long_name': 'Sea Floor Depth Below Geoid',
         'units': 'm',
         'var_name': 'deptho',
         'standard_name': 'sea_floor_depth_below_geoid',
         'cell_methods': 'area: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude ',
         'cmor_name': 'deptho',
         'modeling_realm': 'ocean',
         'frequency': 'fx',
         'cell_measures': 'area: areacello',
         'uid': 'baa3e4d0-e5dd-11e5-8482-ac72891c3257'},
        {'table_name': 'Ofx',
         'long_name': 'Ocean Grid-Cell Mass per area',
         'units': 'kg m-2',
         'var_name': 'masscello',
         'standard_name': 'sea_water_mass_per_unit_area',
         'cell_methods': 'area: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude olevel ',
         'cmor_name': 'masscello',
         'modeling_realm': 'ocean',
         'frequency': 'fx',
         'cell_measures': 'area: areacello volume: volcello',
         'uid': 'baa3ea2a-e5dd-11e5-8482-ac72891c3257'},
        {'table_name': 'Ofx',
         'long_name': 'Upward Geothermal Heat Flux at Sea Floor',
         'units': 'W m-2',
         'var_name': 'hfgeou',
         'standard_name': 'upward_geothermal_heat_flux_at_sea_floor',
         'cell_methods': 'area: mean',
         'positive': 'up',
         'variable_type': 'real',
         'dimensions': 'longitude latitude ',
         'cmor_name': 'hfgeou',
         'modeling_realm': 'ocean',
         'frequency': 'fx',
         'cell_measures': 'area: areacello',
         'uid': 'baa3fb50-e5dd-11e5-8482-ac72891c3257'},
        {'table_name': 'SIday',
         'long_name': 'Sea-ice area fraction',
         'units': '%',
         'var_name': 'siconca',
         'standard_name': 'sea_ice_area_fraction',
         'cell_methods': 'area: time: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time typesi',
         'cmor_name': 'siconca',
         'modeling_realm': 'seaIce',
         'frequency': 'day',
         'cell_measures': 'area: areacella',
         'uid': 'd243b4a4-4a9f-11e6-b84e-ac72891c3257'},
        {'table_name': 'SIday',
         'long_name': 'Fraction of time steps with sea ice',
         'units': '1',
         'var_name': 'sitimefrac',
         'standard_name': 'sea_ice_time_fraction',
         'cell_methods': 'area: time: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time',
         'cmor_name': 'sitimefrac',
         'modeling_realm': 'seaIce',
         'frequency': 'day',
         'cell_measures': 'area: areacella',
         'uid': 'd243af0e-4a9f-11e6-b84e-ac72891c3257'},
        {'table_name': 'fx',
         'long_name': 'Grid-Cell Area for River Model Variables',
         'units': 'm2',
         'var_name': 'areacellr',
         'standard_name': 'cell_area',
         'cell_methods': 'area: sum',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude ',
         'cmor_name': 'areacellr',
         'modeling_realm': 'land',
         'frequency': 'fx',
         'cell_measures': '',
         'uid': '8306180c-76ca-11e7-ba39-ac72891c3257'},
        {'table_name': 'fx',
         'long_name': 'Land Ice Area Fraction',
         'units': '%',
         'var_name': 'sftgif',
         'standard_name': 'land_ice_area_fraction',
         'cell_methods': 'area: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude  typeli',
         'cmor_name': 'sftgif',
         'modeling_realm': 'land',
         'frequency': 'fx',
         'cell_measures': 'area: areacella',
         'uid': 'bab73a76-e5dd-11e5-8482-ac72891c3257'},
        {'table_name': 'fx',
         'long_name': 'Capacity of Soil to Store Water (Field Capacity)',
         'units': 'kg m-2',
         'var_name': 'mrsofc',
         'standard_name': 'soil_moisture_content_at_field_capacity',
         'cell_methods': 'area: mean where land',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude ',
         'cmor_name': 'mrsofc',
         'modeling_realm': 'land',
         'frequency': 'fx',
         'cell_measures': 'area: areacella',
         'uid': 'bab1c08c-e5dd-11e5-8482-ac72891c3257'},
        {'table_name': 'fx',
         'long_name': 'Maximum Root Depth',
         'units': 'm',
         'var_name': 'rootd',
         'standard_name': 'root_depth',
         'cell_methods': 'area: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude ',
         'cmor_name': 'rootd',
         'modeling_realm': 'land',
         'frequency': 'fx',
         'cell_measures': 'area: areacella',
         'uid': 'bab5c7fe-e5dd-11e5-8482-ac72891c3257'},
        {'table_name': 'fx',
         'long_name': 'Altitude of Model Full-Levels',
         'units': 'm',
         'var_name': 'zfull',
         'standard_name': 'height_above_reference_ellipsoid',
         'cell_methods': 'area: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude alevel ',
         'cmor_name': 'zfull',
         'modeling_realm': 'atmos',
         'frequency': 'fx',
         'cell_measures': 'area: areacella',
         'uid': '0ea7a738776ef049ed7bef9c701a819c8c9ca036'},

    ]

    # create the variable requests
    for new_vreq in new_vreqs:
        _vr = get_or_create(VariableRequest, **new_vreq)

    activity_id = 'primWP5'

    new_dreqs = [
        'uas_3hr',
        'vas_3hr',
        'pr_6hrPlev',
        'psl_6hrPlev',
        'tas_6hrPlev',
        'uas_6hrPlev',
        'vas_6hrPlev',
        'zg1000_6hrPlev',
        'ta_6hrPlevPt',
        'ua_6hrPlevPt',
        'va_6hrPlevPt',
        'zg27_6hrPlevPt',
        'zg500_AERday',
        'ptp_AERmon',
        'rlutaf_AERmon',
        'rlutcsaf_AERmon',
        'rsutaf_AERmon',
        'rsutcsaf_AERmon',
        'toz_AERmon',
        'ztp_AERmon',
        'clivi_Amon',
        'clt_Amon',
        'clwvi_Amon',
        'evspsbl_Amon',
        'hfls_Amon',
        'hfss_Amon',
        'hur_Amon',
        'hurs_Amon',
        'hus_Amon',
        'huss_Amon',
        'o3_Amon',
        'pr_Amon',
        'prc_Amon',
        'prsn_Amon',
        'prw_Amon',
        'ps_Amon',
        'psl_Amon',
        'rlds_Amon',
        'rldscs_Amon',
        'rlus_Amon',
        'rlut_Amon',
        'rlutcs_Amon',
        'rsds_Amon',
        'rsdscs_Amon',
        'rsdt_Amon',
        'rsus_Amon',
        'rsuscs_Amon',
        'rsut_Amon',
        'rsutcs_Amon',
        'sbl_Amon',
        'sfcWind_Amon',
        'ta_Amon',
        'tas_Amon',
        'tasmax_Amon',
        'tasmin_Amon',
        'tauu_Amon',
        'tauv_Amon',
        'ts_Amon',
        'ua_Amon',
        'uas_Amon',
        'va_Amon',
        'vas_Amon',
        'wap_Amon',
        'zg_Amon',
        'ps_CFday',
        'mlotst_Eday',
        'rivo_Eday',
        't20d_Eday',
        'ta850_Eday',
        'ts_Eday',
        'sftgrf_Efx',
        'cropFracC3_Emon',
        'cropFracC4_Emon',
        'evspsblpot_Emon',
        'grassFracC3_Emon',
        'grassFracC4_Emon',
        'intuadse_Emon',
        'intuaw_Emon',
        'intvadse_Emon',
        'intvaw_Emon',
        'mrtws_Emon',
        'nwdFracLut_Emon',
        'orog_Emon',
        'rls_Emon',
        'rss_Emon',
        'sfcWindmax_Emon',
        't20d_Emon',
        'thetaot_Emon',
        'thetaot2000_Emon',
        'thetaot300_Emon',
        'thetaot700_Emon',
        'vegFrac_Emon',
        'wtd_Emon',
        'lwsnl_LImon',
        'sftgif_LImon',
        'sftgrf_LImon',
        'snc_LImon',
        'snd_LImon',
        'snw_LImon',
        'baresoilFrac_Lmon',
        'c3PftFrac_Lmon',
        'c4PftFrac_Lmon',
        'cropFrac_Lmon',
        'gpp_Lmon',
        'grassFrac_Lmon',
        'lai_Lmon',
        'mrfso_Lmon',
        'mrro_Lmon',
        'mrros_Lmon',
        'mrso_Lmon',
        'mrsos_Lmon',
        'npp_Lmon',
        'prveg_Lmon',
        'ra_Lmon',
        'rh_Lmon',
        'treeFrac_Lmon',
        'tsl_Lmon',
        'sos_Oday',
        'tos_Oday',
        'areacello_Ofx',
        'basin_Ofx',
        'deptho_Ofx',
        'hfgeou_Ofx',
        'masscello_Ofx',
        'thkcello_Ofx',
        'bigthetao_Omon',
        'bigthetaoga_Omon',
        'ficeberg_Omon',
        'friver_Omon',
        'hfbasin_Omon',
        'hfcorr_Omon',
        'hfds_Omon',
        'hfx_Omon',
        'hfy_Omon',
        'htovgyre_Omon',
        'htovovrt_Omon',
        'mfo_Omon',
        'mlotst_Omon',
        'mlotstmin_Omon',
        'msftyz_Omon',
        'rsntds_Omon',
        'so_Omon',
        'soga_Omon',
        'sos_Omon',
        'sosga_Omon',
        'tauuo_Omon',
        'tauvo_Omon',
        'thetao_Omon',
        'thkcello_Omon',
        'tos_Omon',
        'umo_Omon',
        'uo_Omon',
        'vmo_Omon',
        'vo_Omon',
        'volo_Omon',
        'wfo_Omon',
        'wmo_Omon',
        'wo_Omon',
        'zos_Omon',
        'zossq_Omon',
        'zostoga_Omon',
        'rsds_Prim6hr',
        'rsdsdiff_Prim6hr',
        'mrso_Primday',
        'siconc_SIday',
        'siconca_SIday',
        'sisnthick_SIday',
        'sispeed_SIday',
        'sitemptop_SIday',
        'sithick_SIday',
        'sitimefrac_SIday',
        'siu_SIday',
        'siv_SIday',
        'siage_SImon',
        'siareaacrossline_SImon',
        'siarean_SImon',
        'siareas_SImon',
        'sicompstren_SImon',
        'siconc_SImon',
        'siconca_SImon',
        'sidconcdyn_SImon',
        'sidconcth_SImon',
        'sidivvel_SImon',
        'sidmassdyn_SImon',
        'sidmassevapsubl_SImon',
        'sidmassgrowthbot_SImon',
        'sidmassgrowthwat_SImon',
        'sidmasslat_SImon',
        'sidmassmeltbot_SImon',
        'sidmassmelttop_SImon',
        'sidmasssi_SImon',
        'sidmassth_SImon',
        'sidmasstranx_SImon',
        'sidmasstrany_SImon',
        'siextentn_SImon',
        'siextents_SImon',
        'sifb_SImon',
        'siflcondbot_SImon',
        'siflcondtop_SImon',
        'siflfwbot_SImon',
        'siflfwdrain_SImon',
        'sifllatstop_SImon',
        'sifllwutop_SImon',
        'siflsensupbot_SImon',
        'siflswdbot_SImon',
        'siflswdtop_SImon',
        'siflswutop_SImon',
        'sihc_SImon',
        'simass_SImon',
        'simassacrossline_SImon',
        'sipr_SImon',
        'sisaltmass_SImon',
        'sishevel_SImon',
        'sisnconc_SImon',
        'sisnhc_SImon',
        'sisnmass_SImon',
        'sisnthick_SImon',
        'sispeed_SImon',
        'sistrxdtop_SImon',
        'sistrxubot_SImon',
        'sistrydtop_SImon',
        'sistryubot_SImon',
        'sitempbot_SImon',
        'sitempsnic_SImon',
        'sitemptop_SImon',
        'sithick_SImon',
        'sitimefrac_SImon',
        'siu_SImon',
        'siv_SImon',
        'sivol_SImon',
        'sivoln_SImon',
        'sivols_SImon',
        'sndmassdyn_SImon',
        'sndmassmelt_SImon',
        'sndmasssi_SImon',
        'sndmasssnf_SImon',
        'sndmasssubl_SImon',
        'snmassacrossline_SImon',
        'clt_day',
        'hfls_day',
        'hfss_day',
        'huss_day',
        'pr_day',
        'psl_day',
        'rlut_day',
        'rsds_day',
        'sfcWindmax_day',
        'snc_day',
        'ta_day',
        'tas_day',
        'tasmax_day',
        'tasmin_day',
        'ua_day',
        'uas_day',
        'va_day',
        'vas_day',
        'zg_day',
        'areacella_fx',
        'areacellr_fx',
        'mrsofc_fx',
        'orog_fx',
        'rootd_fx',
        'sftgif_fx',
        'sftlf_fx',
        'zfull_fx',
    ]

    institute_details = {
        'id': 'CNRM-CERFACS',
        'model_ids': ['CNRM-CM6-1-HR', 'CNRM-CM6-1'],
        'calendar': CALENDAR_GREGORIAN
    }

    experiments = {
        'primWP5-amv-neg': {'start_date': datetime(1950, 1, 1),
                         'end_date': datetime(1960, 1, 1)},
        'primWP5-amv-pos': {'start_date': datetime(1950, 1, 1),
                           'end_date': datetime(1960, 1, 1)}
    }

    variant_labels = ['r{}i1p2f1'.format(i) for i in range(1, 11)]

    # activity_id
    ActivityId.objects.get_or_create(short_name=activity_id,
                                     full_name=activity_id)

    # Experiment cache
    experiment_objs = []
    for expt in experiments:
        expt_obj = match_one(Experiment, short_name=expt)
        if expt_obj:
            experiment_objs.append(expt_obj)
        else:
            msg = 'experiment {} not found in the database.'.format(expt)
            print(msg)
            raise ValueError(msg)

    # Institute
    result = match_one(Institute, short_name=institute_details['id'])
    if result:
        institute = result
    else:
        msg = 'institute_id {} not found in the database.'.format(
            institute_details['id']
        )
        print(msg)
        raise ValueError(msg)

    # Look up the ClimateModel object for each institute_id  and save the
    # results to a dictionary for quick look up later
    model_objs = []
    for clim_model in institute_details['model_ids']:
        result = match_one(ClimateModel, short_name=clim_model)
        if result:
            model_objs.append(result)
        else:
            msg = ('climate_model {} not found in the database.'.
                   format(clim_model))
            print(msg)
            raise ValueError(msg)

    # The standard reference time
    std_units = Settings.get_solo().standard_time_units

    # create the new data requests
    for new_dreq in new_dreqs:
        cmor_name, table_name = new_dreq.split('_')
        if table_name.startswith('Prim'):
            project = match_one(Project, short_name='PRIMAVERA')
        else:
            project = match_one(Project, short_name='CMIP6')

        var_req_obj = match_one(VariableRequest, cmor_name=cmor_name,
                                table_name=table_name)
        if var_req_obj:
            for expt in experiment_objs:
                for clim_model in model_objs:
                    for var_lab in variant_labels:
                        _dr = get_or_create(
                            DataRequest,
                            project=project,
                            institute=institute,
                            climate_model=clim_model,
                            experiment=expt,
                            variable_request=var_req_obj,
                            request_start_time=date2num(
                                experiments[expt.short_name]['start_date'],
                                std_units, institute_details['calendar']
                            ),
                            request_end_time=date2num(
                                experiments[expt.short_name]['end_date'],
                                std_units, institute_details['calendar']
                            ),
                            time_units=std_units,
                            calendar=institute_details['calendar'],
                            rip_code = var_lab
                        )
        else:
            msg = ('Unable to find variable request matching '
                   'cmor_name {} and table_name {} in the '
                   'database.'.format(cmor_name, table_name))
            print(msg)
            raise ValueError(msg)


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
