"""Example for XPD data"""
import os

import matplotlib.pyplot as plt
import tzlocal

from databroker.assets.handlers import AreaDetectorTiffHandler
from databroker.broker import Broker
# pull from local data, not needed at beamline
from databroker.assets.sqlite import RegistryRO
from databroker.headersource.sqlite import MDSRO
from xpdan.pipelines.main import conf_main_pipeline
from tempfile import TemporaryDirectory
import copy

# from xpdan.tools import better_mask_img

d = {'directory': '/home/christopher/live_demo_data',
     'timezone': tzlocal.get_localzone().zone,
     'dbpath': os.path.join('/home/christopher/live_demo_data', 'filestore')}
mds = MDSRO(d)
fs = RegistryRO(d)
fs.register_handler('AD_TIFF', AreaDetectorTiffHandler)
db = Broker(mds=mds, reg=fs)
db.prepare_hook = lambda x, y: copy.deepcopy(y)
td = TemporaryDirectory()


def to_cryostream(x):
    """Make normal data look like cryostat"""
    name, doc = x
    if name == 'start':
        doc['sample_name'] = 'x48'
    if name == 'descriptor':
        doc.update({'configuration':
                        {'cryostat':
                             {'data': {'cryostat_cntrl': 'A',
                                       'cryostat_dead_band': 1,
                                       'cryostat_heater_range': 'MEDIUM',
                                       'cryostat_mode': 'Closed Loop'},
                              'data_keys': {
                                  'cryostat_cntrl': {
                                      'dtype': 'string',
                                      'enum_strs': ['-', 'A',
                                                    'B', 'C',
                                                    'D'],
                                      'lower_ctrl_limit': None,
                                      'shape': [],
                                      'source': 'PV:XF:28IDC_ES1:LS335:{CryoStat}:OUT1:Cntrl',
                                      'units': None,
                                      'upper_ctrl_limit': None},
                                  'cryostat_dead_band': {
                                      'dtype': 'integer',
                                      'shape': [],
                                      'source': 'PY:cryostat._dead_band'},
                                  'cryostat_heater_range': {
                                      'dtype': 'string',
                                      'enum_strs': ['OFF',
                                                    'LOW',
                                                    'MEDIUM',
                                                    'HIGH'],
                                      'lower_ctrl_limit': None,
                                      'shape': [],
                                      'source': 'PV:XF:28IDC_ES1:LS335:{CryoStat}:HTR1:Range',
                                      'units': None,
                                      'upper_ctrl_limit': None},
                                  'cryostat_mode': {
                                      'dtype': 'string',
                                      'enum_strs': ['Off',
                                                    'Closed Loop',
                                                    'Zone',
                                                    'Open Loop'],
                                      'lower_ctrl_limit': None,
                                      'shape': [],
                                      'source': 'PV:XF:28IDC_ES1:LS335:{CryoStat}:OUT1:Mode',
                                      'units': None,
                                      'upper_ctrl_limit': None}},
                              'timestamps': {
                                  'cryostat_cntrl': 1483809695.166514,
                                  'cryostat_dead_band': 1508438314.228604,
                                  'cryostat_heater_range': 1483823485.04882,
                                  'cryostat_mode': 1483798670.414342}},
                         'pe1': {
                             'data': {'pe1_cam_acquire_period': 5.0,
                                      'pe1_cam_acquire_time': 0.1,
                                      'pe1_cam_image_mode': 2,
                                      'pe1_cam_manufacturer': 'Perkin Elmer',
                                      'pe1_cam_model': 'XRD [0820/1620/1621] xN',
                                      'pe1_cam_num_exposures': 2,
                                      'pe1_cam_trigger_mode': 0,
                                      'pe1_images_per_set': 400.0,
                                      'pe1_number_of_sets': 1},
                             'data_keys': {
                                 'pe1_cam_acquire_period': {
                                     'dtype': 'number',
                                     'lower_ctrl_limit': 0.0,
                                     'precision': 3,
                                     'shape': [],
                                     'source': 'PV:XF:28IDC-ES:1{Det:PE1}cam1:AcquirePeriod_RBV',
                                     'units': '',
                                     'upper_ctrl_limit': 0.0},
                                 'pe1_cam_acquire_time': {
                                     'dtype': 'number',
                                     'lower_ctrl_limit': 0.0,
                                     'precision': 3,
                                     'shape': [],
                                     'source': 'PV:XF:28IDC-ES:1{Det:PE1}cam1:AcquireTime_RBV',
                                     'units': '',
                                     'upper_ctrl_limit': 0.0},
                                 'pe1_cam_image_mode': {
                                     'dtype': 'integer',
                                     'enum_strs': ['Single',
                                                   'Multiple',
                                                   'Continuous',
                                                   'Average'],
                                     'lower_ctrl_limit': None,
                                     'shape': [],
                                     'source': 'PV:XF:28IDC-ES:1{Det:PE1}cam1:ImageMode_RBV',
                                     'units': None,
                                     'upper_ctrl_limit': None},
                                 'pe1_cam_manufacturer': {
                                     'dtype': 'string',
                                     'lower_ctrl_limit': None,
                                     'shape': [],
                                     'source': 'PV:XF:28IDC-ES:1{Det:PE1}cam1:Manufacturer_RBV',
                                     'units': None,
                                     'upper_ctrl_limit': None},
                                 'pe1_cam_model': {'dtype': 'string',
                                                   'lower_ctrl_limit': None,
                                                   'shape': [],
                                                   'source': 'PV:XF:28IDC-ES:1{Det:PE1}cam1:Model_RBV',
                                                   'units': None,
                                                   'upper_ctrl_limit': None},
                                 'pe1_cam_num_exposures': {
                                     'dtype': 'integer',
                                     'lower_ctrl_limit': 0,
                                     'shape': [],
                                     'source': 'PV:XF:28IDC-ES:1{Det:PE1}cam1:NumExposures_RBV',
                                     'units': '',
                                     'upper_ctrl_limit': 0},
                                 'pe1_cam_trigger_mode': {
                                     'dtype': 'integer',
                                     'enum_strs': ['Internal',
                                                   'External',
                                                   'Free Running',
                                                   'Soft Trigger'],
                                     'lower_ctrl_limit': None,
                                     'shape': [],
                                     'source': 'PV:XF:28IDC-ES:1{Det:PE1}cam1:TriggerMode_RBV',
                                     'units': None,
                                     'upper_ctrl_limit': None},
                                 'pe1_images_per_set': {
                                     'dtype': 'number',
                                     'shape': [],
                                     'source': 'SIM:pe1_images_per_set'},
                                 'pe1_number_of_sets': {
                                     'dtype': 'number',
                                     'shape': [],
                                     'source': 'SIM:pe1_number_of_sets'}},
                             'timestamps': {
                                 'pe1_cam_acquire_period': 1508444265.806128,
                                 'pe1_cam_acquire_time': 1508444265.80613,
                                 'pe1_cam_image_mode': 1508444265.806241,
                                 'pe1_cam_manufacturer': 1508444265.80611,
                                 'pe1_cam_model': 1508444265.806125,
                                 'pe1_cam_num_exposures': 1508444265.806199,
                                 'pe1_cam_trigger_mode': 1508444266.010907,
                                 'pe1_images_per_set': 1508478013.9327526,
                                 'pe1_number_of_sets': 1508438325.964695}},
                         'ss_stg2_x': {
                             'data': {'ss_stg2_x_acceleration': 0.2,
                                      'ss_stg2_x_motor_egu': 'mm',
                                      'ss_stg2_x_user_offset': -46.1,
                                      'ss_stg2_x_user_offset_dir': 0,
                                      'ss_stg2_x_velocity': 2.5},
                             'data_keys': {
                                 'ss_stg2_x_acceleration': {
                                     'dtype': 'number',
                                     'lower_ctrl_limit': -1e+300,
                                     'precision': 3,
                                     'shape': [],
                                     'source': 'PV:XF:28IDC-ES:1{Stg:Smpl2-Ax:X}Mtr.ACCL',
                                     'units': 'sec',
                                     'upper_ctrl_limit': 1e+300},
                                 'ss_stg2_x_motor_egu': {
                                     'dtype': 'string',
                                     'lower_ctrl_limit': None,
                                     'shape': [],
                                     'source': 'PV:XF:28IDC-ES:1{Stg:Smpl2-Ax:X}Mtr.EGU',
                                     'units': None,
                                     'upper_ctrl_limit': None},
                                 'ss_stg2_x_user_offset': {
                                     'dtype': 'number',
                                     'lower_ctrl_limit': -1e+300,
                                     'precision': 3,
                                     'shape': [],
                                     'source': 'PV:XF:28IDC-ES:1{Stg:Smpl2-Ax:X}Mtr.OFF',
                                     'units': 'mm',
                                     'upper_ctrl_limit': 1e+300},
                                 'ss_stg2_x_user_offset_dir': {
                                     'dtype': 'integer',
                                     'enum_strs': ['Pos', 'Neg'],
                                     'lower_ctrl_limit': None,
                                     'shape': [],
                                     'source': 'PV:XF:28IDC-ES:1{Stg:Smpl2-Ax:X}Mtr.DIR',
                                     'units': None,
                                     'upper_ctrl_limit': None},
                                 'ss_stg2_x_velocity': {
                                     'dtype': 'number',
                                     'lower_ctrl_limit': 0.0,
                                     'precision': 3,
                                     'shape': [],
                                     'source': 'PV:XF:28IDC-ES:1{Stg:Smpl2-Ax:X}Mtr.VELO',
                                     'units': 'mm/sec',
                                     'upper_ctrl_limit': 5.0}},
                             'timestamps': {
                                 'ss_stg2_x_acceleration': 1508478019.195544,
                                 'ss_stg2_x_motor_egu': 1508478019.195544,
                                 'ss_stg2_x_user_offset': 1508478019.195544,
                                 'ss_stg2_x_user_offset_dir': 1508478019.195544,
                                 'ss_stg2_x_velocity': 1508478019.195544}}},
                    'data_keys': {'cryostat_T': {'dtype': 'number',
                                                 'lower_ctrl_limit': 0.0,
                                                 'object_name': 'cryostat',
                                                 'precision': 4,
                                                 'shape': [],
                                                 'source': 'PV:XF:28IDC_ES1:LS335:{CryoStat}:IN1',
                                                 'units': '',
                                                 'upper_ctrl_limit': 0.0},
                                  'cryostat_setpoint': {'dtype': 'number',
                                                        'lower_ctrl_limit': 0.0,
                                                        'object_name': 'cryostat',
                                                        'precision': 4,
                                                        'shape': [],
                                                        'source': 'PV:XF:28IDC_ES1:LS335:{CryoStat}:OUT1:SP_RBV',
                                                        'units': '',
                                                        'upper_ctrl_limit': 0.0},
                                  'pe1_image': {'dtype': 'array',
                                                'external': 'FILESTORE:',
                                                'object_name': 'pe1',
                                                'shape': [2048, 2048, 0],
                                                'source': 'PV:XF:28IDC-ES:1{Det:PE1}'},
                                  'pe1_stats1_total': {'dtype': 'number',
                                                       'lower_ctrl_limit': 0.0,
                                                       'object_name': 'pe1',
                                                       'precision': 0,
                                                       'shape': [],
                                                       'source': 'PV:XF:28IDC-ES:1{Det:PE1}Stats1:Total_RBV',
                                                       'units': '',
                                                       'upper_ctrl_limit': 0.0},
                                  'ss_stg2_x': {'dtype': 'number',
                                                'lower_ctrl_limit': -24.1,
                                                'object_name': 'ss_stg2_x',
                                                'precision': 3,
                                                'shape': [],
                                                'source': 'PV:XF:28IDC-ES:1{Stg:Smpl2-Ax:X}Mtr.RBV',
                                                'units': 'mm',
                                                'upper_ctrl_limit': 24.800000000000004},
                                  'ss_stg2_x_user_setpoint': {
                                      'dtype': 'number',
                                      'lower_ctrl_limit': -24.1,
                                      'object_name': 'ss_stg2_x',
                                      'precision': 3,
                                      'shape': [],
                                      'source': 'PV:XF:28IDC-ES:1{Stg:Smpl2-Ax:X}Mtr.VAL',
                                      'units': 'mm',
                                      'upper_ctrl_limit': 24.800000000000004}},
                    'hints': {'ss_stg2_x': {'fields': ['ss_stg2_x']}},
                    'name': 'primary',
                    'object_keys': {
                        'cryostat': ['cryostat_T', 'cryostat_setpoint'],
                        'pe1': ['pe1_image', 'pe1_stats1_total'],
                        'ss_stg2_x': ['ss_stg2_x', 'ss_stg2_x_user_setpoint']},
                    })
    elif name == 'event':
        doc['data']['cryostat_T'] = doc['data'].pop('temperature')
    return x


vis = True
source = conf_main_pipeline(db, td.name,
                            vis=vis,
                            write_to_disk=True
                            # write_to_disk=False,
                            # verbose=True
                            )
for hdr in list((db[-1],)):
    for e in hdr.documents():
        e = to_cryostream(e)
        if e[0] == 'start':
            e[1].update(composition_string='EuTiO3')
        if e[0] == 'event' and vis:
            plt.pause(.1)
        source.emit(e)
print('clearing tempdir')
td.cleanup()
print('tempdir clear')
if vis:
    plt.show()
    plt.close("all")
