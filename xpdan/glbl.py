##############################################################################
#
# xpdan            by Billinge Group
#                   Simon J. L. Billinge sb2896@columbia.edu
#                   (c) 2016 trustees of Columbia University in the City of
#                        New York.
#                   All rights reserved
#
# File coded by:    Timothy Liu, Christopher J. Wright
#
# See AUTHORS.txt for a list of people who contributed.
# See LICENSE.txt for license information.
#
##############################################################################
import os
import shutil
import tempfile
from time import strftime

import matplotlib
from xpdan.simulation import build_pymongo_backed_broker
from databroker import Broker
import warnings

matplotlib.use('qt4agg')


def build_broker_from_config(mds_name='metadatastore', fs_name='filestore'):
    try:
        import metadatastore.conf
        mds_config = metadatastore.conf.load_configuration(
            mds_name, 'MDS',
            ['host', 'database', 'port', 'timezone'])

        import filestore.conf
        fs_config = filestore.conf.load_configuration(fs_name, 'FS',
                                                      ['host', 'database',
                                                       'port'])

        from filestore.fs import FileStoreRO
        from metadatastore.mds import MDSRO
    except (KeyError, ImportError) as exc:
        warnings.warn(
            "No default DataBroker object will be created because "
            "the necessary configuration was not found: %s" % exc)
    else:
        return Broker(MDSRO(mds_config), FileStoreRO(fs_config))


def make_glbl(env_code=0, db1=None, db2=None):
    """ make a instance of Glbl class

    Glbl class is used to handle attributes and directories
     depends on environment variable

    Parameters
    ----------
    env_code : int
        environment variable to specify current situation

    Note
    ----
    by default: env_var 0 means beamline, 1 means test, 2 means simulation
    """

    HOME_DIR_NAME = 'xpdUser'
    BLCONFIG_DIR_NAME = 'xpdConfig'
    BEAMLINE_HOST_NAME = 'xf28id1-ws2'
    ARCHIVE_BASE_DIR_NAME = '/direct/XF28ID1/pe1_data/.userBeamtimeArchive'
    USER_BACKUP_DIR_NAME = strftime('%Y')
    OWNER = 'xf28id1'
    BEAMLINE_ID = 'xpd'
    GROUP = 'XPD'
    DET_IMAGE_FIELD = 'pe1_image'
    DARK_FIELD_KEY = 'sc_dk_field_uid'
    CALIB_CONFIG_NAME = 'pyFAI_calib.yml'

    # change this to be handled by an environment variable later
    if int(env_code) == 1:
        # test
        BASE_DIR = tempfile.mkdtemp()
        print('creating {}'.format(BASE_DIR))
    elif int(env_code) == 2:
        # simulation
        BASE_DIR = os.getcwd()
        db1 = build_pymongo_backed_broker()
        db2 = build_pymongo_backed_broker()
    else:
        # beamline
        BASE_DIR = os.path.abspath('/direct/XF28ID1/pe2_data')
        db1 = build_broker_from_config()
        db2 = build_broker_from_config('an_metadatastore', 'an_filestore')

    # top directories
    HOME_DIR = os.path.join(BASE_DIR, HOME_DIR_NAME)
    BLCONFIG_DIR = os.path.join(BASE_DIR, BLCONFIG_DIR_NAME)
    ARCHIVE_BASE_DIR = os.path.abspath(ARCHIVE_BASE_DIR_NAME)

    # aquire object directories
    CONFIG_BASE = os.path.join(HOME_DIR, 'config_base')
    # copying pyFAI calib dict yml for test
    # Replace this with a resource
    if int(env_code) == 1:
        a = os.path.dirname(os.path.abspath(__file__))
        b = a.split('glbl.py')[0]
        os.makedirs(CONFIG_BASE, exist_ok=True)
        shutil.copyfile(os.path.join(b, 'tests/pyFAI_calib.yml'),
                        os.path.join(CONFIG_BASE, 'pyFAI_calib.yml'))
    YAML_DIR = os.path.join(HOME_DIR, 'config_base', 'yml')
    BT_DIR = YAML_DIR
    SAMPLE_DIR = os.path.join(YAML_DIR, 'samples')
    EXPERIMENT_DIR = os.path.join(YAML_DIR, 'experiments')
    SCANPLAN_DIR = os.path.join(YAML_DIR, 'scanplans')
    # other dirs
    IMPORT_DIR = os.path.join(HOME_DIR, 'Import')
    ANALYSIS_DIR = os.path.join(HOME_DIR, 'userAnalysis')
    USERSCRIPT_DIR = os.path.join(HOME_DIR, 'userScripts')
    TIFF_BASE = os.path.join(HOME_DIR, 'tiff_base')
    USER_BACKUP_DIR = os.path.join(ARCHIVE_BASE_DIR, USER_BACKUP_DIR_NAME)

    ALL_FOLDERS = [
        HOME_DIR,
        BLCONFIG_DIR,
        YAML_DIR,
        CONFIG_BASE,
        SAMPLE_DIR,
        EXPERIMENT_DIR,
        SCANPLAN_DIR,
        TIFF_BASE,
        USERSCRIPT_DIR,
        IMPORT_DIR,
        ANALYSIS_DIR
    ]

    # only create dirs if running test
    if int(env_code) == 1:
        for folder in ALL_FOLDERS:
            os.makedirs(folder, exist_ok=True)

    # directories that won't be tar in the end of beamtime
    _EXCLUDE_DIR = [HOME_DIR, BLCONFIG_DIR, YAML_DIR]
    glbl = dict(beamline_host_name=BEAMLINE_HOST_NAME,
                base=BASE_DIR,
                home=HOME_DIR,
                _export_tar_dir=[CONFIG_BASE, USERSCRIPT_DIR],
                xpdconfig=BLCONFIG_DIR,
                import_dir=IMPORT_DIR,
                config_base=CONFIG_BASE,
                tiff_base=TIFF_BASE,
                usrScript_dir=USERSCRIPT_DIR,
                usrAnalysis_dir=ANALYSIS_DIR,
                yaml_dir=YAML_DIR,
                bt_dir=BT_DIR,
                sample_dir=SAMPLE_DIR,
                experiment_dir=EXPERIMENT_DIR,
                scanplan_dir=SCANPLAN_DIR,
                allfolders=ALL_FOLDERS,
                archive_dir=USER_BACKUP_DIR,
                owner=OWNER,
                beamline_id=BEAMLINE_ID,
                group=GROUP,
                det_image_field=DET_IMAGE_FIELD,
                dark_field_key=DARK_FIELD_KEY,
                calib_config_name=CALIB_CONFIG_NAME,
                exp_db=db1,
                an_db=db2,
                mask_dict={'edge': 30, 'lower_thresh': 0.0,
                           'upper_thresh': None, 'bs_width': 13,
                           'tri_offset': 13, 'v_asym': 0,
                           'alpha': 2.5, 'tmsk': None},
                _exclude_dir=_EXCLUDE_DIR)

    return glbl


try:
    env_code = os.environ['XPDAN_SETUP']
except KeyError:
    env_code = 1
print('ENV_CODE = {}'.format(env_code))
an_glbl = make_glbl(env_code)
