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
import sys

import numpy as np
import pytest

from xpdan.data_reduction import DataReduction
from xpdan.glbl import make_glbl
from xpdan.io import fit2d_save
from xpdan.simulation import build_pymongo_backed_broker
from xpdan.tests.utils import insert_imgs
import tempfile
from xpdan.fuzzybroker import FuzzyBroker
from pkg_resources import resource_filename as rs_fn
import yaml
from uuid import uuid4

pyfai_path = rs_fn('xpdan', 'data/pyfai/pyFAI_calib.yml')

if sys.version_info >= (3, 0):
    pass


@pytest.fixture(params=[
    # 'sqlite',
    'mongo'], scope='module')
def db(request):
    print('Making DB')
    param_map = {
        # 'sqlite': build_sqlite_backed_broker,
        'mongo': build_pymongo_backed_broker}
    databroker = param_map[request.param](request)
    yield databroker
    print("DROPPING MDS")
    databroker.mds._connection.drop_database(databroker.mds.config['database'])
    print("DROPPING FS")
    databroker.fs._connection.drop_database(databroker.fs.config['database'])


@pytest.fixture(scope='module')
def exp_db(db, mk_glbl, img_size, wavelength):
    glbl = mk_glbl
    mds = db.mds
    fs = db.fs
    with open(pyfai_path) as f:
        pyfai_dict = yaml.load(f)
    cal_dict = dict(calibration_md=pyfai_dict,
                    calibration_collection_uid=str(uuid4()),
                    wavelength=wavelength)
    insert_imgs(mds, fs, 5, img_size, glbl.base, bt_safN=0, pi_name='chris',
                **cal_dict)
    insert_imgs(mds, fs, 5, img_size, glbl.base, pi_name='tim', bt_safN=1,
                **cal_dict)
    insert_imgs(mds, fs, 5, img_size, glbl.base, pi_name='chris', bt_safN=2,
                **cal_dict)
    yield db
    print("DROPPING EXP MDS")
    db.mds._connection.drop_database(db.mds.config['database'])
    print("DROPPING EXP FS")
    db.fs._connection.drop_database(db.fs.config['database'])


@pytest.fixture(scope='module')
def fuzzdb(exp_db):
    fb = FuzzyBroker(exp_db.mds, exp_db.fs)
    yield fb
    print("DROPPING FZ MDS")
    fb.mds._connection.drop_database(fb.mds.config['database'])
    print("DROPPING FZ FS")
    fb.fs._connection.drop_database(fb.fs.config['database'])
    print("DROPPING EXP MDS")
    exp_db.mds._connection.drop_database(exp_db.mds.config['database'])
    print("DROPPING EXP FS")
    exp_db.fs._connection.drop_database(exp_db.fs.config['database'])


@pytest.fixture(params=[
    # 'sqlite',
    'mongo'], scope='module')
def an_db(request):
    print('Making DB')
    param_map = {
        # 'sqlite': build_sqlite_backed_broker,
        'mongo': build_pymongo_backed_broker}
    databroker = param_map[request.param](request)
    yield databroker
    print("DROPPING AN MDS")
    databroker.mds._connection.drop_database(databroker.mds.config['database'])
    print("DROPPING AN FS")
    databroker.fs._connection.drop_database(databroker.fs.config['database'])


@pytest.fixture(scope='module')
def handler(exp_db):
    h = DataReduction(exp_db=exp_db)
    yield h
    print("DROPPING EXP MDS")
    exp_db.mds._connection.drop_database(exp_db.mds.config['database'])
    print("DROPPING EXP FS")
    exp_db.fs._connection.drop_database(exp_db.fs.config['database'])


@pytest.fixture(scope='module')
def img_size():
    a = np.random.random_integers(100, 200)
    yield (a, a)


@pytest.fixture(scope='module')
def mk_glbl():
    a = make_glbl(1)
    yield a
    if os.path.exists(a.base):
        print('removing {}'.format(a.base))
        shutil.rmtree(a.base)


@pytest.fixture(scope='module')
def wavelength():
    yield 1.15


@pytest.fixture(scope='module')
def disk_mask(mk_glbl, img_size):
    mask = np.random.random_integers(0, 1, img_size).astype(bool)
    dirn = mk_glbl.base
    file_name_msk = os.path.join(dirn, 'mask_test' + '.msk')
    assert ~os.path.exists(file_name_msk)
    fit2d_save(mask, 'mask_test', dirn)
    assert os.path.exists(file_name_msk)
    file_name = os.path.join(dirn, 'mask_test' + '.npy')
    assert ~os.path.exists(file_name)
    np.save(file_name, mask)
    assert os.path.exists(file_name)
    yield (file_name_msk, file_name, mask)


@pytest.fixture(scope='module')
def tmp_dir():
    td = tempfile.mkdtemp()
    print('creating {}'.format(td))
    yield td
    if os.path.exists(td):
        print('removing {}'.format(td))
        shutil.rmtree(td)
