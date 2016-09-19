import shutil
import sys
import uuid

import pytest
from databroker import Broker
from filestore.handlers import NpyHandler

from .utils import insert_imgs
from xpdan.startup.start import save_loc
from filestore.utils import install_sentinels
if sys.version_info >= (3, 0):
    pass


@pytest.fixture(params=[
    # 'sqlite',
    'mongo'], scope='function')
def db(request):
    param_map = {
        # 'sqlite': build_sqlite_backed_broker,
        'mongo': build_pymongo_backed_broker}

    return param_map[request.param](request)


@pytest.fixture(params=[
    # 'sqlite',
    'mongo'], scope='function')
def db_with_imgs(request):
    param_map = {
        # 'sqlite': build_sqlite_backed_broker,
        'mongo': build_pymongo_backed_broker_with_imgs}

    return param_map[request.param](request)


# def build_sqlite_backed_broker(request):
#     """Uses mongoquery + sqlite -- no pymongo or mongo server anywhere"""
#     from portable_mds.sqlite.mds import MDS
#     from portable_fs.sqlite.fs import FileStore
#
#     tempdirname = tempfile.mkdtemp()
#     mds = MDS({'directory': tempdirname,
#                'timezone': tzlocal.get_localzone().zone}, version=1)
#     filenames = ['run_starts.json', 'run_stops.json',
#                  'event_descriptors.json',
#                  'events.json']
#     for fn in filenames:
#         with open(os.path.join(tempdirname, fn), 'w') as f:
#             f.write('[]')
#
#     def delete_mds():
#         shutil.rmtree(tempdirname)
#
#     request.addfinalizer(delete_mds)
#
#     tf = tempfile.NamedTemporaryFile()
#     fs = FileStore({'dbpath': tf.name}, version=1)
#
#     def delete_fs():
#         os.remove(tf.name)
#
#     request.addfinalizer(delete_fs)
#
#     return Broker(mds, fs)


def build_pymongo_backed_broker(request):
    """Provide a function level scoped MDS instance talking to
    temporary database on localhost:27017 with v1 schema.

    """
    from metadatastore.mds import MDS
    from filestore.utils import create_test_database
    from filestore.fs import FileStore

    db_name = "mds_testing_disposable_{}".format(str(uuid.uuid4()))
    mds_test_conf = dict(database=db_name, host='localhost',
                         port=27017, timezone='US/Eastern')
    mds = MDS(mds_test_conf, 1,
              # auth=False
              )

    def delete_mds():
        print("DROPPING DB")
        mds._connection.drop_database(mds_test_conf['database'])

    request.addfinalizer(delete_mds)

    db_name = "fs_testing_base_disposable_{}".format(str(uuid.uuid4()))
    fs_test_conf = create_test_database(host='localhost',
                                        port=27017, version=1,
                                        db_template=db_name)
    fs = FileStore(fs_test_conf, version=1)
    fs.register_handler('npy', NpyHandler)

    def delete_fs():
        print("DROPPING DB")
        fs._connection.drop_database(fs_test_conf['database'])

    request.addfinalizer(delete_fs)

    return Broker(mds, fs)


def build_pymongo_backed_broker_with_imgs(request):
    """Provide a function level scoped MDS instance talking to
    temporary database on localhost:27017 with v1 schema.

    """
    from metadatastore.mds import MDS
    from filestore.utils import create_test_database
    from filestore.fs import FileStore

    db_name = "mds_testing_disposable_{}".format(str(uuid.uuid4()))
    mds_test_conf = dict(database=db_name, host='localhost',
                         port=27017, timezone='US/Eastern')
    mds = MDS(mds_test_conf, 1,
              # auth=False
              )

    def delete_mds():
        print("DROPPING DB")
        mds._connection.drop_database(mds_test_conf['database'])

    request.addfinalizer(delete_mds)

    db_name = "fs_testing_disposable_{}".format(str(uuid.uuid4()))
    fs_test_conf = create_test_database(host='localhost',
                                        port=27017, version=1,
                                        db_template=db_name)
    try:
        install_sentinels(fs_test_conf, 1)
    except (RuntimeError, AttributeError):
        pass
    fs = FileStore(fs_test_conf, version=1)
    fs.register_handler('npy', NpyHandler)

    def delete_fs():
        print("DROPPING DB")
        fs._connection.drop_database(fs_test_conf['database'])

    request.addfinalizer(delete_fs)
    # add images/headers via bluesky run engine
    save_dir = insert_imgs(mds, fs, 5, (200, 200))

    def delete_imgs():
        shutil.rmtree(save_dir)

    request.addfinalizer(delete_imgs)

    def delete_analyized_data():
        shutil.rmtree(save_loc)

    request.addfinalizer(delete_analyized_data)

    return Broker(mds, fs)
