"""
This module is for initializing the analysis database
"""

from metadatastore.mds import MDS  # "metadata store read and write"
from filestore.fs import FileStore  # "file store read and write"
from databroker import Broker
from uuid import uuid4

# This an example. You'll need to know your local configuration.
mds = MDS({'host': 'localhost',
             'port': 27017,
             'database': 'metadatastore-production-v1',
             'timezone': 'US/Eastern'})

# This an example. You'll need to know your local configuration.
fs = FileStore({'host': 'localhost',
                  'port': 27017,
                  'database': 'filestore-production-v1'})

db = Broker(mds, fs)


# TODO: smarter person(s) than me should split this into two decorators
def mds_fs_dec(data_names, data_sub_keys, save_func=None, save_loc=None, spec=None, fs_kwargs={}, **kwargs):
    """
    Decorator for saving analysis outputs to MDS/FS

    Parameters
    ----------
    data_names: list of str
        The names for each data
    data_sub_keys: list of dicts
        The list of data_keys
    save_func: function or list of functions
        The function used to save the raw data for each data key there must be
        a function in the list. Must be the same length as the return of the
        decorated function.
    save_loc: str
        The save location for the data on disk
    spec: str
        The FS spec string
    fs_kwargs:
        kwargs passed to fs.insert_datum

    Returns
    -------
    stuff

    Notes
    -----
    The way I see this there are 3 possible outcomes where data could be
    analyzed and then "saved".
    1. The data is generated and then saved on disk because it is huge, it is
    then inserted into FS. This translates to, `data_keys` has a
    `external='Filestore:` key-value pair so we should return the uid
    2. The data is generated and then saved in MDS because it is not huge.
    This translates to there is no `external` for this data key and no `save_func`,
     just store the python object directly, we should return the python object
     itself.
    3. The data already exists in FS, we just want to link it to some other
    data. Translation, there is (maybe) no `save_func` (or is there a dummy
    save func which hands back the uid) but we still need to give back
    the FS uid, because there is an `external` key.
    """
    def wrap(f):
        def wrapper(*args, **kwargs):
            data_keys = {k: v for k, v in zip(data_names, data_sub_keys)}
            # Run the function
            a = f(*args, **kwargs) # snowflake retrieval/processing gen here
            for outs in a:
                returns = []
                for b, s in zip(outs, save_func):
                    if s is None:
                        returns.append(b)
                    uid = str(uuid4())
                    # make save name
                    save_name = save_loc+uid
                    # Save using the save function
                    s(b, save_name)
                    # Insert into FS
                    uid = str(uuid4())
                    fs_res = fs.insert_resource(spec, save_name)
                    fs.insert_datum(fs_res, uid, fs_kwargs)
                # TODO: need to unpack a little better
                yield returns, data_names, data_keys, outs
        return wrapper
    return wrap

# Deltete this when the main decorator runs, we know this one works kinda
# def d(data_names, data_keys, save_func=None, save_loc='.'):
#     def wrap(f):
#         def wrapper(*args, **kwargs):
#             print('prep for MDS')
#             print(data_keys)
#             print('insert logic to get a generator')
#             generator = [1, 2, 3]
#             for h in generator:
#                 a = f(h, *args, **kwargs)
#                 print('save data at {} using {}'.format(save_loc, save_func))
#                 print('insert data into FS')
#                 uid = str(uuid4())
#                 yield uid, data_names, data_keys, a
#         return wrapper
#     return wrap