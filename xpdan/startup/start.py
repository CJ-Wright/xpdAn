"""
This module is for initializing the analysis database
"""

from metadatastore.mds import MDS  # "metadata store read and write"
from filestore.fs import FileStore  # "file store read and write"
from databroker import Broker

# This an example. You'll need to know your local configuration.
mds = MDS({'host': 'localhost',
           'port': 27017,
           'database': 'metadatastore-analysis-alpha',
           'timezone': 'US/Eastern',
           'mongo_user': 'tom',
           'mongo_pwd': 'jerry'})

# This an example. You'll need to know your local configuration.
fs = FileStore({'host': 'localhost',
                'port': 27017,
                'database': 'filestore-analysis-alpha',
                'mongo_user':'tom',
                'mongo_pwd':'jerry'})

db = Broker(mds, fs)

# Delete this when the main decorator runs, we know this one works kinda
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
