"""
This module is for initiallizing the analysis database
"""

from metadatastore.mds import MDS  # "metadata store read and write"
from filestore.fs import FileStore  # "file store read and write"
from databroker import Broker

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


def analysis_event_handeling(f, data_dict):
    def wrapper():
