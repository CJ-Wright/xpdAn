"""
This module is for initializing the analysis database
"""

from metadatastore.mds import MDS  # "metadata store read and write"
from filestore.fs import FileStore  # "file store read and write"
from filestore.utils import install_sentinels
from databroker import Broker
import tempfile
from filestore.handlers import NpyHandler

# This an example. You'll need to know your local configuration.
mds = MDS({'host': 'localhost',
           'port': 27017,
           'database': 'metadatastore-analysis-alpha',
           'timezone': 'US/Eastern',
           # 'mongo_user': 'tom',
           # 'mongo_pwd': 'jerry'
           })
# This an example. You'll need to know your local configuration.
fs = FileStore({'host': 'localhost',
                'port': 27017,
                'database': 'filestore-analysis-alpha',
                # 'mongo_user':'tom',
                # 'mongo_pwd':'jerry'
                })
try:
    install_sentinels(fs.config, 1)
except (RuntimeError, AttributeError):
    pass
fs.register_handler('npy', NpyHandler)
analysis_db = Broker(mds, fs)

save_loc = tempfile.mkdtemp()
