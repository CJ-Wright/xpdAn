"""Example for XPD data"""
import matplotlib
matplotlib.use('Agg')

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

d = {'directory': '/home/christopher/live_demo_data',
     'timezone': tzlocal.get_localzone().zone,
     'dbpath': os.path.join('/home/christopher/live_demo_data', 'filestore')}
mds = MDSRO(d)
fs = RegistryRO(d)
fs.register_handler('AD_TIFF', AreaDetectorTiffHandler)
db = Broker(mds=mds, reg=fs)
db.prepare_hook = lambda x, y: copy.deepcopy(y)
td = TemporaryDirectory()

for vis in [True, False]:
    for write_to_disk in [True, False]:
        source = conf_main_pipeline(db, td.name,
                                    vis=vis,
                                    write_to_disk=write_to_disk
                                    )
        source.visualize('main_vis={}_write={}.png'.format(vis, write_to_disk))
        plt.close('all')
td.cleanup()
