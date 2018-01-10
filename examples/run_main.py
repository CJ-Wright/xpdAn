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
from xpdan.pipelines.main2 import raw_source, filler, bg_query, bg_dark_query, \
    fg_dark_query

filler.db = db
bg_query.kwargs['db'] = db
bg_dark_query.kwargs['db'] = db
fg_dark_query.kwargs['db'] = db
for hdr in list((db[-1], )):
    for e in hdr.documents():
        if e[0] == 'start':
            e[1].update(composition_string='EuTiO3')
        if e[0] == 'event':
            plt.pause(.1)
        if e[0] == 'event':
            print(e[1]['seq_num'])
            if e[1]['seq_num'] > 1:
                # break
                # AAA
                pass
        raw_source.emit(e)


plt.show()
plt.close("all")
# '''
