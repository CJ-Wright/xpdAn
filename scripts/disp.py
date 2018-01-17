import os
from tempfile import TemporaryDirectory

import tzlocal
import zmq.asyncio as zmq_asyncio
from bluesky.callbacks.zmq import RemoteDispatcher
from bluesky.utils import install_qt_kicker
from shed.event_streams import istar

from databroker.assets.handlers import AreaDetectorTiffHandler
# pull from local data, not needed at beamline
from databroker.assets.sqlite import RegistryRO
from databroker.broker import Broker
from databroker.headersource.sqlite import MDSRO
from xpdan.pipelines.main import (raw_source, filler, bg_query,
                                  bg_dark_query, fg_dark_query)

# from xpdan.tools import better_mask_img

d = {'directory': '/home/christopher/live_demo_data',
     'timezone': tzlocal.get_localzone().zone,
     'dbpath': os.path.join('/home/christopher/live_demo_data', 'filestore')}
mds = MDSRO(d)
fs = RegistryRO(d)
fs.register_handler('AD_TIFF', AreaDetectorTiffHandler)
db = Broker(mds=mds, reg=fs)
td = TemporaryDirectory()


filler.db = db
bg_query.kwargs['db'] = db
bg_dark_query.kwargs['db'] = db
fg_dark_query.kwargs['db'] = db

loop = zmq_asyncio.ZMQEventLoop()
install_qt_kicker(loop=loop)

disp = RemoteDispatcher('127.0.0.1:5568', loop=loop)
disp.subscribe(istar(raw_source.emit))
print("REMOTE IS READY TO START")
disp.start()
