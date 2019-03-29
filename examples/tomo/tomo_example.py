import os
import time

import bluesky.plans as bp
import dxchange
import numpy as np
import tomopy
from bluesky.run_engine import RunEngine
from ophyd.sim import SynSignal, hw, SynSignalWithRegistry
from xpdan.vend.callbacks.zmq import Publisher
from xpdconf.conf import glbl_dict

# setup the hardware
hw = hw()

m = hw.motor1
m.kind = "hinted"
mm = hw.motor2
mm.kind = "hinted"
mmm = hw.motor3
mmm.kind = "hinted"


# setup run engine
from databroker import Broker
import yaml
dbs = {}
for yaml_file in ['raw', 'an']:
    with open(f'{yaml_file}.yml', 'r') as f:
        dbs[yaml_file] = Broker.from_config(yaml.load(f))

RE = RunEngine()
RE.md['analysis_stage'] = 'raw'
p = Publisher(glbl_dict["inbound_proxy_address"], prefix=b"raw")
RE.subscribe_lossless(dbs['raw'].insert)
t = RE.subscribe(p)

# pull the tooth data
fname = os.path.expanduser("~/Downloads/tooth.h5")

proj, flat, dark, theta = dxchange.read_aps_32id(fname, sino=(0, 1))

# find the center (this is done in a prior scan)
rot_center = tomopy.find_center(proj, theta, init=290, ind=0, tol=0.5)


def f():
    """Full field function"""
    v = m.get()[0]
    out = proj[int(v), :, :]
    out += np.random.random(out.shape)
    print(out.shape)
    print(v)
    time.sleep(.5)
    return out


class Pencil:
    def __call__(self, *args, **kwargs):
        v = m.get()[0]
        vv = mm.get()[0]
        out = proj[int(v), :, int(vv)]
        print(v, vv, mmm.get()[0])
        time.sleep(.5)
        return np.squeeze(out)


# make the detectors
det = SynSignalWithRegistry(f, name="img", labels={"detectors"},)
det.kind = "hinted"

dark_det = SynSignalWithRegistry(lambda: dark.squeeze(),
                                 name="img", labels={"detectors"},)
det.kind = "hinted"

flat_det = SynSignalWithRegistry(lambda: flat.squeeze(),
                                 name="img", labels={"detectors"},)
det.kind = "hinted"

#g = Pencil()
#det2 = SynSignal(g, name="img", labels={"detectors"})
#det2.kind = "hinted"


# dark shot
dark_uid, = RE(bp.count([dark_det], 1))

# flat shot
flat_uid, = RE(bp.count([flat_det], 1))

for uid in [dark_uid, flat_uid]:
    for nd in dbs['raw'][uid].documents(fill=True):
        print(nd)

# Build scan
l = [0, 90]
for i in range(8):
    ll = l.copy()
    interval = sorted(set(ll))[1] / 2
    for lll in ll:
        j = lll + interval
        j = round(j, 0)
        if j not in l and j < 180:
            l.append(j)
# Run Full Field Scans, each scan has more slices, showing how we can minimize
# the number of slices by interleaving them by half
RE(
    bp.list_scan(
        [det],
        m,
        l[:2],
        md={
            "tomo": {
                "type": "full_field",
                "rotation": "motor1",
                "center": rot_center,
            },
            "sc_dk_field_uid": dark_uid,
            "sc_flat_field_uid": flat_uid,
        },
    )
)
'''
# Run in pencil beam geometry (this takes a long time!)
RE(
    bp.grid_scan(
        [det2],
        m,
        0,
        180,
        181,
        mm,
        0,
        639,
        640,
        True,
        md={
            "tomo": {
                "type": "pencil",
                "rotation": "motor1",
                "translation": "motor2",
                "center": rot_center,
            }
        },
    )
)
# Run in 3D pencil beam geometry
RE(
    bp.grid_scan(
        [det2],
        mmm,
        0,
        2,
        10,
        m,
        0,
        180,
        41,
        True,
        mm,
        200,
        401,
        21,
        True,

        md={
            "tomo": {
                "type": "pencil",
                "rotation": "motor1",
                "translation": "motor2",
                "stack": "motor3",
                "center": rot_center - 200,
            }
        },
    )
)
RE.abort()
'''
