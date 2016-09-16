import numpy as np
from uuid import uuid4
import time
import tempfile


def insert_imgs(mds, fs, n, shape):
    imgs = [np.ones(shape)] * n
    run_start = mds.insert_run_start(uid=str(uuid4()), time=time.time(),
                                     name='test')
    data_keys = {
        'img': dict(source='background_subtraction', external='FILESTORE:',
                    dtype='array')}
    data_hdr = dict(run_start=run_start,
                    data_keys=data_keys,
                    time=time.time(), uid=str(uuid4()))
    descriptor = mds.insert_descriptor(**data_hdr)
    for i, img in imgs:
        # insert into FS
        save_name = tempfile.mkstemp()
        fs
        mds.insert_event(
            descriptor=descriptor,
            uid=str(uuid4()),
            time=time.time(),
            data={k: v for k, v in zip(data_names, res)},
            timestamps={},
            seq_num=i)