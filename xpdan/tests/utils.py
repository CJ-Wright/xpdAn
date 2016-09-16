import os
import tempfile
import time
from uuid import uuid4

import numpy as np


def insert_imgs(mds, fs, n, shape, save_dir=tempfile.mkdtemp()):
    """
    Insert images into mds and fs for testing

    Parameters
    ----------
    mds
    fs
    n
    shape
    save_dir

    Returns
    -------

    """
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
    for i, img in enumerate(imgs):
        fs_uid = str(uuid4())
        fn = os.path.join(save_dir, fs_uid + '.npy')
        np.save(fn, img)
        fs_res = fs.insert_resource('npy', fn)
        fs.insert_datum(fs_res, fs_uid, fs_kwargs={})
        # insert into FS
        mds.insert_event(
            descriptor=descriptor,
            uid=str(uuid4()),
            time=time.time(),
            data={k: v for k, v in zip(['img'], fs_uid)},
            timestamps={},
            seq_num=i)
    mds.insert_run_stop(run_start=run_start,
                        uid=str(uuid4()),
                        time=time.time())
    return save_dir
