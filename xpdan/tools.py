from xpdan.startup.start import db, mds, fs
from uuid import uuid4
import time
from itertools import islice
from tifffile import imsave
import scipy.stats as sts
import numpy as np


def subs_dark(hdr, dark_hdr_idx=-1, dark_event_idx=-1):
    data_names = ['img']
    data_keys = {k: dict(
        source='subs_dark',
        external='FILESTORE:',
        dtype='array'
    ) for k in data_names}

    dark_hdr = db(is_dark_img=True, dark_uid=hdr['dark_uid'])[dark_hdr_idx]
    dark_events = db.get_events(dark_hdr, fill=True)
    dark_img = islice(dark_events, dark_event_idx)
    for event in db.get_events(hdr, fill=True):
        light_img = event['data']['img']
        img = light_img - dark_img
        # save
        # Eventually save this as a sparse array to save space
        imsave('file_loc', img)

        # insert into filestore
        uid = str(uuid4())
        fs_res = fs.insert_resource('TIFF', 'file_loc')
        fs.insert_datum(fs_res, uid)
        yield uid, data_names, data_keys, img


def mask_img(hdr, cal_hdr,
             alpha=2.5, lower_thresh=0.0, upper_thresh=None,
             margin=30., tmsk=None):
    data_names = ['msk']
    data_keys = {k: dict(
        source='auto_mask',
        external='FILESTORE:',
        dtype='array'
    ) for k in data_names}

    geo = next(db.get_events(cal_hdr, fill=True))['data']['poni']
    for event in db.get_events(hdr, fill=True):
        img = event['data']['img']
        r = geo.rArray(img.shape)
        pixel_size = [getattr(geo, a) for a in ['pixel1', 'pixel2']]
        rres = np.hypot(*pixel_size)
        rbins = np.arange(np.min(r) - rres / 2., np.max(r) + rres / 2., rres)
        if tmsk is None:
            tmsk = np.ones(img.shape, dtype=int).astype(bool)
        if margin:
            tmsk *= margin_mask(img.shape, margin)
        if lower_thresh:
            tmsk *= (img > lower_thresh).astype(bool)
        if upper_thresh:
            tmsk *= (img < upper_thresh).astype(bool)
        if alpha:
            tmsk *= ring_blur_mask(img, r, alpha, rbins, mask=tmsk)
        # save
        # Eventually save this as a sparse array to save space
        np.save('file_loc', tmsk)

        # insert into filestore
        uid = str(uuid4())
        fs_res = fs.insert_resource('npy', 'file_loc')
        fs.insert_datum(fs_res, uid)
        yield uid, data_names, data_keys, tmsk


def polarization_correction(hdr, cal_hdr, polarization=.99):
    data_names = ['img']
    data_keys = {k: dict(
        source='pyFAI-polarization',
        external='FILESTORE:',
        dtype='array'
    ) for k in data_names}

    geo = next(db.get_events(cal_hdr, fill=True))['data']['poni']
    for event in db.get_events(hdr, fill=True):
        img = event['data']['img']
        img /= geo.polarization(img.shape, polarization)
        # save
        imsave('file_loc', img)

        # insert into filestore
        uid = str(uuid4())
        fs_res = fs.insert_resource('TIFF', 'file_loc')
        fs.insert_datum(fs_res, uid)
        yield uid, data_names, data_keys, img


def integrate(img_hdr, mask_hdr, cal_hdr, stats='mean', npt=1500):
    if not isinstance(stats, list):
        stats = [stats]
    data_names = ['iq_{}'.format(stat) for stat in stats]

    data_keys = {dn: dict(
        source='cjw-integrate',
        external='FILESTORE:',
        dtype='array'
    ) for dn in data_names}

    geo = next(db.get_events(cal_hdr, fill=True))['data']['poni']
    for img_event, mask_event in zip(db.get_events(img_hdr, fill=True),
                                     db.get_events(mask_hdr, fill=True)):
        mask = mask_event['data']['msk']
        img = img_event['data']['img'][mask]
        q = geo.qArray(img.shape)[mask] / 10  # pyFAI works in nm^1
        uids = []
        data = []
        for stat in stats:
            iq, q, _ = sts.binned_statistic(q, img, statistic=stat, bins=npt)
            data.append((q, iq))
            # save
            save_output(q, iq, 'save_loc', 'Q')

            # insert into filestore
            uid = str(uuid4())
            fs_res = fs.insert_resource('CHI', 'file_loc')
            fs.insert_datum(fs_res, uid)
            uids.append(uid)
        yield uids, data_names, data_keys, data


def associate_background(hdr, iqs, bg_hdr, bg_iq, match_key=None):
    data_names = ['foreground_iq', 'background_iq']
    data_keys = {k: dict(
        source='associate_background',
        external='FILESTORE:',
        dtype='array'
    ) for k in data_names}

    # mux the background data with the foreground data
    # TODO: get more complex, handle multiple match keys with a cost function
    bg_iq_events = db.get_events(bg_iq)
    if match_key is not None:
        table = db.get_table(bg_hdr, fields=match_key)
        for event, iq in zip(db.get_events(hdr), db.get_events(iqs)):
            bg_event_idx = np.argmin(
                np.abs(event[match_key] - table[match_key]))
            bg_event = next(islice(bg_iq_events, bg_event_idx))
            # TODO: support multiple iqs
            yield [iq['data']['iq_mean'], bg_event['data']['iq_mean']], \
                  data_names, data_keys


def background_subtraction(hdr, bg_scale=1):
    data_names = ['iq']
    data_keys = {k: dict(
        source='background_subtraction',
        external='FILESTORE:',
        dtype='array'
    ) for k in data_names}

    ran_manual = False

    for event in db.get_events(hdr, fill=True):
        fg_iq, bg_iq = [event['data'][k] for k in
                        ['foreground_iq', 'background_iq']]
        if bg_scale == 'auto':
            raise NotImplementedError(
                'There is no automatic background scaling,yet')
        if not ran_manual and bg_scale == 'manual':
            raise NotImplementedError('WE have not implemented slider bar'
                                      'based manual background scaling, yet')
        corrected_iq = fg_iq[1] - bg_iq[1] * bg_scale
        # save
        save_output(fg_iq[0], corrected_iq, 'file_loc', 'Q')

        # insert into filestore
        uid = str(uuid4())
        fs_res = fs.insert_resource('CHI', 'file_loc')
        fs.insert_datum(fs_res, uid)
        yield uid, data_names, data_keys, corrected_iq
