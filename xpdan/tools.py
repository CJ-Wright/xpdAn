from xpdan.startup.start import db, mds, fs, mds_fs_dec
from uuid import uuid4
import time
from itertools import islice
from tifffile import imsave
import scipy.stats as sts
import numpy as np
from matplotlib.path import Path
from skbeam.io.save_powder_output import save_output


@mds_fs_dec(['img'],
            dict(source='subs_dark', external='FILESTORE:', dtype='array'),
            imsave,
            '.',
            'TIFF')
def subs_dark(hdr, dark_hdr_idx=-1, dark_event_idx=-1):
    dark_hdr = db(is_dark_img=True, dark_uid=hdr['dark_uid'])[dark_hdr_idx]
    dark_events = db.get_events(dark_hdr, fill=True)
    dark_img = islice(dark_events, dark_event_idx)
    for event in db.get_events(hdr, fill=True):
        light_img = event['data']['img']
        img = light_img - dark_img
        yield img


@mds_fs_dec(['msk'],
            dict(source='auto_mask', external='FILESTORE:', dtype='array'),
            np.save,
            '.',
            'npy')
def mask_img(hdr, cal_hdr, alpha=2.5, lower_thresh=0.0, upper_thresh=None,
             margin=30., bs_width=13, tri_offset=13, v_asym=0, tmsk=None):
    geo = next(db.get_events(cal_hdr, fill=True))['data']['poni']
    for event in db.get_events(hdr, fill=True):
        img = event['data']['img']
        working_mask = mask(img, geo, alpha, lower_thresh, upper_thresh,
                            margin, bs_width, tri_offset, v_asym, tmsk)
        yield working_mask


@mds_fs_dec(['img'],
            dict(source='pyFAI-polarization', external='FILESTORE:', dtype='array'),
            np.save,
            '.',
            'TIFF')
def polarization_correction(hdr, cal_hdr, polarization=.99):
    geo = next(db.get_events(cal_hdr, fill=True))['data']['poni']
    for event in db.get_events(hdr, fill=True):
        img = event['data']['img']
        img /= geo.polarization(img.shape, polarization)
        yield img


@mds_fs_dec(['iq_mean'],
            dict(source='cjw-integrate', external='FILESTORE:', dtype='array'),
            save_output,
            '.',
            'CHI',
            'Q')
def integrate(img_hdr, mask_hdr, cal_hdr, stat='mean', npt=1500):

    # TODO: add these back when we figure out how to handle arbitrary numbers
    # of events
    # if not isinstance(stats, list):
    #     stats = [stats]
    geo = next(db.get_events(cal_hdr, fill=True))['data']['poni']
    for img_event, mask_event in zip(db.get_events(img_hdr, fill=True),
                                     db.get_events(mask_hdr, fill=True)):
        mask = mask_event['data']['msk']
        img = img_event['data']['img'][mask]
        q = geo.qArray(img.shape)[mask] / 10  # pyFAI works in nm^1
        iq, q, _ = sts.binned_statistic(q, img, statistic=stat, bins=npt)
        yield q, iq
        # # TODO: to be restored with the list stats
        # for stat in stats:
        #     iq, q, _ = sts.binned_statistic(q, img, statistic=stat, bins=npt)
        #     data.append((q, iq))
        #     # save
        #     save_output(q, iq, 'save_loc', 'Q')
        #
        #     # insert into filestore
        #     uid = str(uuid4())
        #     fs_res = fs.insert_resource('CHI', 'file_loc')
        #     fs.insert_datum(fs_res, uid)
        #     uids.append(uid)
        # yield uids, data_names, data_keys, data


@mds_fs_dec([['foreground_iq', 'background_iq']],
            dict(source='associate_background', external='FILESTORE:',
                 dtype='array'),
            )
def associate_background(hdr, iqs, bg_hdr, bg_iq, match_key=None):
    # mux the background data with the foreground data
    # TODO: get more complex, handle multiple match keys with a cost function
    bg_iq_events = db.get_events(bg_iq)
    if match_key is not None:
        table = db.get_table(bg_hdr, fields=match_key)
        for event, iq in zip(db.get_events(hdr), db.get_events(iqs)):
            bg_event_idx = np.argmin(
                np.abs(event[match_key] - table[match_key]))
            bg_event = next(islice(bg_iq_events, bg_event_idx))
            k, = iq['data'].keys()
            yield [iq['data'][k], bg_event['data'][k]]
            # iq_names = set(iq['data'].keys())
            # bg_iq_names = set(bg_event['data'].keys())
            # names = iq_names & bg_iq_names
            # TODO: support multiple iqs
            # yield [(iq['data'][n], bg_event['data'][n]) for n in names]


@mds_fs_dec(['iq'],
            dict(source='background_subtraction', external='FILESTORE:', dtype='array'),
            save_output,
            '.',
            'CHI',
            'Q')
def background_subtraction(hdr, bg_scale=1):
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
        yield fg_iq[0], corrected_iq


def mask(img, geo, alpha=2.5, lower_thresh=0.0, upper_thresh=None, margin=30.,
         bs_width=13, tri_offset=13, v_asym=0, tmsk=None):
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
    if all([a is not None for a in [bs_width, tri_offset, v_asym]]):
        center_x, center_y = [geo.getFit2D()[k] for k in
                              ['centerX', 'centerY']]
        nx, ny = img.shape
        mask_verts = [(center_x - bs_width, center_y),
                      (center_x, center_y - tri_offset),
                      (center_x + bs_width, center_y),
                      (center_x + bs_width + v_asym, ny),
                      (center_x - bs_width - v_asym, ny)]

        x, y = np.meshgrid(np.arange(nx), np.arange(ny))
        x, y = x.flatten(), y.flatten()

        points = np.vstack((x, y)).T

        path = Path(mask_verts)
        grid = path.contains_points(points)
        # Plug msk_grid into into next (edge-mask) step in automask
        tmsk *= grid.reshape((ny, nx))

    if alpha:
        tmsk *= ring_blur_mask(img, r, alpha, rbins, mask=tmsk)
    return tmsk
