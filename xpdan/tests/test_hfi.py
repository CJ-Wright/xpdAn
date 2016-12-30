from redsky.savers import NPYSaver
from redsky.streamer import db_store_single_resource_single_file

from ..hfi import *
from numpy.testing import assert_array_equal
from pprint import pprint
from pyFAI import AzimuthalIntegrator


def test_spoof_wavelength_calibration_hfi(exp_db, an_db, wavelength):
    hdr = exp_db[-1]
    dec_hfi = db_store_single_resource_single_file(
        an_db, {})(spoof_wavelength_calibration_hfi)
    # Actually run the thing
    for (n, z) in dec_hfi(exp_db.restream(hdr, fill=True)):
        pprint(n)
        pprint(z)
        print()
        if n == 'start':
            assert z['parents'] == [hdr['start']['uid'], ]
        if n == 'descriptor':
            assert z['data_keys']['wavelength']['dtype'] == 'float'

        if n == 'stop':
            assert z['exit_status'] == 'success'

        if n == 'event':
            assert z['data']['wavelength'] == wavelength


def test_spoof_detector_calibration_hfi(exp_db, an_db, wavelength):
    hdr = exp_db[-1]
    dec_hfi = db_store_single_resource_single_file(
        an_db, {})(spoof_detector_calibration_hfi)
    # Actually run the thing
    for (n, z) in dec_hfi(exp_db.restream(hdr, fill=True)):
        pprint(n)
        pprint(z)
        print()
        if n == 'start':
            assert z['parents'] == [hdr['start']['uid'], ]
        if n == 'descriptor':
            assert z['data_keys']['detector_calibration']['dtype'] == 'object'

        if n == 'stop':
            assert z['exit_status'] == 'success'

        if n == 'event':
            assert type(z['data']['detector_calibration']) == dict
            ai = AzimuthalIntegrator()
            ai.setPyFAI(**z['data']['detector_calibration'])


def test_dark_subtraction_hfi(exp_db, an_db, tmp_dir, img_size):
    hdr = exp_db[-1]
    dec_hfi = db_store_single_resource_single_file(
        an_db, {'img': (NPYSaver, (tmp_dir,), {})})(dark_subtraction_hfi)
    dark_hdr = exp_db(dark_uid=hdr['start']['dark_uid'], is_dark_img=True)[0]
    dark_img = next(exp_db.get_events(dark_hdr,
                                      fill=True))['data']['pe1_image']
    # Actually run the thing
    for (n, z), (n2, z2) in zip(dec_hfi((
            exp_db.restream(hdr, fill=True),
            exp_db.restream(dark_hdr, fill=True))),
            exp_db.restream(hdr, fill=True)):
        pprint(n)
        pprint(z)
        print()
        if n == 'start':
            assert z['parents'] == [hdr['start']['uid'],
                                    dark_hdr['start']['uid']]
        if n == 'descriptor':
            for ss1, ss2 in zip(z['data_keys']['img']['shape'], img_size):
                assert ss1 == ss2

        if n == 'stop':
            assert z['exit_status'] == 'success'

        if n == 'event':
            assert_array_equal(z['data']['img'],
                               z2['data']['pe1_image'] - dark_img)


def test_margin_mask_hfi(exp_db, an_db, tmp_dir, img_size):
    hdr = exp_db[-1]
    dec_hfi = db_store_single_resource_single_file(
        an_db, {'mask': (NPYSaver, (tmp_dir,), {})})(margin_mask_hfi)
    dark_hdr = exp_db(dark_uid=hdr['start']['dark_uid'], is_dark_img=True)[0]
    dark_img = next(exp_db.get_events(dark_hdr,
                                      fill=True))['data']['pe1_image']
    # Actually run the thing
    for (n, z), (n2, z2) in zip(dec_hfi(exp_db.restream(hdr, fill=True),
                                        edge=13, image_name='pe1_image'),
                                exp_db.restream(hdr, fill=True)):
        pprint(n)
        pprint(z)
        print()
        if n == 'start':
            assert z['parents'] == [hdr['start']['uid']]
        if n == 'descriptor':
            for ss1, ss2 in zip(z['data_keys']['mask']['shape'], img_size):
                assert ss1 == ss2

        if n == 'stop':
            assert z['exit_status'] == 'success'

        if n == 'event':
            assert_array_equal(z['data']['mask'], margin(img_size, 13))
