from redsky.savers import NPYSaver
from redsky.streamer import db_store_single_resource_single_file

from ..hfi import *
from numpy.testing import assert_array_equal
from pprint import pprint
from pyFAI import AzimuthalIntegrator


def test_spoof_wavelength_calibration_hfi(exp_db, an_db, wavelength):
    hdr = exp_db[-1]
    hfi = spoof_wavelength_calibration_hfi
    dec_hfi = db_store_single_resource_single_file(
        an_db, {})(hfi)
    # Actually run the thing
    for (n, z) in dec_hfi(exp_db.restream(hdr, fill=True)):
        pprint(n)
        pprint(z)
        print()
        if n == 'start':
            assert z['parents'] == [hdr['start']['uid'], ]
            assert z['hfi'] == hfi.__name__
            for k, expected_v in {'hfi_module': inspect.getmodule(
                    hfi).__name__, 'hfi': hfi.__name__,
                                  'args': (), 'kwargs': {},
                                  'process': 'spoof',
                                  'process_module': 'spoof'}.items():
                assert z['provenance'][k] == expected_v
        if n == 'descriptor':
            assert z['data_keys']['wavelength']['dtype'] == 'float'

        if n == 'stop':
            assert z['exit_status'] == 'success'

        if n == 'event':
            assert z['data']['wavelength'] == wavelength

    for (n, z) in an_db.restream(an_db[-1], fill=True):
        pprint(n)
        pprint(z)
        print()
        if n == 'start':
            assert z['parents'] == [hdr['start']['uid'], ]
            assert z['hfi'] == hfi.__name__
            for k, expected_v in {'hfi_module': inspect.getmodule(
                    hfi).__name__, 'hfi': hfi.__name__,
                                  'args': [], 'kwargs': {},
                                  'process': 'spoof',
                                  'process_module': 'spoof'}.items():
                assert z['provenance'][k] == expected_v
        if n == 'descriptor':
            assert z['data_keys']['wavelength']['dtype'] == 'float'

        if n == 'stop':
            assert z['exit_status'] == 'success'

        if n == 'event':
            assert z['data']['wavelength'] == wavelength


def test_spoof_detector_calibration_hfi(exp_db, an_db):
    hdr = exp_db[-1]
    hfi = spoof_detector_calibration_hfi
    dec_hfi = db_store_single_resource_single_file(
        an_db, {})(hfi)
    # Actually run the thing
    for (n, z) in dec_hfi(exp_db.restream(hdr, fill=True)):
        pprint(n)
        pprint(z)
        print()
        if n == 'start':
            assert z['parents'] == [hdr['start']['uid'], ]
            assert z['hfi'] == hfi.__name__
            for k, expected_v in {'hfi_module': inspect.getmodule(
                    hfi).__name__, 'hfi': hfi.__name__,
                                  'args': (), 'kwargs': {},
                                  'process': 'spoof',
                                  'process_module': 'spoof'}.items():
                assert z['provenance'][k] == expected_v
        if n == 'descriptor':
            assert z['data_keys']['detector_calibration']['dtype'] == 'object'

        if n == 'stop':
            assert z['exit_status'] == 'success'

        if n == 'event':
            assert type(z['data']['detector_calibration']) == dict
            ai = AzimuthalIntegrator()
            ai.setPyFAI(**z['data']['detector_calibration'])

    for (n, z) in an_db.restream(an_db[-1], fill=True):
        pprint(n)
        pprint(z)
        print()
        if n == 'start':
            assert z['parents'] == [hdr['start']['uid'], ]
            assert z['hfi'] == hfi.__name__
            for k, expected_v in {'hfi_module': inspect.getmodule(
                    hfi).__name__, 'hfi': hfi.__name__,
                                  'args': [], 'kwargs': {},
                                  'process': 'spoof',
                                  'process_module': 'spoof'}.items():
                assert z['provenance'][k] == expected_v
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
    hfi = dark_subtraction_hfi
    process = sub
    dec_hfi = db_store_single_resource_single_file(
        an_db, {'img': (NPYSaver, (tmp_dir,), {})})(hfi)
    dark_hdr = exp_db(dark_collection_uid=hdr['start']['dark_collection_uid'],
                      is_dark=True)[0]
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
            assert z['hfi'] == hfi.__name__
            for k, expected_v in {'hfi_module': inspect.getmodule(
                    hfi).__name__, 'hfi': hfi.__name__,
                                  'args': (),
                                  'kwargs': {'dark_event_number': 0},
                                  'process_module': inspect.getmodule(
                                      process).__name__,
                                  'process': process.__name__}.items():
                assert z['provenance'][k] == expected_v

        if n == 'descriptor':
            for ss1, ss2 in zip(z['data_keys']['img']['shape'], img_size):
                assert ss1 == ss2

        if n == 'stop':
            assert z['exit_status'] == 'success'

        if n == 'event':
            assert_array_equal(z['data']['img'],
                               z2['data']['pe1_image'] - dark_img)


def test_polarization_correction_hfi(exp_db, an_db, tmp_dir, img_size):
    hdr = exp_db[-1]
    hfi = spoof_detector_calibration_hfi
    dec_hfi = db_store_single_resource_single_file(
        an_db, {})(hfi)
    for a in dec_hfi(exp_db.restream(hdr, fill=True)):
        pass
    cal_hdr = an_db[-1]
    geo = AzimuthalIntegrator()
    geo.setPyFAI(
        **next(an_db.get_events(cal_hdr))['data']['detector_calibration'])

    hfi = polarization_correction_hfi
    process = correct_polarization
    dec_hfi = db_store_single_resource_single_file(
        an_db, {'img': (NPYSaver, (tmp_dir,), {})})(hfi)

    # Actually run the thing
    for (n, z), (n2, z2) in zip(dec_hfi((
            exp_db.restream(hdr, fill=True),
            an_db.restream(cal_hdr, fill=True)), image_name='pe1_image',
            polarization_factor=.95),
            exp_db.restream(hdr, fill=True)):
        pprint(n)
        pprint(z)
        print()
        if n == 'start':
            assert z['parents'] == [hdr['start']['uid'],
                                    cal_hdr['start']['uid']]
            assert z['hfi'] == hfi.__name__
            for k, expected_v in {'hfi_module': inspect.getmodule(
                    hfi).__name__, 'hfi': hfi.__name__,
                                  'args': (),
                                  'kwargs': dict(polarization_factor=.95),
                                  'process_module': inspect.getmodule(
                                      process).__name__,
                                  'process': process.__name__}.items():
                assert z['provenance'][k] == expected_v

        if n == 'descriptor':
            for ss1, ss2 in zip(z['data_keys']['img']['shape'], img_size):
                assert ss1 == ss2

        if n == 'stop':
            assert z['exit_status'] == 'success'

        if n == 'event':
            assert z['data']['img'].shape == img_size
            assert_array_equal(
                z['data']['img'],
                correct_polarization(geo, z2['data']['pe1_image'], .95))


def test_integrate_hfi(exp_db, an_db, tmp_dir):
    hdr = exp_db[-1]
    hfi = spoof_detector_calibration_hfi
    dec_hfi = db_store_single_resource_single_file(
        an_db, {})(hfi)
    for a in dec_hfi(exp_db.restream(hdr, fill=True)):
        pass
    cal_hdr = an_db[-1]

    hfi = integrate_hfi
    geo = AzimuthalIntegrator
    process = geo.integrate1d
    dec_hfi = db_store_single_resource_single_file(
        an_db, {'iq': (NPYSaver, (tmp_dir,), {}),
                'q': (NPYSaver, (tmp_dir,), {})})(hfi)

    kwargs = dict(npt=2000)
    # Actually run the thing
    for (n, z), (n2, z2) in zip(dec_hfi((
            exp_db.restream(hdr, fill=True),
            an_db.restream(cal_hdr, fill=True)), image_name='pe1_image',
            **kwargs),
            exp_db.restream(hdr, fill=True)):
        pprint(n)
        pprint(z)
        print()
        if n == 'start':
            assert z['parents'] == [hdr['start']['uid'],
                                    cal_hdr['start']['uid']]
            assert z['hfi'] == hfi.__name__
            for k, expected_v in dict(
                    hfi_module=inspect.getmodule(hfi).__name__,
                    hfi=hfi.__name__, args=(),
                    kwargs=kwargs,
                    process_module=inspect.getmodule(process).__name__,
                    process=process.__name__).items():
                assert z['provenance'][k] == expected_v

        if n == 'descriptor':
            assert z['data_keys']['iq']['shape'] == kwargs['npt']

        if n == 'stop':
            assert z['exit_status'] == 'success'

        if n == 'event':
            assert z['data']['iq'].shape[0] == kwargs['npt']
            assert z['data']['q'].shape[0] == kwargs['npt']

