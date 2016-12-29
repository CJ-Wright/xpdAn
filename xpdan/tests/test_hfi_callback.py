from redsky.savers import NPYSaver
from redsky.streamer import db_store_single_resource_single_file, \
    db_store_single_resource_single_file_non_gen

from ..hfi_callback import DarkSubtractionCallback, for_call_methods
from numpy.testing import assert_array_equal
from pprint import pprint


def test_dark_subtraction_hfi(exp_db, an_db, tmp_dir, img_size):
    hdr = exp_db[-1]

    DSC = for_call_methods(db_store_single_resource_single_file_non_gen,
                           an_db, {'img': (NPYSaver, (tmp_dir,), {})})(
        DarkSubtractionCallback)
    DecCallback = DSC(exp_db)

    dark_hdr = exp_db(dark_uid=hdr['start']['dark_uid'], is_dark_img=True)[0]
    dark_img = next(exp_db.get_events(dark_hdr,
                                      fill=True))['data']['pe1_image']

    for n2, z2 in exp_db.restream(hdr, fill=True):
        n, z = DecCallback(n2, z2)
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


def test_dark_subtraction_hfi_process(exp_db, an_db, tmp_dir, img_size):
    hdr = exp_db[-1]

    DSC = DarkSubtractionCallback(exp_db)
    DecCallback = for_call_methods(db_store_single_resource_single_file,
                              an_db, {'img': (NPYSaver, (tmp_dir,), {})})(
        DSC)

    dark_hdr = exp_db(dark_uid=hdr['start']['dark_uid'], is_dark_img=True)[0]
    dark_img = next(exp_db.get_events(dark_hdr,
                                      fill=True))['data']['pe1_image']
    exp_db.process(hdr, DecCallback, fill=True)
    for n, d in an_db.restream(an_db[-1], fill=True):
        print(n)
        pprint(d)
        print()
