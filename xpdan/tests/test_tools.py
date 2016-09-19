from xpdan.run_engine import analysis_run_engine
from xpdan.tools import sum_images
from xpdan.startup.start import analysis_db
import numpy as np
from numpy.testing import assert_array_almost_equal


def test_sum_images(db_with_imgs):
    hdr_uid = analysis_run_engine([db_with_imgs[-1]], sum_images,
                                  md={'name': 'test'}, db=db_with_imgs)
    hdr = analysis_db[hdr_uid]
    print(hdr['stop'])
    events = analysis_db.get_events(hdr)
    summed_events = len(list(events))
    print(summed_events)
    assert summed_events == 1
    events = analysis_db.get_events(hdr, fill=True)
    img = next(events)['data']['img']
    assert_array_almost_equal(img, np.ones(img.shape)*5)
