from xpdan.run_engine import analysis_run_engine
from xpdan.tools import sum_images
from xpdan.startup.start import analysis_db


def test_sum_images(db_with_imgs):
    print(db_with_imgs[-1])
    print(db_with_imgs[-1].keys())
    hdr_uid = analysis_run_engine(db_with_imgs[-1], sum_images,
                                  md={'name': 'test'})
    print(hdr_uid)
    hdr = analysis_db[hdr_uid]
    print(hdr)
    events = analysis_db.get_events(hdr)
    assert len(list(events)) == 1
