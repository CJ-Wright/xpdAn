from xpdan.pipelines.master import conf_master_pipeline
import matplotlib.pyplot as plt
import os
from pprint import pprint


def test_master_pipeline(exp_db, fast_tmp_dir):
    """Decider between pipelines"""

    source = conf_master_pipeline(exp_db, fast_tmp_dir, vis=False,
                                  write_to_disk=True)
    for nd in exp_db[-1].documents(fill=True):
        source.emit(nd)
    pprint(os.listdir(fast_tmp_dir))
    AAA