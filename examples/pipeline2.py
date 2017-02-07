"""This is the task graph to be implemented at XPD on Feb 7th
Note that this is tied to the v0 schema"""
from dask.dot import dot_graph

from xpdan.callbacks import dark_corrected_exporter, iq_exporter, \
    raw_exporter, dark_exporter
from xpdan.hfi import *


def get_raw(): pass


def terminate(): pass


exp_db = None

t = {
    'raw_no_fs': (get_raw,),
    'raw': (defensive_filestore_call_hfi, 'raw_no_fs', exp_db),
    're': (raw_exporter.__call__, 'raw'),
    'dark': (get_dark, 'raw', exp_db),
    'de': (dark_exporter.__call__, 'dark'),
    'dark_corrected': (dark_subtraction_hfi, 'raw', 'dark'),
    'dce': (dark_corrected_exporter.__call__, 'dark_corrected'),
    'calibration': (spoof_detector_calibration_hfi, 'raw_no_fs'),
    'polarization_corrected': (polarization_correction_hfi, 'calibration', 'dark_corrected'),
    'mask': (spoof_mask_hfi, 'raw_no_fs'),
    'iq': (integrate_hfi, 'polarization_corrected', 'calibration', 'mask'),
    'iqe': (iq_exporter.__call__, 'iq'),
    'terminate': (terminate, 're', 'de', 'dce', 'iqe')
    # 'vis': (vis,),
    # '2dvis': (vis2d, 'dark_corrected', 'vis'),
    # 'vis1d_iq': (vis1d, 'iq', 'vis'),
    # 'bg_iq': (get_background, 'raw'),
    # 'muxed_bg': (remux, 'bg_iq', 'raw'),
    # 'bg_corrected_iq': (sub, 'iq', 'muxed_bg'),
    # 'gr': (get_gr, 'bg_corrected_iq', 'raw'),
    # 'vis1d_gr': (vis1d, 'gr', 'vis'),
    # 'candidate_structures': (get_candidates, 'raw'),
    # 'fit_structures': (fit, 'candidate_structures', 'gr'),
    # 'vis_struc': (vis_struct, 'fit_structures', 'vis')
}

dot_graph(t, 'xpd_pipeline_feb7', format='pdf')
