"""This is the task graph to be implemented at XPD on Feb 7th
Note that this is tied to the v0 schema"""
from dask.dot import dot_graph
from xpdan.hfi import *
from xpdan.callbacks_core import Exporter
from xpdan.glbl import an_glbl
from xpdan.callbacks import template, data_fields
from tifffile import imsave
from skbeam.io.save_powder_output import save_output


def pipeline_save_iq(filename, q, iq):
    save_output(q, iq, filename, 'Q')


def get_raw(): pass


exp_db = None


def get_dark(stream, db):
    n, start = next(stream)
    dark_uid = start.get(an_glbl.dark_field_key, None)
    dark_hdr = db(**{'dark_collection_uid': dark_uid, 'is_dark': True})[0]
    yield from db.restream(dark_hdr, fill=True)


dark_corrected_exporter = Exporter({'img': 'data'}, template, imsave,
                                   data_fields=data_fields, overwrite=True)
iq_exporter = Exporter({'q': 'q', 'iq': 'iq'}, template, pipeline_save_iq,
                       data_fields=data_fields, overwrite=True)

t = {'raw_no_fs': (get_raw,),
     'raw': (defensive_filestore_call_hfi, 'raw_no_fs', exp_db),
     # 'vis': (vis,),
     'dark': (get_dark, 'raw', exp_db),
     'dark_corrected': (dark_subtraction_hfi, 'raw', 'dark'),
     'dark_write_file_callback': (
     dark_corrected_exporter.__call__, 'dark_corrected'),
     'calibration': (spoof_detector_calibration_hfi, 'raw'),
     'polarization_corrected': (
     polarization_correction_hfi, 'muxed_calibration',
     'dark_corrected'),
     # '2dvis': (vis2d, 'dark_corrected', 'vis'),
     'mask': (spoof_mask_hfi, 'muxed_calibration', 'polarization_corrected'),
     'iq': (
         integrate_hfi, 'polarization_corrected', 'calibration', 'mask'),
     'iq_write_file_callback': (iq_exporter.__call__, 'iq')
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

dot_graph(t, 'xpd_pipeline_feb7.pdf')
