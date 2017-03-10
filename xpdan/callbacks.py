"""This module is for instantiation of Callbacks"""
from tifffile import imsave

from xpdan.io import pipeline_save_iq
from .callbacks_core import Exporter

# xpdAcq standard instantiation
# TODO: put this in globals?
template = '/direct/XF28ID1/pe2_data/xpdUser/tiff_base/{start.sample_name}'
data_fields = ['temperature', 'diff_x', 'diff_y', 'eurotherm']  # known devices
xpdacq_tiff_export = Exporter('pe1_image', template, imsave, data_fields,
                              overwrite=True)
dark_corrected_exporter = Exporter({'img': 'data'}, template, imsave,
                                   data_fields=data_fields, overwrite=True,
                                   suffex='_sub')

dark_exporter = Exporter({'pe1_image': 'data'}, template, imsave,
                         data_fields=data_fields, overwrite=True,
                         suffex='_dark')

raw_exporter = Exporter({'img': 'data'}, template, imsave,
                        data_fields=data_fields, overwrite=True,
                        suffex='_raw')

iq_exporter = Exporter({'q': 'q', 'iq': 'iq'}, template, pipeline_save_iq,
                       data_fields=data_fields, overwrite=True)
