"""This module is for instantiation of Callbacks"""
from .callbacks_core import Exporter

# xpdAcq standard instantiation
template = '/direct/XF28ID1/pe2_data/xpdUser/tiff_base/{start.sample_name}'
data_fields = ['temperature', 'diff_x', 'diff_y', 'eurotherm']  # known devices
xpdacq_tiff_export = Exporter('pe1_image', template, data_fields,
                              overwrite=True)
