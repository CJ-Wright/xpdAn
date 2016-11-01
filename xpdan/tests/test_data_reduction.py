##############################################################################
#
# xpdan            by Billinge Group
#                   Simon J. L. Billinge sb2896@columbia.edu
#                   (c) 2016 trustees of Columbia University in the City of
#                        New York.
#                   All rights reserved
#
# File coded by:    Christopher J. Wright
#
# See AUTHORS.txt for a list of people who contributed.
# See LICENSE.txt for license information.
#
##############################################################################
from xpdan.data_reduction import image_stream, associate_dark, subtract_gen, \
    pol_correct_gen, mask_logic, an_glbl, decompress_mask, mask_img,\
    _load_config, read_fit2d_msk
from itertools import tee, product
import pytest
from pprint import pprint

import numpy as np
import os
from numpy.testing import assert_array_equal
import fabio
from pyFAI.azimuthalIntegrator import AzimuthalIntegrator


ai = AzimuthalIntegrator()
sum_idx_values = (
    None, 'all', [1, 2, 3], [(1, 3)], [[1, 2, 3], [2, 3]], [[1, 3], (1, 3)])

integrate_params = ['dark_sub_bool',
                    'polarization_factor',
                    'mask_setting',
                    'mask_dict',
                    'save_image',
                    'root_dir',
                    'config_dict',
                    'sum_idx_list']
good_kwargs = [(True, False), (.99,
                               # .95, .5
                               ),
               ('use_saved_mask_msk', 'use_saved_mask',
                'default', 'auto',
                'None',
                'array'),
               [None, {'alpha': 3}],
               (True, False), [None], [None], sum_idx_values]

bad_integrate_params = ['dark_sub_bool',
                        'polarization_factor',
                        'mask_setting',
                        'mask_dict',
                        'save_image',
                        'config_dict',
                        'sum_idx_list']

bad_kwargs = [['str'] for i in range(len(bad_integrate_params))]

integrate_kwarg_values = product(*good_kwargs)
integrate_kwargs = []
for vs in integrate_kwarg_values:
    d = {k: v for (k, v) in zip(integrate_params, vs)}
    integrate_kwargs.append((d, False))

for vs in bad_kwargs:
    d = {k: v for (k, v) in zip(bad_integrate_params, vs)}
    integrate_kwargs.append((d, True))

save_tiff_kwargs = []
save_tiff_params = ['dark_sub_bool', 'max_count', 'dryrun']
save_tiff_kwarg_values = [(True, False), (None, 1), (True, False)]


for vs in save_tiff_kwarg_values:
    d = {k: v for (k, v) in zip(save_tiff_params, vs)}
    save_tiff_kwargs.append((d, False))


def test_image_stream(exp_db, handler):
    events = exp_db.get_events(exp_db[-1], fill=True)
    imgs = exp_db.get_images(exp_db[-1], handler.image_field)
    for e, i in zip(events, imgs):
        assert_array_equal(e['data'][handler.image_field], i)


def test_associate_dark(exp_db, handler):
    hdr = exp_db[-1]
    events = exp_db.get_events(hdr, fill=True)
    darks = associate_dark(hdr, events, handler)
    ideal_dark = exp_db.get_images(hdr, handler.image_field)[0]
    for img in darks:
        assert_array_equal(img, ideal_dark)


mask_kwargs = ['use_saved_mask_msk', 'use_saved_mask', 'default', 'auto',
               'None', 'array']


@pytest.mark.parametrize("mask_setting", mask_kwargs)
def test_mask_logic(exp_db, handler, mask_setting, disk_mask):
    hdr = exp_db[-1]
    imd = an_glbl.mask_dict
    imgs = exp_db.get_images(hdr, handler.image_field)

    if mask_setting == 'auto':
        ai.setPyFAI(**_load_config(hdr))
        pprint(_load_config(hdr))
        pprint(ai.getFit2D())
        expected = (mask_img(img, ai, **imd) for img in imgs)
        gen = mask_logic(imgs, mask_setting, imd, hdr, ai=ai)
        for a, b in zip(gen, expected):
            assert_array_equal(a, b)
        return
    if mask_setting == 'use_saved_mask_msk':
        mask_setting = disk_mask[0]
        expected = read_fit2d_msk(disk_mask[0])
    elif mask_setting == 'use_saved_mask':
        mask_setting = disk_mask[1]
        expected = np.load(disk_mask[1])
    elif mask_setting == 'default':
        mask_md = hdr.start.get('mask', None)
        if mask_md is None:
            expected = None
        else:
            expected = decompress_mask(*mask_md)
    elif mask_setting == 'None':
        expected = None
    elif mask_setting == 'array':
        mask_setting = np.random.random_integers(
            0, 1, disk_mask[-1].shape).astype(bool)
        expected = mask_setting
    gen = mask_logic(imgs, mask_setting, imd, hdr)
    for m in gen:
        if isinstance(m, np.ndarray):
            assert_array_equal(m, expected)
