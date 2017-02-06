##############################################################################
#
# xpdan            by Billinge Group
#                   Simon J. L. Billinge sb2896@columbia.edu
#                   (c) 2016 trustees of Columbia University in the City of
#                        New York.
#                   All rights reserved
#
# File coded by:    Timothy Liu
#
# See AUTHORS.txt for a list of people who contributed.
# See LICENSE.txt for license information.
#
##############################################################################

import os
import tempfile
import pytest
from itertools import product
from tifffile import imread, imsave
from xpdan.tests.conftest import img_size
from xpdan.callbacks_core import Exporter
import numpy as np
from numpy.testing import assert_array_equal

# standard config
data_fields = ['temperature', 'diff_x', 'diff_y', 'eurotherm']  # known devices

# function options
good_params = ['save_dark']
allowed_kwargs = [(True, False), (True, False), (True, False)]
# bad_params = ['save_dark', 'dryrun', 'overwrite']
# fail_kwargs = [['fail'] for i in range(len(allowed_kwargs))]

# parametrize
test_kwargs = []
allowed_kwargs_values = product(*allowed_kwargs)

for el in allowed_kwargs_values:
    d = {k: v for k, v in zip(good_params, el)}
    test_kwargs.append((d, False))


# @pytest.mark.parametrize(("kwargs", "known_fail_bool"), test_kwargs)
def test_tiff_export(exp_db, tif_exporter_template):
    tif_export = Exporter('pe1_image', tif_exporter_template, imsave,
                          data_fields=data_fields,
                          overwrite=True, db=exp_db)
    exp_db.process(exp_db[-1], tif_export)
    # make sure files are saved
    for fn in tif_export.filenames:
        assert os.path.isfile(fn)

    for fn, db_img in zip(tif_export.filenames,
                          exp_db.get_images(exp_db[-1], 'pe1_image')):
        img = imread(fn)
        assert_array_equal(img, db_img)
