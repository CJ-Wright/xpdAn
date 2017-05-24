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
import numpy as np
from matplotlib.path import Path
from scipy.sparse import csr_matrix
from multiprocessing import Pool, cpu_count
from functools import partial
from skbeam.core.mask import binned_outlier, margin
from skbeam.core.accumulators.binned_statistic import BinnedStatistic1D
from numba import jit


def mask_ring(v_list, p_list, a_std=3.):
    mask_list = []
    while len(v_list) > 0:
        norm_v_list = np.abs(v_list - np.mean(v_list)) / np.std(v_list)
        if np.all(norm_v_list < a_std):
            break
        # get the index of the worst pixel
        worst_idx = np.argmax(norm_v_list)
        # get the position of the worst pixel
        worst_p = p_list[worst_idx]
        # add the worst position to the mask
        # delete the worst position
        v_list = np.delete(v_list, worst_idx)
        p_list = np.delete(p_list, worst_idx)
        mask_list.append(worst_p)
    return mask_list


def new_masking_method(img, geo, alpha=3, tmsk=None):
    r = geo.rArray(img.shape)
    q = geo.qArray(img.shape) / 10
    delta_q = geo.deltaQ(img.shape) / 10

    pixel_size = [getattr(geo, a) for a in ['pixel1', 'pixel2']]
    rres = np.hypot(*pixel_size)
    rbins = np.arange(np.min(r) - rres / 2., np.max(r) + rres / 2., rres / 2.)
    rbinned = BinnedStatistic1D(r.ravel(), statistic=np.max, bins=rbins)

    qbin_sizes = np.nan_to_num(rbinned(delta_q.ravel()))
    qbin = np.cumsum(qbin_sizes)
    ipos = np.arange(0, np.size(img)).reshape(img.shape)
    qbinned = BinnedStatistic1D(q.ravel(), bins=qbin)
    xy = qbinned.xy
    if tmsk is None:
        tmsk = np.ones(img.shape)

    ring_values = []
    ring_positions = []
    for i in np.unique(xy):
        ring_values.append(img.ravel()[xy == i])
        ring_positions.append(ipos.ravel()[xy == i])
    with Pool(cpu_count()) as p:
        vp = [v + (alpha, ) for v in zip(ring_values, ring_positions) if len(v[0]) > 0]
        print('start mask')
        ss = p.starmap(mask_ring, vp)
        print('finished mask')

    mask_list = [i for s in ss for i in s]

    mask = np.ones(img.shape)
    mask[np.unravel_index(mask_list, img.shape)] = False
    # mask = mask.astype(bool)
    mask *= tmsk
    return mask.astype(bool)


def mask_img(img, geo,
             edge=30,
             lower_thresh=0.0,
             upper_thresh=None,
             bs_width=13, tri_offset=13, v_asym=0,
             alpha=2.5,
             tmsk=None):
    """
    Mask an image based off of various methods

    Parameters
    ----------
    img: ndarray
        The image to be masked
    geo: pyFAI.geometry.Geometry
        The pyFAI description of the detector orientation or any
        subclass of pyFAI.geometry.Geometry class
    edge: int, optional
        The number of edge pixels to mask. Defaults to 30. If None, no edge
        mask is applied
    lower_thresh: float, optional
        Pixels with values less than or equal to this threshold will be masked.
        Defaults to 0.0. If None, no lower threshold mask is applied
    upper_thresh: float, optional
        Pixels with values greater than or equal to this threshold will be
        masked.
        Defaults to None. If None, no upper threshold mask is applied.
    bs_width: int, optional
        The width of the beamstop in pixels. Defaults to 13.
        If None, no beamstop polygon mask is applied.
    tri_offset: int, optional
        The triangular pixel offset to create a pointed beamstop polygon mask.
        Defaults to 13. If None, no beamstop polygon mask is applied.
    v_asym: int, optional
        The vertical asymmetry of the polygon beamstop mask. Defaults to 0.
        If None, no beamstop polygon mask is applied.
    alpha: float or tuple or, 1darray, optional
        Then number of acceptable standard deviations, if tuple then we use
        a linear distribution of alphas from alpha[0] to alpha[1], if array
        then we just use that as the distribution of alphas. Defaults to 2.5.
        If None, no outlier masking applied.
    tmsk: ndarray, optional
        The starting mask to be compounded on. Defaults to None. If None mask
        generated from scratch.

    Returns
    -------
    tmsk: ndarray
        The mask as a boolean array. True pixels are good pixels, False pixels
        are masked out.

    """

    r = geo.rArray(img.shape)
    pixel_size = [getattr(geo, a) for a in ['pixel1', 'pixel2']]
    rres = np.hypot(*pixel_size)
    rbins = np.arange(np.min(r) - rres / 2., np.max(r) + rres / 2., rres)
    if tmsk is None:
        working_mask = np.ones(img.shape).astype(bool)
    else:
        working_mask = tmsk.copy()
    if edge:
        working_mask *= margin(img.shape, edge)
    if lower_thresh:
        working_mask *= (img >= lower_thresh).astype(bool)
    if upper_thresh:
        working_mask *= (img <= upper_thresh).astype(bool)
    if all([a is not None for a in [bs_width, tri_offset, v_asym]]):
        center_x, center_y = [geo.getFit2D()[k] for k in
                              ['centerX', 'centerY']]
        nx, ny = img.shape
        mask_verts = [(center_x - bs_width, center_y),
                      (center_x, center_y - tri_offset),
                      (center_x + bs_width, center_y),
                      (center_x + bs_width + v_asym, ny),
                      (center_x - bs_width - v_asym, ny)]

        x, y = np.meshgrid(np.arange(nx), np.arange(ny))
        x, y = x.flatten(), y.flatten()

        points = np.vstack((x, y)).T

        path = Path(mask_verts)
        grid = path.contains_points(points)
        # Plug msk_grid into into next (edge-mask) step in automask
        working_mask *= ~grid.reshape((ny, nx))

    if alpha:
        working_mask *= binned_outlier(img, r, alpha, rbins, mask=working_mask)
    return working_mask


def better_mask_img(img, geo,
                    edge=30,
                    lower_thresh=0.0,
                    upper_thresh=None,
                    bs_width=13, tri_offset=13, v_asym=0,
                    alpha=2.5,
                    tmsk=None):
    """
    Mask an image based off of various methods

    Parameters
    ----------
    img: ndarray
        The image to be masked
    geo: pyFAI.geometry.Geometry
        The pyFAI description of the detector orientation or any
        subclass of pyFAI.geometry.Geometry class
    edge: int, optional
        The number of edge pixels to mask. Defaults to 30. If None, no edge
        mask is applied
    lower_thresh: float, optional
        Pixels with values less than or equal to this threshold will be masked.
        Defaults to 0.0. If None, no lower threshold mask is applied
    upper_thresh: float, optional
        Pixels with values greater than or equal to this threshold will be
        masked.
        Defaults to None. If None, no upper threshold mask is applied.
    bs_width: int, optional
        The width of the beamstop in pixels. Defaults to 13.
        If None, no beamstop polygon mask is applied.
    tri_offset: int, optional
        The triangular pixel offset to create a pointed beamstop polygon mask.
        Defaults to 13. If None, no beamstop polygon mask is applied.
    v_asym: int, optional
        The vertical asymmetry of the polygon beamstop mask. Defaults to 0.
        If None, no beamstop polygon mask is applied.
    alpha: float or tuple or, 1darray, optional
        Then number of acceptable standard deviations, if tuple then we use
        a linear distribution of alphas from alpha[0] to alpha[1], if array
        then we just use that as the distribution of alphas. Defaults to 2.5.
        If None, no outlier masking applied.
    tmsk: ndarray, optional
        The starting mask to be compounded on. Defaults to None. If None mask
        generated from scratch.

    Returns
    -------
    tmsk: ndarray
        The mask as a boolean array. True pixels are good pixels, False pixels
        are masked out.

    """

    if tmsk is None:
        working_mask = np.ones(img.shape).astype(bool)
    else:
        working_mask = tmsk.copy()
    if edge:
        working_mask *= margin(img.shape, edge)
    if lower_thresh:
        working_mask *= (img >= lower_thresh).astype(bool)
    if upper_thresh:
        working_mask *= (img <= upper_thresh).astype(bool)
    if all([a is not None for a in [bs_width, tri_offset, v_asym]]):
        center_x, center_y = [geo.getFit2D()[k] for k in
                              ['centerX', 'centerY']]
        nx, ny = img.shape
        mask_verts = [(center_x - bs_width, center_y),
                      (center_x, center_y - tri_offset),
                      (center_x + bs_width, center_y),
                      (center_x + bs_width + v_asym, ny),
                      (center_x - bs_width - v_asym, ny)]

        x, y = np.meshgrid(np.arange(nx), np.arange(ny))
        x, y = x.flatten(), y.flatten()

        points = np.vstack((x, y)).T

        path = Path(mask_verts)
        grid = path.contains_points(points)
        # Plug msk_grid into into next (edge-mask) step in automask
        working_mask *= ~grid.reshape((ny, nx))

    if alpha:
        working_mask *= new_masking_method(img, geo, alpha=alpha,
                                           tmsk=working_mask)
    return working_mask


def compress_mask(mask):
    """Compress a mask via a csr sparse matrix

    Parameters
    ----------
    mask: 2d boolean array
        The mask, True/1 are good pixels, False/0 are bad

    Returns
    -------
    list:
        The csr_matrix data
    list:
        The csr_matrix indices
    list:
        The csr_matrix indptr

    See Also:
    ---------
    scipy.sparse.csr_matrix
    """
    cmask = csr_matrix(~mask)
    # FIXME: we may need to also return the mask shape
    return cmask.data.tolist(), cmask.indices.tolist(), cmask.indptr.tolist()


def decompress_mask(data, indices, indptr, shape):
    """Decompress a csr sparse matrix into a mask

    Parameters
    ----------
    data: list
        The csr_matrix data
    indices: list
        The csr_matrix indices
    indptr: list
        The csr_matrix indptr
    shape: tuple
        The shape of the array to be recreated

    Returns
    -------
    mask: 2d boolean array
        The mask, True/1 are good pixels, False/0 are bad

    See Also:
    ---------
    scipy.sparse.csr_matrix
    """
    cmask = csr_matrix(
        tuple([np.asarray(a) for a in [data, indices, indptr]]), shape=shape)
    return ~cmask.toarray().astype(bool)
