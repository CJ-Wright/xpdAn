#!/usr/bin/env python
##############################################################################
#
# xpdan            by Billinge Group
#                   Simon J. L. Billinge sb2896@columbia.edu
#                   (c) 2016 trustees of Columbia University in the City of
#                        New York.
#                   All rights reserved
#
# File coded by:    Timothy Liu, Christopher J. Wright
#
# See AUTHORS.txt for a list of people who contributed.
# See LICENSE.txt for license information.
#
##############################################################################
import os
from itertools import islice, tee, chain

import numpy as np
import tifffile as tif
import yaml
from pyFAI.azimuthalIntegrator import AzimuthalIntegrator

from .glbl import an_glbl
from .tools import mask_img, decompress_mask
from .utils import _clean_info, _timestampstr
from xpdan.io import read_fit2d_msk

# top definition for minimal impacts on the code

w_dir = os.path.join(an_glbl.home, 'tiff_base')
W_DIR = w_dir  # in case of crashes in old codes


class DataReduction:
    """ class that handle operations on images from databroker header

        Note: not a callback
    """

    def __init__(self, exp_db=an_glbl.exp_db, image_field=None):
        # for file name 
        self.fields = ['sample_name', 'sp_type', 'sp_requested_exposure']
        self.labels = ['dark_frame']
        self.data_fields = ['temperature']
        self.root_dir_name = 'sample_name'
        self.exp_db = exp_db
        if image_field is None:
            self.image_field = an_glbl.det_image_field

    def _feature_gen(self, event):
        """ generate a human readable file name.

        file name is generated by metadata information in event
        run_start
        """
        feature_list = []
        run_start = event.descriptor['run_start']
        uid = run_start['uid'][:6]
        # get special label
        for el in self.labels:
            label = run_start.get(el, None)
            if label is not None:
                feature_list.append(str(label))
            else:
                pass
        # get fields
        for key in self.fields:
            el = str(run_start.get(key, None))
            if el is not None:
                # truncate string length
                if len(el) > 12:
                    value = el[:12]
                # clear space
                feature = _clean_info(el)
                feature_list.append(feature)
            else:
                pass
        # get data fields
        for key in self.data_fields:
            val = event['data'].get(key, None)
            if el is not None:
                feature = "{}={}".format(key, val)
                feature_list.append(feature)
            else:
                pass
        # get uid
        feature_list.append(uid)
        return "_".join(feature_list)

    def pull_dark(self, header):
        dark_uid = header.start.get(an_glbl.dark_field_key, None)
        if dark_uid is None:
            print("INFO: no dark frame is associated in this header, "
                  "subrraction will not be processed")
            return None, header.start.time
        else:
            dark_search = {'group': 'XPD', 'uid': dark_uid}
            dark_header = self.exp_db(**dark_search)
            dark_img = np.asarray(self.exp_db.get_images(dark_header,
                                                         self.image_field)
                                  ).squeeze()
        return dark_img, dark_header[0].start.time

    def _dark_sub(self, event, dark_img):
        """ priviate method operates on event level """
        is_dark_sub = False
        img = event['data'][self.image_field]
        if dark_img is not None and isinstance(dark_img, np.ndarray):
            is_dark_sub = True
            img -= dark_img
        ind = event['seq_num']
        event_timestamp = event['timestamps'][self.image_field]
        return img, event_timestamp, ind, is_dark_sub

    def dark_sub(self, header):
        """ public method operates on header level """
        dark_img, dark_time_stamp = self.pull_dark(header)
        for ev in self.exp_db.get_events(header, fill=True):
            sub_img, timestamp, ind, is_dark_sub = self._dark_sub(ev,
                                                                  dark_img)
            yield sub_img, timestamp, ind, dark_img, header.start, ev

    def _file_name(self, event, event_timestamp, ind):
        """ priviate method operates on event level """
        f_name = self._feature_gen(event)
        f_name = '_'.join([_timestampstr(event_timestamp),
                           f_name])
        f_name = '{}_{:04d}.tif'.format(f_name, ind)
        return f_name

    def construct_event_stream(self, header):
        for event in self.exp_db.get_events(header, fill=True):
            yield event['data'][self.image_field], \
                  event['timestamps'][self.image_field], \
                  event['seq_num'], event


# init
xpd_data_proc = DataReduction()
ai = AzimuthalIntegrator()


def image_stream(events, handler):
    for e in events:
        yield e[handler.image_field]


def associate_dark(header, events, handler):
    dark_uid = header.start.get(an_glbl.dark_field_key, None)
    if dark_uid is None:
        print("INFO: no dark frame is associated in this header, "
              "subrraction will not be processed")
        dark_img = None
    else:
        dark_search = {'uid': dark_uid}
        dark_header = handler.exp_db(**dark_search)
        dark_img = np.asarray(handler.exp_db.get_images(dark_header,
                                                        handler.image_field)
                              ).squeeze()
    for e in events:
        yield dark_img


def subtract_gen(event_stream1, event_stream2):
    for e1, e2 in zip(event_stream1, event_stream2):
        if all([e1, e2]):
            yield e1 - e2
        else:
            yield e1


def pol_correct_gen(img_stream, ai):
    for img in img_stream:
        yield img / ai.polarization_cor


def integrate(header, dark_sub_bool=True,
                       polarization_factor=0.99,
                       mask_setting='default', mask_dict=None,
                       save_image=True, root_dir=None,
                       config_dict=None, handler=xpd_data_proc,
                       sum_idx_list=None,
                       **kwargs):
    # Prep integrator
    root = header.start.get(handler.root_dir_name, None)
    if root is not None:
        root_dir = os.path.join(W_DIR, root)
        os.makedirs(root_dir, exist_ok=True)
    else:
        root_dir = W_DIR

    # config_dict
    if config_dict is None:
        config_dict = _load_config(header)  # default dict
        if config_dict is None:  # still None
            print("INFO: can't find calibration parameter under "
                  "xpdUser/config_base/ or header metadata\n"
                  "data reduction can not be perfomed.")
            return
    # setting up geometry
    ai.setPyFAI(**config_dict)
    npt = _npt_cal(config_dict)
    events = handler.exp_db.get_images(header, fill=True)
    l_events = list(tee(events, 2))
    img_stream = image_stream(l_events.pop(), handler)
    l_img_stream = list(tee(img_stream, 5))

    # Dark logic
    if dark_sub_bool:
        # Associate dark image(s)
        dark_imgs = associate_dark(header, l_img_stream.pop())
        # Subtract dark image(s)
        imgs = subtract_gen(l_img_stream.pop(), dark_imgs)

    # Sum images
    if sum_idx_list:
        imgs = sum_images(imgs, sum_idx_list)

    # Correct for polarization
    if polarization_factor:
        imgs = (img/ai.polarization(img.shape, polarization_factor) for img in imgs)

    # Mask
    mask = None
    if mask_setting != 'auto':
        if type(mask_setting) == np.ndarray and \
                        mask_setting.dtype == np.dtype('bool'):
            mask = mask_setting
        elif type(mask_setting) == str and os.path.exists(mask_setting):
            if os.path.splitext(mask_setting)[-1] == '.msk':
                mask = read_fit2d_msk(mask_setting)
            else:
                mask = np.load(mask_setting)
        elif mask_setting == 'default':
            mask_md = header.start.get('mask', None)
            if mask_md is None:
                print("INFO: no mask associated or mask information was"
                      " not set up correctly, no mask will be applied")
                mask = None
            else:

                mask = decompress_mask(*mask_md)
        elif mask_setting == 'None':
            mask = None
        mask_stream = (mask for i in imgs)
    else:
        mask_stream = (mask_img(img, ai, **an_glbl.mask_dict) for img in imgs)
    # Warn for odd data
    # Get filename
    # Integrate
    for img, mask in zip(imgs, mask_stream):
        rvs = []
        if mask is not None:
            # make a copy, don't overwrite it
            _mask = ~mask
        else:
            _mask = None
        if save_image:

        for unit, fn, l in zip(["q_nm^-1", "2th_deg"],
                               [chi_fn_Q, chi_fn_2th]):
            print("INFO: save chi file: {}".format(fn))

            rv = ai.integrate1d(img, npt, filename=fn, mask=_mask,
                                # polarization_factor=polarization_factor,
                                unit=unit, **kwargs)
            rvs.append(rv)
        yield rvs



""" analysis function operates at header level """


def _prepare_header_list(headers):
    if not isinstance(headers, list):
        # still do it in two steps, easier to read
        header_list = list()
        header_list.append(headers)
    else:
        header_list = headers
    return header_list


def _load_config(header):
    try:
        with open(
                os.path.join(an_glbl.config_base,
                             an_glbl.calib_config_name)) as f:
            config_dict = yaml.load(f)
    except FileNotFoundError:
        config_dict = header.start.get('calibration_md', None)
        if config_dict is None:
            # back support
            config_dict = header.start.get('sc_calibration_md', None)

    return config_dict


def _npt_cal(config_dict, total_shape=(2048, 2048)):
    """ config_dict should be a PyFAI calibration dict """
    x_0, y_0 = (config_dict['centerX'], config_dict['centerY'])
    center_len = np.hypot(x_0, y_0)
    # FIXME : use hardwired shape now, use direct info later
    x_total, y_total = total_shape
    total_len = np.hypot(x_total, y_total)
    # FIXME : use the longest diagonal distance. Optimal value might have
    # to do with grid of Fourier transform. Need to revisit it later
    dist = max(total_len, total_len - center_len)
    return dist


def integrate_and_save(headers, dark_sub_bool=True,
                       polarization_factor=0.99,
                       mask_setting='default', mask_dict=None,
                       save_image=True, root_dir=None,
                       config_dict=None, handler=xpd_data_proc,
                       sum_idx_list=None,
                       **kwargs):
    """ integrate and save dark subtracted images for given list of headers

    Parameters
    ----------
    headers : list
        a list of databroker.header objects
    dark_sub_bool : bool, optional
        option to turn on/off dark subtraction functionality
    polarization_factor : float, optional
        polarization correction factor, ranged from -1(vertical) to +1
        (horizontal). default is 0.99. set to None for no
        correction.
    mask_setting : str, ndarray optional
        string for mask option. Valid options are 'default', 'auto' and
        'None'. If 'default', mask included in metadata will be
        used. If 'auto', a new mask would be generated from current
        image. If 'None', no mask would be applied. If a ndarray of bools use
        as mask. Predefined option is 'default'.
    mask_dict : dict, optional
        dictionary stores options for automasking functionality.
        default is defined by an_glbl.auto_mask_dict.
        Please refer to documentation for more details
    save_image : bool, optional
        option to save dark subtracted images. images will be 
        saved to the same directory of chi files. default is True.
    root_dir : str, optional
        path of chi files that are going to be saved. default is 
        the same as your image file
    config_dict : dict, optional
        dictionary stores integration parameters of pyFAI azimuthal 
        integrator. default is the most recent parameters saved in 
        xpdUser/conifg_base
    handler : instance of class, optional
        instance of class that handles data process, don't change it 
        unless needed.
    sum_idx_list: list of lists and tuple or list or 'all', optional
        The list of lists and tuples which specify the images to be summed.
        If 'all', sum all the images in the run. If None, do nothing.
        Defaults to None.
    kwargs :
        addtional keywords to overwrite integration behavior. Please
        refer to pyFAI.azimuthalIntegrator.AzimuthalIntegrator for
        more information

    Note
    ----
    complete docstring of masking functionality could be find in
    ``mask_img``

    customized mask can be assign to by kwargs (It must be a ndarray)
    >>> integrate_and_save(mask_setting=my_mask)

    See also
    --------
    xpdan.tools.mask_img
    pyFAI.azimuthalIntegrator.AzimuthalIntegrator
    """
    # normalize list
    header_list = _prepare_header_list(headers)

    total_rv_list_Q = []
    total_rv_list_2theta = []

    # iterate over header
    for header in header_list:
        root = header.start.get(handler.root_dir_name, None)
        if root is not None:
            root_dir = os.path.join(W_DIR, root)
            os.makedirs(root_dir, exist_ok=True)
        else:
            root_dir = W_DIR

        # config_dict
        if config_dict is None:
            config_dict = _load_config(header)  # default dict
            if config_dict is None:  # still None
                print("INFO: can't find calibration parameter under "
                      "xpdUser/config_base/ or header metadata\n"
                      "data reduction can not be perfomed.")
                return

        # setting up geometry
        ai.setPyFAI(**config_dict)
        npt = _npt_cal(config_dict)

        header_rv_list_Q = []
        header_rv_list_2theta = []

        # dark logic
        if dark_sub_bool:
            event_stream = handler.dark_sub(header)
        else:
            event_stream = handler.construct_event_stream(header)
        for img, event_timestamp, ind, *rest, event in sum_images(
                event_stream, sum_idx_list):

            f_name = handler._file_name(event, event_timestamp, ind)
            if dark_sub_bool:
                f_name = 'sub_' + f_name
            if sum_idx_list:
                f_name = 'sum_' + rest[-1] + f_name

            # copy tiff_name here
            tiff_fn = f_name

            # masking logic
            # workflow for xpdAcq v0.5.1 release, will change later
            mask = None
            if (type(mask_setting) == np.ndarray and
                        mask_setting.dtype == np.dtype('bool')):
                mask = mask_setting
            elif type(mask_setting) == str and os.path.exists(mask_setting):
                if os.path.splitext(mask_setting)[-1] == '.msk':
                    mask = read_fit2d_msk(mask_setting)
                else:
                    mask = np.load(mask_setting)
            elif mask_setting == 'default':
                mask_md = header.start.get('mask', None)
                if mask_md is None:
                    print("INFO: no mask associated or mask information was"
                          " not set up correctly, no mask will be applied")
                    mask = None
                else:
                    # unpack here
                    data, ind, indptr = mask_md
                    print("INFO: pull off mask associate with your image: {}"
                          .format(f_name))
                    mask = decompress_mask(data, ind, indptr, img.shape)
            elif mask_setting == 'auto':
                mask = mask_img(img, ai, **an_glbl.mask_dict)
            elif mask_setting == 'None':
                mask = None

            mask_fn = os.path.splitext(f_name)[0]  # remove ext
            if mask_setting is not None:
                print("INFO: mask file '{}' is saved at {}"
                      .format(mask_fn, root_dir))
                np.save(os.path.join(root_dir, mask_fn),
                        mask_setting)  # default is .npy from np.save

            # integration logic
            stem, ext = os.path.splitext(f_name)
            chi_name_Q = 'Q_' + stem + '.chi'  # q_nm^-1
            chi_name_2th = '2th_' + stem + '.chi'  # deg^-1
            print("INFO: integrating image: {}".format(f_name))
            # Q-integration
            chi_fn_Q = os.path.join(root_dir, chi_name_Q)
            chi_fn_2th = os.path.join(root_dir, chi_name_2th)
            for unit, fn, l in zip(["q_nm^-1", "2th_deg"],
                                   [chi_fn_Q, chi_fn_2th],
                                   [header_rv_list_Q, header_rv_list_2theta]):
                print("INFO: save chi file: {}".format(fn))
                if mask is not None:
                    # make a copy, don't overwrite it
                    _mask = ~mask
                else:
                    _mask = None

                rv = ai.integrate1d(img, npt, filename=fn, mask=_mask,
                                    polarization_factor=polarization_factor,
                                    unit=unit, **kwargs)
                l.append(rv)

            # save image logic
            w_name = os.path.join(root_dir, tiff_fn)
            if save_image:
                tif.imsave(w_name, img)
                if os.path.isfile(w_name):
                    print('image "%s" has been saved at "%s"' %
                          (tiff_fn, root_dir))
                else:
                    print('Sorry, something went wrong with your tif saving')
                    return

        # save run_start
        stem, ext = os.path.splitext(w_name)
        config_name = w_name.replace(ext, '.yml')
        with open(config_name, 'w') as f:
            yaml.dump(header.start, f)  # save all md in start

        # each header generate  a list of rv
        total_rv_list_Q.append(header_rv_list_Q)
        total_rv_list_2theta.append(header_rv_list_2theta)

    print("INFO: chi/image files are saved at {}".format(root_dir))
    return total_rv_list_Q, total_rv_list_2theta


def integrate_and_save_last(dark_sub_bool=True, polarization_factor=0.99,
                            mask_setting='default', mask_dict=None,
                            save_image=True, root_dir=None,
                            config_dict=None, handler=xpd_data_proc,
                            sum_idx_list=None,
                            **kwargs):
    """ integrate and save dark subtracted images for given list of headers

    Parameters
    ----------
    dark_sub_bool : bool, optional
        option to turn on/off dark subtraction functionality
    polarization_factor : float, optional
        polarization correction factor, ranged from -1(vertical) to 
        +1 (horizontal). default is 0.99. set to None for no
        correction.
    mask_setting : str, ndarray optional
        string for mask option. Valid options are 'default', 'auto' and
        'None'. If 'default', mask included in metadata will be
        used. If 'auto', a new mask would be generated from current
        image. If 'None', no mask would be applied. If a ndarray of bools use
        as mask. Predefined option is 'default'.
    mask_dict : dict, optional
        dictionary stores options for automasking functionality. 
        default is defined by an_glbl.auto_mask_dict. 
        Please refer to documentation for more details.
    save_image : bool, optional
        option to save dark subtracted images. images will be 
        saved to the same directory of chi files. default is True.
    root_dir : str, optional
        path of chi files that are going to be saved. default is 
        xpdUser/userAnalysis/
    config_dict : dict, optional
        dictionary stores integration parameters of pyFAI azimuthal 
        integrator. default is the most recent parameters saved in 
        xpdUser/conifg_base
    handler : instance of class, optional
        instance of class that handles data process, don't change it 
        unless needed.
    sum_idx_list: list of lists and tuple or list or 'all', optional
        The list of lists and tuples which specify the images to be summed.
        If 'all', sum all the images in the run. If None, do nothing.
        Defaults to None.
    kwargs :
        addtional keywords to overwrite integration behavior. Please
        refer to pyFAI.azimuthalIntegrator.AzimuthalIntegrator for
        more information

    Note
    ----
    complete docstring of masking functionality could be find in
    ``mask_img``

    customized mask can be assign to by kwargs (It must be a ndarray)
    >>> integrate_and_save_last(mask=my_mask)

    See also
    --------
    xpdan.tools.mask_img
    pyFAI.azimuthalIntegrator.AzimuthalIntegrator
    """
    integrate_and_save(handler.exp_db[-1], dark_sub_bool=dark_sub_bool,
                       polarization_factor=polarization_factor,
                       mask_setting=mask_setting, mask_dict=mask_dict,
                       save_image=save_image,
                       root_dir=root_dir,
                       config_dict=config_dict,
                       handler=handler, sum_idx_list=sum_idx_list, **kwargs)


def save_tiff(headers, dark_sub_bool=True, max_count=None, dryrun=False,
              handler=xpd_data_proc):
    """ save images obtained from dataBroker as tiff format files.

    Parameters
    ----------
    headers : list
        a list of header objects obtained from a query to dataBroker.

    dark_sub_bool : bool, optional
        Default is True, which allows dark/background subtraction to 
        be done before saving each image. If header doesn't contain
        necessary information to perform dark subtraction, uncorrected
        image will be saved.

    max_count : int, optional
        The maximum number of events to process per-run.  This can be
        useful to 'preview' an export or if there are corrupted files
        in the data stream (ex from the IOC crashing during data
        acquisition).

    dryrun : bool, optional
        if set to True, file won't be saved. default is False

    handler : instance of class
        instance of class that handles data process, don't change it
        unless needed.
    """
    # normalize list
    header_list = _prepare_header_list(headers)

    for header in header_list:
        # create root_dir
        root = header.start.get(handler.root_dir_name, None)
        if root is not None:
            root_dir = os.path.join(W_DIR, root)
            os.makedirs(root_dir, exist_ok=True)
        else:
            root_dir = W_DIR
        # dark logic
        dark_img = None
        if dark_sub_bool:
            dark_img, dark_time = handler.pull_dark(header)
        # event
        for event in handler.exp_db.get_events(header, fill=True):
            img, event_timestamp, ind, is_dark_sub = handler._dark_sub(
                event, dark_img)
            f_name = handler._file_name(event, event_timestamp, ind)
            if is_dark_sub:
                f_name = 'sub_' + f_name
            # save tif
            w_name = os.path.join(root_dir, f_name)
            if not dryrun:
                tif.imsave(w_name, img)
                if os.path.isfile(w_name):
                    print('image "%s" has been saved at "%s"' %
                          (f_name, root_dir))
                else:
                    print('Sorry, something went wrong with your tif saving')
                    return
            # dryrun : print
            else:
                print("dryrun: image {} has been saved at {}"
                      .format(f_name, root_dir))
            if max_count is not None and ind >= max_count:
                # break the loop if max_count reached, move to next header
                break

        # save run_start
        stem, ext = os.path.splitext(w_name)
        config_name = w_name.replace(ext, '.yml')
        with open(config_name, 'w') as f:
            yaml.dump(header.start, f)  # save all md in start

    print(" *** {} *** ".format('Saving process finished'))


def save_last_tiff(dark_sub_bool=True, max_count=None, dryrun=False,
                   handler=xpd_data_proc):
    """ save images from the most recent scan as tiff format files.

    Parameters
    ----------
    dark_sub_bool : bool, optional
        Default is True, which allows dark/background subtraction to 
        be done before saving each image. If header doesn't contain
        necessary information to perform dark subtraction, uncorrected
        image will be saved.

    max_count : int, optional
        The maximum number of events to process per-run.  This can be
        useful to 'preview' an export or if ithere are corrupted files
        in the data stream (ex from the IOC crashing during data acquisition).

    dryrun : bool, optional
        if set to True, file won't be saved. default is False

    handler : instance of class
        instance of class that handles data process, don't change it
        unless needed.
    """

    save_tiff(handler.exp_db[-1], dark_sub_bool, max_count, dryrun,
              handler=handler)


def sum_images(event_stream, idxs_list=None):
    """Sum images in a header

    Sum the images in a header according to the idxs_list

    Parameters
    ----------
    event_stream: generator
        The event stream to be summed. The image must be first, with the
        event itself last
    idxs_list: list of lists and tuple or list or 'all', optional
        The list of lists and tuples which specify the images to be summed.
        If 'all', sum all the images in the run. If None, do nothing.
        Defaults to None.
    Yields
    -------
    event_stream:
        The event stream, with the images (in the first position) summed

    >>> from databroker import db
    >>> hdr = db[-1]
    >>> total_imgs = sum_images(hdr) # Sum all the images
    >>> assert len(total_imgs) == 1
    >>> total_imgs = sum_images(hdr, [1, 2, 3])
    >>> assert len(total_imgs) == 1
    >>> total_imgs = sum_images(hdr, [[1, 2, 3], (5,10)])
    >>> assert len(total_imgs) == 2
    """
    if idxs_list is None:
        yield from event_stream
    if idxs_list is 'all':
        total_img = None
        for img, *rest, event in event_stream:
            if total_img is None:
                total_img = img
            else:
                total_img += img
        yield chain([total_img], rest, ['all', event])
    elif idxs_list:
        # If we only have one list make it into a list of lists
        if not all(isinstance(e1, list) or isinstance(e1, tuple) for e1 in
                   idxs_list):
            idxs_list = [idxs_list]
        # Each idx list gets its own copy of the event stream
        # This is to prevent one idx list from eating the generator
        event_stream_copies = tee(event_stream, len(idxs_list))
        for idxs, sub_event_stream in zip(idxs_list, event_stream_copies):
            total_img = None
            if isinstance(idxs, tuple):
                for idx in range(idxs[0], idxs[1]):
                    img, *rest, event = next(islice(sub_event_stream, idx))
                    if total_img is None:
                        total_img = img
                    else:
                        total_img += img
                yield chain([total_img], rest,
                            ['({}-{})'.format(*idxs), event])
            else:
                total_img = None
                for idx in idxs:
                    img, *rest, event = next(islice(sub_event_stream, idx))
                    if total_img is None:
                        total_img = img
                    else:
                        total_img += img
                yield chain([total_img], rest, ['[{}]'.format(
                    ','.join(map(str, idxs))), event])
