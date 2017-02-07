from itertools import tee

from xpdan.callbacks import *
from xpdan.conf_glbl import an_glbl
from xpdan.hfi import (dark_subtraction_hfi, spoof_detector_calibration_hfi,
                       polarization_correction_hfi, integrate_hfi,
                       spoof_mask_hfi,
                       defensive_filestore_call_hfi, get_dark, terminate)

'''
def integration_pipeline(img_stream,
                         dark_stream=None, dark_kwargs=None,
                         detector_calibration_stream=None,
                         mask_stream=None, mask_kwargs=None,
                         polarization_kwargs=None,
                         integration_kwargs=None,
                         glbl=an_glbl):
    """Integrate

    Parameters
    ----------
    img_stream: generator
        The image data stream
    dark_stream: generator, optional
        The dark image data stream. If None search for the dark data.
        Defaults to None
    dark_kwargs: dict, optional
        Kwargs for the dark processing
    detector_calibration_stream: generator, optional
        The detector calibration stream. If None search for the detector
        calibration data. Defaults to None
    mask_stream: generator, optional
        The mask stream. If 'None' don't use a mask. If None generate a mask
        from scratch. Defaults to None
    mask_kwargs: dict, optional
        The mask kwargs. See `xpdan.hfi.master_mask_hfi`
    polarization_kwargs: dict, optional
        The polarization correction kwargs. Defaults to .99
    integration_kwargs: dict, optional

    glbl

    Returns
    -------

    """
    exp_db = glbl.exp_db
    an_db = glbl.an_db

    img_dec = db_store_single_resource_single_file(
        an_db, {'img': (NPYSaver, (glbl.usrAnalysis_dir,), {})})
    mask_dec = db_store_single_resource_single_file(
        an_db, {'mask': (NPYSaver, (glbl.usrAnalysis_dir,), {})})
    if dark_kwargs is None:
        dark_kwargs = {}
    if mask_kwargs is None:
        mask_kwargs = {}
    if integration_kwargs is None:
        integration_kwargs = {'npt': 1500}  # TODO: pull dynamically
    if polarization_kwargs is None:
        polarization_kwargs = {'polarization_factor': .99}

    # We need to peek at the image start document to fill in the None headers
    img_start = None
    if img_start is None:
        name, doc = next(img_stream)
        img_start = doc
        # Quickly put it back on the top no one will notice
        img_stream = chain((i for i in ((name, doc),)), img_stream)

    img_stream, img_stream2 = tee(img_stream, 2)
    # If None go get it and use the latest
    if dark_stream is None:
        dark_hdr = exp_db(dark_collection_uid=img_start['dark_collection_uid'],
                          is_dark=True)[0]
        dark_stream = exp_db.restream(dark_hdr, fill=True)

    dark_corrected_stream = img_dec(dark_subtraction_hfi)(
        (img_stream, dark_stream), **dark_kwargs)

    # If None go get it
    if detector_calibration_stream is None:
        detector_calibration_hdr = an_db(calibration_collection_uid=img_start[
            'calibration_collection_uid'])
        if len(detector_calibration_hdr) > 0:
            detector_calibration_hdr = detector_calibration_hdr[0]
            detector_calibration_stream = an_db.restream(
                detector_calibration_hdr, fill=True)
        # If None exist make one
        else:
            """
            XXX Rebuild this when we are using GUI calibration
            calibration_img_hdr = exp_db(
                calibration_collection_uid=img_start[
                'calibration_collection_uid'])[0]
            calibration_img_stream = exp_db.restream(calibration_img_hdr,
                                                     fill=True)
            """

            detector_calibration_stream = db_store_single_resource_single_file(
                an_db)(spoof_detector_calibration_hfi)(img_stream2)

    detector_calibration_streams = tee(detector_calibration_stream, 3)
    polarization_corrected_stream = img_dec(polarization_correction_hfi)(
        (dark_corrected_stream, detector_calibration_streams[0]),
        **polarization_kwargs)

    # masks
    # If string 'None' do nothing
    if mask_stream is 'None':
        pre_integration_stream = polarization_corrected_stream
        mask_stream = None

    # If python None create the mask
    elif mask_stream is None:
        pre_integration_stream, pre_integration_stream2 = tee(
            polarization_corrected_stream, 2)
        mask_stream = mask_dec(master_mask_hfi)((pre_integration_stream2,
                                                 detector_calibration_streams[
                                                     1]), **mask_kwargs)
    # Otherwise use the provided mask
    else:
        pre_integration_stream = polarization_corrected_stream

    iq_stream = db_store_single_resource_single_file(
        an_db, {'iq': (NPYSaver, (glbl.usrAnalysis_dir,), {}),
                'q': (NPYSaver, (glbl.usrAnalysis_dir,), {}), })(
        integrate_hfi)((pre_integration_stream,
                        detector_calibration_streams[2]),
                       mask_stream=mask_stream,
                       **integration_kwargs)
    yield from iq_stream


def db_integrate(img_hdr, glbl=an_glbl, **kwargs):
    """Integrate at header

    Parameters
    ----------
    img_hdr: databroker.header
        The header to integrate
    glbl: dict
        The analysis global

    Returns
    -------

    """
    exp_db = glbl.exp_db
    an_db = glbl.an_db
    img_stream = exp_db.restream(img_hdr, fill=True)

    # Replace 'hdr's with 'stream's in kwargs
    for key, db in [('dark_hdr', exp_db),
                    ('detector_calibration_hdr', an_db),
                    ('mask_hdr', an_db), ]:
        new_key = key.replace('hdr', 'stream')
        # If the header is None or doesn't exist the stream is None
        if kwargs.get(key, None) is None:
            kwargs[new_key] = None
        # Otherwise restream the header
        else:
            kwargs[new_key] = db.restream(key, fill=True)
            kwargs.pop(key)

    kwargs.update(glbl=glbl)
    p = integration_pipeline(img_stream, **kwargs)
    for n, d in p:
        if n == 'start':
            rv = d['uid']
        pass

    return rv

'''


def integration_pipeline(raw_no_fs,
                         dark_stream=None, dark_kwargs=None,
                         detector_calibration_stream=None,
                         mask_stream=None, mask_kwargs=None,
                         polarization_kwargs=None,
                         integration_kwargs=None,
                         glbl=an_glbl):
    """This is the Feb. 7th 2017 integration pipeline for XPD it is based off
     of xpdSchema v0

    Parameters
    ----------
    raw_no_fs: generator
        The generator with all the raw data

    Returns
    -------

    """
    exp_db = an_glbl.exp_db
    raw_no_fss = tee(raw_no_fs, 3)
    mask = spoof_mask_hfi(raw_no_fs.pop())
    calibration = spoof_detector_calibration_hfi(raw_no_fs.pop())
    calibrations = tee(calibration, 2)

    raw = defensive_filestore_call_hfi(stream=raw_no_fss.pop(), db=exp_db)
    raws = tee(raw, 3)
    # export raw
    re = (raw_exporter(d) for d in raws.pop())

    dark = get_dark(raws.pop(), exp_db)
    darks = tee(dark, 2)
    # dark_export
    de = (dark_exporter(d) for d in darks.pop())

    dark_corrected = dark_subtraction_hfi((raws.pop(), darks.pop()),
                                          image_name=an_glbl.det_image_field)
    dark_correcteds = tee(dark_corrected, 2)
    # dark corrected export
    dce = (dark_corrected_exporter(d) for d in dark_correcteds.pop())

    polarization_corrected = polarization_correction_hfi(dark_correcteds.pop(), calibrations.pop())

    iq = integrate_hfi(polarization_corrected, calibration.pop(), mask)

    # iq_export
    iqe = (iq_exporter(d) for d in iq)
    terminate(iqe, de, dce, re)