from itertools import chain, tee

from xpdan.glbl import an_glbl
from xpdan.hfi import (dark_subtraction_hfi, spoof_detector_calibration_hfi,
                       polarization_correction_hfi, master_mask_hfi,
                       integrate_hfi)


def spoof_db_integrate(img_hdr, glbl=an_glbl, **kwargs):
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
    img_stream = exp_db.restream(img_hdr, fill=True)

    # Replace 'hdr's with 'stream's in kwargs
    kwargs.update(glbl=glbl)
    p = spoof_integration_pipeline(img_stream, **kwargs)
    for n, d in p:
        if n == 'start':
            rv = d['uid']
        pass

    return rv


def spoof_integration_pipeline(raw_stream,
                               dark_stream=None, dark_kwargs=None,
                               detector_calibration_stream=None,
                               mask_stream=None, mask_kwargs=None,
                               polarization_kwargs=None,
                               integration_kwargs=None,
                               glbl=an_glbl):
    """Integrate

    Parameters
    ----------
    raw_stream: generator
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
        name, doc = next(raw_stream)
        img_start = doc
        # Quickly put it back on the top no one will notice
        raw_stream = chain((i for i in ((name, doc),)), raw_stream)

    raw_stream, raw_stream2 = tee(raw_stream, 2)
    # If None go get it and use the latest
    if dark_stream is None:
        dark_hdr = exp_db(dark_collection_uid=img_start['dark_collection_uid'],
                          is_dark=True)[0]
        dark_stream = exp_db.restream(dark_hdr, fill=True)

    dark_corrected_stream = dark_subtraction_hfi((raw_stream, dark_stream),
                                                 **dark_kwargs)

    # If None go get it
    if detector_calibration_stream is None:
        detector_calibration_stream = spoof_detector_calibration_hfi(
            raw_stream2)

    detector_calibration_streams = tee(detector_calibration_stream, 3)

    polarization_corrected_stream = polarization_correction_hfi(
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
        mask_stream = master_mask_hfi((pre_integration_stream2,
                                       detector_calibration_streams[
                                           1]), **mask_kwargs)
    # Otherwise use the provided mask
    else:
        pre_integration_stream = polarization_corrected_stream

    iq_stream = integrate_hfi((pre_integration_stream,
                               detector_calibration_streams[2]),
                              mask_stream=mask_stream,
                              **integration_kwargs)
    yield from iq_stream
