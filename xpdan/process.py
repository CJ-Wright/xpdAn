from xpdan.tools import *
from xpdan.run_engine import analysis_run_engine

def process_to_iq(hdrs, det_cal_hdr_idx=-1):
    """
    Process raw data from MDS to I(Q) data

    Parameters
    ----------
    hdrs: header or list of headers
        The data to be processed
    det_cal_hdr_idx: int, optional
        Calibration index to use if there are multiple calibrations
        Defaults to the latest calibration

    Yields
    -------
    iqs: analysis header
        The headers associated with the I(Q) data
    """
    if not isinstance(hdrs, list):
        hdrs = [hdrs]
    for hdr in hdrs:
        # 1. get a detector calibration
        cal_hdrs = db(is_detector_calibration=True,
                      detector_calibration_uid=hdr['detector_calibration_uid'])
        cal_hdr = cal_hdrs[det_cal_hdr_idx]
        cal_geo_hdr = find_an_hdr(cal_hdr['uid'], 'calibrate_detector')
        if not cal_geo_hdr:
            cal_geo_hdr = calibrate_detector(cal_hdr)
        # 2. dark subtraction
        imgs = analysis_run_engine(hdr, subs_dark)
        # 3. polarization correction
        corrected_imgs = analysis_run_engine([imgs, cal_geo_hdr],
                                             polarization_correction)
        # 4. mask
        masks = analysis_run_engine([corrected_imgs, cal_geo_hdr], mask_img)
        # 5. integrate
        iqs = analysis_run_engine([imgs, masks, cal_geo_hdr], integrate)
        yield iqs


def process_to_pdf(hdrs, bg_hdr_idx=-1, det_cal_hdr_idx=-1):
    """
    Process a raw MDS header to the PDF

    Parameters
    ----------
    hdrs: header or list of headers
        The data to be processed
    bg_hdr_idx: int, optional
        Background index to use if there are multiple background headers
    det_cal_hdr_idx: int, optional
        Calibration index to use if there are multiple calibrations
        Defaults to the latest calibration

    Yields
    -------
    pdf: analysis header
        The headers associated with the PDF data
    """
    if not isinstance(hdrs, list):
        hdrs = [hdrs]
    for hdr in hdrs:
        iqs = process_to_iq(hdr, det_cal_hdr_idx=det_cal_hdr_idx)
        bg_hdrs = db(is_background=True, background_uid=hdr['background_uid'])
        bg_hdr = bg_hdrs[bg_hdr_idx]
        bg_iq_hdr = find_an_hdr(bg_hdr['uid'], 'integrate')
        if not bg_iq_hdr:
            bg_iq_hdr = process_to_iq(bg_hdr)

        # 6a. associate background
        associated_bg_hdr = analysis_run_engine([hdr, iqs, bg_hdr, bg_iq_hdr],
                                                associate_background)
        # 7. subtract background
        corrected_iqs = analysis_run_engine(associated_bg_hdr,
                                            background_subtraction)
        # 8., 9. optimize PDF params, get PDF
        pdf = analysis_run_engine(corrected_iqs, optimize_pdf_parameters)
        # 10. Profit
        yield pdf