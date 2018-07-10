import os

import numpy as np
from shed.translation import FromEventStream
from streamz_ext import Stream
from streamz_ext.link import link
from xpdan.callbacks import StartStopCallback
from xpdan.db_utils import query_background, query_dark, temporal_prox
from xpdan.pipelines.pipeline_utils import (
    _timestampstr,
    Filler,
)
from xpdconf.conf import glbl_dict
from xpdtools.calib import _save_calib_param
from xpdtools.pipelines.raw_pipeline import (
    make_pipeline as make_raw_pipeline,
)

image_name = glbl_dict["image_field"]
db = glbl_dict["exp_db"]
calibration_md_folder = {"folder": "xpdAcq_calib_info.yml"}


def make_general_pipeline():
    filler = Filler(db=db)
    # Build the general pipeline from the raw_pipeline
    raw_source = Stream(stream_name="raw source")

    # TODO: change this when new dark logic comes
    # Check that the data isn't a dark
    dk_uid = FromEventStream("start", (), upstream=raw_source).map(
        lambda x: "sc_dk_field_uid" in x
    )
    # Fill the raw event stream
    source = (
        raw_source.combine_latest(dk_uid)
        .filter(lambda x: x[1])
        .pluck(0)
        # Filler returns None for resource/datum data
        .starmap(filler)
        .filter(lambda x: x is not None)
    )
    # Get all the documents
    start_docs = FromEventStream("start", (), source)
    descriptor_docs = FromEventStream(
        "descriptor", (), source, event_stream_name="primary"
    )
    event_docs = FromEventStream(
        "event", (), source, event_stream_name="primary"
    )
    all_docs = event_docs.combine_latest(
        start_docs, descriptor_docs, emit_on=0
    ).starmap(
        lambda e, s, d: {
            "raw_event": e,
            "raw_start": s,
            "raw_descriptor": d,
            "human_timestamp": _timestampstr(s["time"]),
        }
    )

    # If new calibration uid invalidate our current calibration cache
    wavelength = FromEventStream("start", ("bt_wavelength",), source).unique(
        history=1
    )
    composition = FromEventStream("start", ("composition_string",), source)
    dspacing = FromEventStream("start", ("dSpacing",), source).unique(
        history=1
    )
    start_timestamp = FromEventStream("start", ("time",), source)
    h_timestamp = start_timestamp.map(_timestampstr)

    bg_docs = (
        start_docs.map(query_background, db=db)
        .zip(start_docs)
        .starmap(temporal_prox)
        .filter(lambda x: x != [])
        .map(lambda x: x[0].documents(fill=True))
        .flatten()
    )

    # Get bg dark
    bg_dark_query = FromEventStream("start", (), bg_docs).map(
        query_dark, db=db
    )

    fg_dark_query = start_docs.map(query_dark, db=db)
    fg_dark_query.filter(lambda x: x != [] and isinstance(x, list)).sink(print)
    fg_dark_query.filter(lambda x: x == []).sink(
        lambda x: print("No dark found!")
    )
    raw_source.starsink(StartStopCallback())

    return {
        "raw_source": raw_source,
        "source": source,
        "start_docs": start_docs,
        "descriptor_docs": descriptor_docs,
        "event_docs": event_docs,
        "all_docs": all_docs,
        "wavelength": wavelength,
        "composition": composition,
        "dspacing": dspacing,
        "h_timestamp": h_timestamp,
        "bg_dark_query": bg_dark_query,
        "fg_dark_query": fg_dark_query,
    }


def make_spec_pipeline(descriptor="primary"):
    start_docs = Stream()
    source = Stream()
    unique_geo = FromEventStream(
        "descriptor",
        ("detector_calibration_client_uid",),
        source,
        event_stream_name=descriptor,
    ).unique(history=1)
    detector = FromEventStream(
        "descriptor", ("detector",), source, event_stream_name=descriptor
    ).unique(history=1)

    is_calibration_img = FromEventStream(
        "descriptor", (), source, event_stream_name=descriptor
    ).map(lambda x: "detector_calibration_server_uid" in x)

    # TODO: pull this from the descriptor
    # Only pass through new calibrations (prevents us from recalculating cals)
    geo_input = FromEventStream(
        "descriptor", ("calibration_md",), source, event_stream_name=descriptor
    ).unique(history=1)
    bg_dark_query = Stream()
    raw_background_dark = FromEventStream(
        "event",
        ("data", image_name),
        bg_dark_query.map(lambda x: x[0].documents(fill=True)).flatten(),
        event_stream_name=descriptor,
    ).map(np.float32)

    bg_docs = Stream()
    # Get background
    raw_background = FromEventStream(
        "event", ("data", image_name), bg_docs, event_stream_name=descriptor
    ).map(np.float32)

    # Get foreground dark
    fg_dark_query = Stream()
    # XXX: this is double defined, because there are two things putting data
    # into this node
    raw_foreground_dark1 = FromEventStream(
        "event",
        ("data", image_name),
        fg_dark_query.filter(lambda x: x != [])
        .map(lambda x: x if not isinstance(x, list) else x[0])
        .map(lambda x: x.documents(fill=True))
        .flatten(),
        event_stream_name=descriptor,
    ).map(np.float32)

    # Pull dark live if possible
    # NOTE: THIS ASSUMES THAT THE DARK STREAMS ARE NAMED <descriptor>_dark
    raw_foreground_dark2 = FromEventStream(
        "event",
        ("data", image_name),
        event_stream_name="{}_{}".format(descriptor, "dark"),
    ).map(np.float32)
    raw_foreground_dark = raw_foreground_dark1.union(raw_foreground_dark2)

    # Get foreground
    img_counter = FromEventStream(
        "event",
        ("seq_num",),
        source,
        stream_name="seq_num",
        event_stream_name=descriptor,
    )
    raw_foreground = FromEventStream(
        "event",
        ("data", image_name),
        source,
        principle=True,
        stream_name="raw_foreground",
        event_stream_name=descriptor,
    ).map(np.float32)

    raw_source = Stream()
    return {
        "raw_source": raw_source,
        "start_docs": start_docs,
        "unique_geo": unique_geo,
        "detector": detector,
        "is_calibration_img": is_calibration_img,
        "geo_input": geo_input,
        "bg_dark_query": bg_dark_query,
        "raw_background_dark": raw_background_dark,
        "bg_docs": bg_docs,
        "raw_background": raw_background,
        "fg_dark_query": fg_dark_query,
        "raw_foreground_dark": raw_foreground_dark,
        "img_counter": img_counter,
        "raw_foreground": raw_foreground,
    }


def make_save_cal_pipeline(descriptor):
    gen_geo_cal = Stream()
    # TODO: move to independent saver factory
    # Save out calibration data to special place
    h_timestamp = Stream()
    (
        gen_geo_cal.pluck(0)
        .zip_latest(h_timestamp)
        .starsink(
            lambda x, y: _save_calib_param(
                x,
                y,
                os.path.join(
                    glbl_dict["config_base"],
                    "{}_{}".format(descriptor, glbl_dict["calib_config_name"]),
                ),
            )
        )
    )
    return {"gen_geo_cal": gen_geo_cal, "h_timestamp": h_timestamp}


def make_pipeline(descriptors=("primary", )):
    out = []
    general = make_general_pipeline()
    for desc in descriptors:
        raw_pipeline = make_raw_pipeline()
        ev_raw_pipeline = link(
            make_spec_pipeline(desc),
            raw_pipeline,
            make_save_cal_pipeline(desc),
        )
        out.append(link(general, ev_raw_pipeline))
    return out
