import operator as op

from rapidz import Stream, move_to_first
from shed import SimpleToEventStream, SimpleFromEventStream
from xpdan.callbacks import StartStopCallback
from xpdan.db_utils import query_dark, query_flat_field
from xpdan.vend.callbacks.core import StripDepVar
import numpy as np
from xpdconf.conf import glbl_dict


def pencil_tomo(source: Stream, qoi_name, translation, rotation, stack=None,
                **kwargs):
    """Extract data from a raw stream for pencil beam tomography

    Parameters
    ----------
    source : Stream
        The stream of raw event model data
    qoi_name : str
        The name of the QOI for this reconstruction
    kwargs

    Returns
    -------
    dict :
        The namespace
    """
    start = SimpleFromEventStream('start', (), upstream=source)
    if stack:
        stack_position = SimpleFromEventStream("event", ("data", stack),
                                               upstream=source)
    x = SimpleFromEventStream("event", ("data", translation), upstream=source)
    th = SimpleFromEventStream("event", ("data", rotation), upstream=source)

    # Extract the index for the translation and rotation so we can
    # extract the dimensions and extents
    # TODO: turn into proper function
    translation_position = SimpleFromEventStream(
        "start", ("motors",), upstream=source
    ).map(lambda x: x.index(translation))
    rotation_position = SimpleFromEventStream(
        "start", ("motors",), upstream=source
    ).map(lambda x: x.index(rotation))

    dims = SimpleFromEventStream("start", ("shape",), upstream=source)
    th_dim = dims.zip(rotation_position).starmap(op.getitem)
    x_dim = dims.zip(translation_position).starmap(op.getitem)

    extents = SimpleFromEventStream("start", ("extents",), upstream=source)
    th_extents = extents.zip(rotation_position).starmap(op.getitem)
    x_extents = extents.zip(translation_position).starmap(op.getitem)

    qoi = SimpleFromEventStream(
        "event", ("data", qoi_name), upstream=source, principle=True
    )
    center = SimpleFromEventStream(
        "start", ("tomo", "center"), upstream=source
    )
    source.starsink(StartStopCallback())
    return locals()


def full_field_tomo(source: Stream, qoi_name, rotation, db=glbl_dict["exp_db"],
                    **kwargs):
    theta = SimpleFromEventStream(
        "event", ("data", rotation), upstream=source
    ).map(np.deg2rad)

    img = SimpleFromEventStream(
        "event", ("data", qoi_name), upstream=source, principle=True
    )
    center = SimpleFromEventStream(
        "start", ("tomo", "center"), upstream=source
    )

    # extract the dark and flat field data
    start_docs = SimpleFromEventStream("start", ())
    dark_query = start_docs.map(query_dark, db=db)
    dark_query.filter(lambda x: x == []).sink(
        lambda x: print("No dark found!")
    )
    flat_field_query = start_docs.map(query_flat_field, db=db)
    flat_field_query .filter(lambda x: x == []).sink(
        lambda x: print("No flat field found!")
    )
    dark = SimpleFromEventStream(
                "event",
                ("data", qoi_name),
                dark_query.filter(lambda x: x != [])
                    .map(lambda x: x if not isinstance(x, list) else x[0])
                    .map(lambda x: x.documents(fill=True))
                    .flatten(),
                event_stream_name="primary",
    ).map(np.float32)
    flat_field = SimpleFromEventStream(
                "event",
                ("data", qoi_name),
                flat_field_query.filter(lambda x: x != [])
                    .map(lambda x: x if not isinstance(x, list) else x[0])
                    .map(lambda x: x.documents(fill=True))
                    .flatten(),
                event_stream_name="primary",
    ).map(np.float32)

    source.starsink(StartStopCallback())
    return locals()


def tomo_event_stream(
    source, rec, sinogram, *, qoi_name, rec_3D=None, **kwargs
):
    raw_stripped = move_to_first(source.starmap(StripDepVar()))

    rec_tes = SimpleToEventStream(
        rec, (f"{qoi_name}_tomo",), analysis_stage="{}_tomo".format(qoi_name)
    )
    # If we have a 3D reconstruction translate it
    if rec_3D:
        rec_3D_tes = SimpleToEventStream(
            rec_3D,
            (f"{qoi_name}_tomo_3D",),
            analysis_stage="{}_tomo_3D".format(qoi_name),
        )

    # Don't run the sinogram for now, since it can produce issues with the viz
    sinogram_tes = SimpleToEventStream(
        sinogram,
        (f"{qoi_name}_sinogram",),
        analysis_stage="{}_sinogram".format(qoi_name),
    )

    return locals()
