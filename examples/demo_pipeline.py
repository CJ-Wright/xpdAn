"""Example for XPD data"""

import copy

# pull from local data, not needed at beamline
import time

from bluesky.callbacks import CallbackBase
from databroker.broker import Broker
import matplotlib.pyplot as plt
from rapidz import Stream

from shed.translation import ToEventStream, FromEventStream, DBFriendly
# from shed.simple import SimpleToEventStream as ToEventStream, SimpleFromEventStream as FromEventStream
import numpy as np
from rapidz.link import link
from xpdan.db_utils import query_dark
from xpdan.pipelines.pipeline_utils import Filler
from xpdtools.tools import call_stream_element
from xpdtools.pipelines.demo_parallel import pipeline_order

from xpdview.callbacks import LiveWaterfall

from rapidz import identity


def start_level(raw_source: Stream, image_name: str, db: Broker, **kwargs):
    # composition = FromEventStream("start", ("composition_string",), raw_source)
    # wavelength = FromEventStream("start", ("bt_wavelength",), raw_source)
    geo_input = FromEventStream("start", ("calibration_md",), raw_source)
    fg_dark_query = FromEventStream("start", (), raw_source).map(
        query_dark, db=db
    )
    dark_docs = (
        fg_dark_query.filter(identity)
        .map(lambda x: x if not isinstance(x, list) else x[0])
        .map(getattr, "documents")
        .map(call_stream_element, fill=True)
        .flatten()
    )
    raw_foreground_dark = FromEventStream(
        "event", ("data", image_name), dark_docs
    ).map(np.float32)
    return locals()


def descriptor_level(**kwargs):
    return locals()


def event_level(raw_source: Stream, image_name: str, db: Broker, **kwargs):
    filler = Filler(db=db)
    # Fill the data from the broker
    source = raw_source.starmap(filler).filter(identity)
    raw_foreground = FromEventStream(
        "event",
        ("data", image_name),
        source,
        principle=True,
        event_stream_name="primary",
        stream_name="raw_foreground",
    ).map(np.float32)

    return locals()


def outputs(mean, q, **kwargs):
    # integrated intensities
    out_mean = ToEventStream(
        mean
        .combine_latest(q, emit_on=0),
        ("iq", "q"),
    )
    # out_mean.sink(lambda x: print('output', x))
    return locals()


def vis(out_mean, **kwargs):
    out_mean.starsink(
        LiveWaterfall(
            "q",
            "iq",
            units=("1/A", "Intensity"),
            window_title="{} vs {}".format("iq", "q"),
        ),
        stream_name="{} {} vis".format("q", "iq"),
    )
    return locals()


def inbound_scatter(geo_input, raw_foreground, raw_foreground_dark,
                    backend='thread', **kwargs):
    geo_input = geo_input.scatter(backend=backend)
    raw_foreground = raw_foreground.scatter(backend=backend)
    raw_foreground_dark = raw_foreground_dark.scatter(backend=backend)
    return locals()


def outbound_gather(mean, q, buffer_size=10, **kwargs):
    mean = mean.buffer(buffer_size).gather()
    q = q.buffer(buffer_size).gather()
    return locals()


def sanitize(n, d):
    print(n)
    if n != 'event':
        return n, d
    else:
        d2 = copy.deepcopy(d)
        d2['data'] = {k: list(v)[10] for k, v in d2['data'].items()}
        return n, d2


def run():
    db = Broker.named("live_demo_data")
    db.prepare_hook = lambda x, y: copy.deepcopy(y)
    raw_source = Stream()
    namespace = link(
        *[start_level, descriptor_level, event_level,
          inbound_scatter,
          *pipeline_order[:-1],
          outbound_gather,
          outputs,
          vis
          ],
        raw_source=raw_source, image_name='pe1_image', db=db,
        buffer_size=50
    )
    # print(namespace['out_mean'].futures)
    dbf = (namespace['out_mean']
           # .starmap(sanitize)
           # .DBFriendly()
           )
    # dbf.sink(print)
    L = dbf.sink_to_list()
    # db2 = Broker.named('temp')
    # dbf.starsink(db2.insert)
    db = Broker.named("live_demo_data")
    db.prepare_hook = lambda x, y: copy.deepcopy(y)
    sn = 50
    for e in db[-1].documents(fill=True):
        if e[0] == 'start':
            e[1].update(composition_string='EuTiO3')
        if e[0] == 'event' and vis:
            plt.pause(.1)
        if e[0] == 'event' and e[1]['seq_num'] > sn:
            raw_source.emit(('stop', db[-1].stop))
            break
        # print(e)
        raw_source.emit(e)
    # print(db2[-1].table())
    while len(L) < 3 + sn:
        print(len(L), 3 + sn)
        plt.pause(1)
    print(L[-1])
    print('done', len(L))
    assert len(L) == 3 + sn

    plt.show()

run()
