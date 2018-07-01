import os

import numpy as np
from shed.translation import FromEventStream
from streamz_ext import Stream
from xpdan.callbacks import StartStopCallback
from xpdan.db_utils import query_background, query_dark, temporal_prox
from xpdan.pipelines.pipeline_utils import (_timestampstr,
                                            clear_combine_latest, Filler)
from xpdconf.conf import glbl_dict
from xpdtools.calib import _save_calib_param
from xpdtools.pipelines.raw_pipeline import make_pipeline, mask_setting

pdf_pipeline = make_pipeline()
saxs_pipeline = make_pipeline()

image_name = glbl_dict['image_field']
db = glbl_dict['exp_db']
calibration_md_folder = {'folder': 'xpdAcq_calib_info.yml'}

filler = Filler(db=db)
# Build the general pipeline from the raw_pipeline
raw_source = Stream(stream_name='raw source')

# TODO: change this when new dark logic comes
# Check that the data isn't a dark
dk_uid = (FromEventStream('start', (), upstream=raw_source)
          .map(lambda x: 'sc_dk_field_uid' in x))
# Fill the raw event stream
source = (raw_source
          .combine_latest(dk_uid)
          .filter(lambda x: x[1])
          .pluck(0)
          # Filler returns None for resource/datum data
          .starmap(filler).filter(lambda x: x is not None))
# Get all the documents
start_docs = FromEventStream('start', (), source)
descriptor_docs = FromEventStream('descriptor', (), source,
                                  event_stream_name='primary')
event_docs = FromEventStream('event', (), source, event_stream_name='primary')
all_docs = (event_docs
            .combine_latest(start_docs, descriptor_docs, emit_on=0)
            .starmap(lambda e, s, d: {'raw_event': e, 'raw_start': s,
                                      'raw_descriptor': d,
                                      'human_timestamp': _timestampstr(
                                          s['time'])}))

# TODO: pull this from descriptor!
# If new calibration uid invalidate our current calibration cache
unique_geo = (
    FromEventStream('start', ('detector_calibration_client_uid',), source)
    .unique(history=1)
    )
wavelength = (FromEventStream('start', ('bt_wavelength',), source)
              .unique(history=1))
composition = FromEventStream('start', ('composition_string',), source)
dspacing = FromEventStream('start', ('dSpacing',), source).unique(history=1)
start_timestamp = FromEventStream('start', ('time',), source)
h_timestamp = start_timestamp.map(_timestampstr)

bg_query = (start_docs.map(query_background, db=db))
bg_docs = (bg_query
           .zip(start_docs)
           .starmap(temporal_prox)
           .filter(lambda x: x != [])
           .map(lambda x: x[0].documents(fill=True))
           .flatten())

# Get bg dark
bg_dark_query = (FromEventStream('start', (), bg_docs)
                 .map(query_dark, db=db))

fg_dark_query = (start_docs.map(query_dark, db=db))
fg_dark_query.filter(lambda x: x != [] and isinstance(x, list)).sink(print)
fg_dark_query.filter(lambda x: x == []).sink(lambda x: print('No dark found!'))

for p, descriptor in zip([pdf_pipeline, saxs_pipeline], ['primary', 'saxs']):
    unique_geo.map(lambda x: p['geometry_img_shape'].lossless_buffer.clear())

    # Clear composition every start document
    (start_docs
     .sink(lambda x: clear_combine_latest(p['iq_comp'], 1)))
    composition.connect(p['composition'])

    # Calibration information
    (wavelength.connect(p['wavelength']))
    (dspacing.connect(p['calibrant']))

    # TODO: pull from descriptor
    (FromEventStream('start', ('detector',), source)
     .unique(history=1)
     .connect(p['detector']))

    (FromEventStream('start', (), source).
     map(lambda x: 'detector_calibration_server_uid' in x).
     connect(p['is_calibration_img']))

    # TODO: pull this from the descriptor
    # Only pass through new calibrations (prevents us from recalculating cals)
    (FromEventStream('start', ('calibration_md',), source).
     unique(history=1).
     connect(p['geo_input']))

    # Clean out the cached darks and backgrounds on start
    # so that this will run regardless of background/dark status
    # note that we get the proper data (if it exists downstream)
    start_docs.sink(lambda x: p['raw_background_dark'].emit(0.0))
    start_docs.sink(lambda x: p['raw_background'].emit(0.0))
    start_docs.sink(lambda x: p['raw_foreground_dark'].emit(0.0))

    (FromEventStream('event', ('data', image_name),
                     bg_dark_query.map(lambda x: x[0].documents(fill=True))
                     .flatten(), event_stream_name=descriptor).map(np.float32)
     .connect(p['raw_background_dark']))

    # Get background
    (FromEventStream('event', ('data', image_name), bg_docs,
                     event_stream_name=descriptor).map(np.float32)
     .connect(p['raw_background']))

    # Get foreground dark
    (FromEventStream('event', ('data', image_name),
                     fg_dark_query.filter(lambda x: x != [])
                     .map(lambda x: x if not isinstance(x, list) else x[0])
                     .map(lambda x: x.documents(fill=True)).flatten(),
                     event_stream_name=descriptor
                     ).map(np.float32)
     .connect(p['raw_foreground_dark']))

    # Pull dark live if possible
    # NOTE: THIS ASSUMES THAT THE DARK STREAMS ARE NAMED saxs_dark
    (FromEventStream('event', ('data', image_name),
                     event_stream_name='{}_{}'.format(descriptor, 'dark')
                     )
     .map(np.float32)
     .connect(p['raw_foreground_dark']))

    # Get foreground
    FromEventStream('event', ('seq_num',), source, stream_name='seq_num',
                    event_stream_name=descriptor
                    ).connect(p['img_counter'])
    (FromEventStream('event', ('data', image_name), source, principle=True,
                     stream_name='raw_foreground',
                     event_stream_name=descriptor).map(np.float32)
     .connect(p['raw_foreground']))

    # TODO: save this in such a way that we can get at it for each calibration
    # Save out calibration data to special place
    (p['gen_geo_cal'].pluck(0)
     .zip_latest(h_timestamp)
     .starsink(lambda x, y: _save_calib_param(x, y, os.path.join(
        glbl_dict['config_base'],
        '{}_{}'.format(descriptor, glbl_dict['calib_config_name'])))))

raw_source.starsink(StartStopCallback())
# raw_source.visualize(os.path.expanduser('~/mystream_new.png'),
#                      source_node=True)
