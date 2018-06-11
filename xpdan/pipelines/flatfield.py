import os

from bluesky.callbacks.broker import LiveImage
from shed.translation import FromEventStream
from xpdan.callbacks import StartStopCallback
from xpdan.pipelines.pipeline_utils import (_timestampstr,
                                            Filler)
from xpdconf.conf import glbl_dict
from xpdconf.conf import XPD_SHUTTER_CONF
from xpdtools.calib import _save_calib_param
from xpdtools.pipelines.flatfield import *

image_name = glbl_dict['image_field']
shutter_name = glbl_dict['shutter_field']
db = glbl_dict['exp_db']
calibration_md_folder = {'folder': 'xpdAcq_calib_info.yml'}

filler = Filler(db=db)
# Build the general pipeline from the raw_pipeline
raw_source = Stream(stream_name='raw source')

# Fill the raw event stream
source = (raw_source
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

# If new calibration uid invalidate our current calibration cache
(FromEventStream('start', ('detector_calibration_client_uid',), source)
 .unique(history=1)
 .map(lambda x: geometry_img_shape.lossless_buffer.clear()))

# Calibration information
(FromEventStream('start', ('bt_wavelength',), source)
 .unique(history=1)
 .connect(wavelength))
(FromEventStream('start', ('dSpacing',), source)
 .unique(history=1)
 .connect(calibrant))
(FromEventStream('start', ('detector',), source)
 .unique(history=1)
 .connect(detector))

(FromEventStream('start', (), source).
 map(lambda x: 'detector_calibration_server_uid' in x).
 connect(is_calibration_img))
# Only pass through new calibrations (prevents us from recalculating cals)
(FromEventStream('start', ('calibration_md',), source).
 unique(history=1).
 connect(geo_input))

start_timestamp = FromEventStream('start', ('time',), source)

# Clean out the cached darks and backgrounds on start
# so that this will run regardless of background/dark status
# note that we get the proper data (if it exists downstream)
start_docs.sink(lambda x: raw_background_dark.emit(0.0))
start_docs.sink(lambda x: raw_background.emit(0.0))
start_docs.sink(lambda x: raw_foreground_dark.emit(0.0))

# Get foreground dark
((FromEventStream('event', ('data', image_name), raw_source,
                  event_stream_name='dark')
  .map(np.float32)
  .connect(raw_foreground_dark)))

# Get foreground
FromEventStream('event', ('seq_num',), source, stream_name='seq_num',
                event_stream_name='primary'
                ).connect(img_counter)
(FromEventStream('event', ('data', image_name), source, principle=True,
                 event_stream_name='primary',
                 stream_name='raw_foreground')
 .map(np.float32)
 .connect(raw_foreground))

# Save out calibration data to special place
h_timestamp = start_timestamp.map(_timestampstr)
(gen_geo_cal.pluck(0)
 .zip_latest(h_timestamp)
 .starsink(lambda x, y: _save_calib_param(x, y, os.path.join(
    glbl_dict['config_base'], glbl_dict['calib_config_name']))))

raw_source.starsink(StartStopCallback())

ave_ff.map(np.nan_to_num).sink(
    LiveImage('image', cmap='viridis',
              limit_func=lambda x: (np.nanpercentile(x, .1),
                                    np.nanpercentile(x, 99.9)),
              #                   norm=SymLogNorm(.1),
              window_title='percent off').update)
