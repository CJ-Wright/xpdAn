from shed.translation import FromEventStream, ToEventStream
from xpdtools.pipelines.raw_pipeline import *
from bluesky.callbacks.broker import LiveImage
from bluesky.callbacks import LiveWaterfall
from xpdtools.calib import _save_calib_param
from xpdan.pipelines.pipeline_utils import _timestampstr

image_name = 'pe1_image'
db = 'db'
adb = 'adb'
mask_kwargs = {}
fq_kwargs = {}
pdf_kwargs = {}
# Build the general pipeline from the streamz chunks

source = Stream(stream_name='raw source')

# send data to streamz pipeline
# When we change the calibration setup clear all the data waiting for a cal
(FromEventStream(source, 'start', ('detector_calibration_client_uid', )).
    unique().map(lambda x: pol_corrected_img_zip.lossless_buffer.clear()))
FromEventStream(source, 'start', ('composition_string', )).connect(composition)
FromEventStream(source, 'start', ('wavelength', )).connect(wavelength)
FromEventStream(source, 'start', ('calibrant', )).connect(calibrant)
FromEventStream(source, 'start', ('detector', )).connect(detector)
# Only pass through new calibrations (prevents us from recalculating cals)
(FromEventStream(source, 'start', ('calibration_md', )).
    unique(history=1).
    connect(geo_input))
(FromEventStream(source, 'start', ()).
    map(lambda x: 'detector_calibration_server_uid' in x).
    connect(is_calibration_img))

start_timestamp = FromEventStream(source, 'start', ('time', ))

FromEventStream(source, 'event', ('data', image_name),
                stream_name='raw_background_dark').connect(raw_background_dark)
FromEventStream(source, 'event', ('data', image_name),
                stream_name='raw_background').connect(raw_background)
FromEventStream(source, 'event', ('data', image_name),
                stream_name='raw_foreground_dark').connect(raw_foreground_dark)
FromEventStream(source, 'event', ('data', image_name), principle=True,
                stream_name='raw_foreground').connect(raw_foreground)

# Save out calibration data to special place
h_timestamp = start_timestamp.map(_timestampstr)
gen_geo_cal.zip_latest(h_timestamp).sink(_save_calib_param)

# edit mask, fq, and pdf kwargs
for kwargs, node in zip([mask_kwargs, fq_kwargs, pdf_kwargs], [mask, fq, pdf]):
    # Don't mutate the base values
    node.kwargs = dict(node.kwargs)
    node.kwargs.update(kwargs)

em_background_corrected_img = bg_corrected_img.ToEventStream(('image',))
em_background_corrected_img.sink(LiveImage('image'))

em_gen_geo = gen_geo.ToEventStream(('geometry',))
# em_gen_geo.sink(db.insert)

# XXX: include median and std and tth
em_iq = q.zip(mean).ToEventStream(('q', 'mean',))
em_iq.sink(LiveWaterfall('q', 'mean'))

sfq = fq.map(np.sum)

em_fq = fq.ToEvenStream(('q', 'fq'))
em_fq.sink(LiveWaterfall('q', 'fq'))

em_pdf = pdf.ToEventStream(('r', 'gr'))
em_pdf.sink(LiveWaterfall('r', 'gr'))

em_z_score = z_score.ToEventStream(('z_score', ))
em_z_score.sink(LiveImage('z_score'))

source.visualize('/home/christopher/mystream.png', source_node=True)
