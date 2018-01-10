from shed.translation import FromEventStream, ToEventStream
from skbeam.io import save_output
from skbeam.io.fit2d import fit2d_save
from xpdtools.pipelines.raw_pipeline import *
from bluesky.callbacks.broker import LiveImage
# from bluesky.callbacks import LiveWaterfall
from bluesky.callbacks import CallbackBase
from xpdview.callbacks import LiveWaterfall
from xpdtools.calib import _save_calib_param
from xpdan.pipelines.pipeline_utils import _timestampstr
from collections import Iterable
from tifffile import imsave
from xpdan.formatters import render_and_clean
from xpdan.pipelines.pipeline_utils import base_template
from xpdan.db_utils import query_background, query_dark, temporal_prox

image_name = 'pe1_image'
db = 'db'
mask_kwargs = {}
fq_kwargs = {}
pdf_kwargs = {}


# TODO: move this to SHED?
def clear_combine_latest(node, position=None):
    if position is None:
        position = range(len(node.last))
    elif not isinstance(position, Iterable):
        position = (position,)
    for p in position:
        node.last[p] = None
        node.missing.add(node.upstreams[p])


# TODO: move this to SHED?
class Filler(CallbackBase):
    """Fill events without provenence"""

    def __init__(self, db):
        self.db = db
        self.descs = None

    def start(self, docs):
        self.descs = []
        return 'start', docs

    def descriptor(self, docs):
        self.descs.append(docs)
        return 'descriptor', docs

    def event(self, docs):
        d = next(self.db.fill_events([docs], self.descs))
        return 'event', d

    def stop(self, docs):
        return 'stop', docs


filler = Filler(db=db)
# Build the general pipeline from the raw_pipeline
raw_source = Stream(stream_name='raw source')
# Fill the raw event stream
source = raw_source.starmap(filler)

# If new calibration uid invalidate our current calibration cache
(FromEventStream(source, 'start', ('detector_calibration_client_uid',))
    .unique(history=1)
    .map(lambda x: geometry_img_shape.lossless_buffer.clear()))

# Clear composition every start document
(FromEventStream(source, 'start', ())
    .sink(lambda x: clear_combine_latest(iq_comp, 1)))
FromEventStream(source, 'start', ('composition_string',)).connect(composition)

# Calibration information
(FromEventStream(source, 'start', ('bt_wavelength',))
    .unique(history=1)
    .connect(wavelength))
(FromEventStream(source, 'start', ('calibrant',))
    .unique(history=1)
    .connect(calibrant))
(FromEventStream(source, 'start', ('detector',))
    .unique(history=1)
    .connect(detector))

(FromEventStream(source, 'start', ()).
    map(lambda x: 'detector_calibration_server_uid' in x).
    connect(is_calibration_img))
# Only pass through new calibrations (prevents us from recalculating cals)
(FromEventStream(source, 'start', ('calibration_md',)).
    unique(history=1).
    connect(geo_input))

start_timestamp = FromEventStream(source, 'start', ('time',))

start_docs = FromEventStream(source, 'start', ())
bg_query = (start_docs.map(query_background, db=db))
bg_docs = (bg_query
    .zip(start_docs)
    .starmap(temporal_prox)
    .filter(lambda x: x is not None)
    .map(lambda x: x.documents(fill=True))
    .flatten())

# Get bg dark
bg_dark_query = (bg_docs
    .FromEventStream('start', ())
    .map(query_dark, db=db)
    )
(bg_dark_query.map(lambda x: x.documents(fill=True)).flatten()
    .FromEventStream('event', ('data', image_name))
    .connect(raw_background_dark))

# Get background
bg_docs.FromEventStream('event', ('data', image_name)).connect(raw_background)

# Get foreground dark
fg_dark_query = (start_docs
    .map(query_dark, db=db))
(fg_dark_query
    .map(lambda x: x.documents(fill=True)).flatten()
    .FromEventStream('event', ('data', image_name))
    .connect(raw_foreground_dark))

# Get foreground
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

# TODO: edit the masking pipeline to optionally mask on first image only
'''
# Write things to disk
# create filename string
descriptor_docs = FromEventStream(source, 'descriptor', (),
                                  event_stream_name='primary')
event_docs = FromEventStream(source, 'event', (), event_stream_name='primary')
all_docs = (event_docs
    .combine_latest(start_docs, descriptor_docs, emit_on=0)
    .starmap(lambda e, s, d: {'raw_event': e, 'raw_start': s,
                              'raw_descriptor': d})
    )
# TODO: format string (with partial/clean formatters)
filename_node = Stream()

# dark corrected img
(dark_corrected_foreground
    .zip(
    all_docs.map(lambda x: x.update(ext='.tiff', analyzed_start={
        'analysis_stage': 'dark_sub'}))
        .map(lambda x, string: render_and_clean(string, **x),
             string=base_template)
)
    .sink(lambda img, s: imsave(s, img),
          stream_name='dark corrected foreground'))

# integrated intensities
for xx, unit, x_ext in zip([q, tth], ['Q', 'tth'], ['', '_tth']):
    for y, y_ext, y_name in zip([mean,
                                 median, std
                                 ],
                                ['',
                                 # '_median', '_std'
                                 ],
                                ['mean',
                                 # 'median', 'std'
                                 ]):
        (y.zip(xx).zip(
            all_docs.map(lambda x: x.update(ext='{} {}'.format(x_ext, y_ext),
                                            analyzed_start={
                                                'analysis_stage': 'dark_sub'}))
                .map(lambda x, string: render_and_clean(string, **x),
                     string=base_template)
        ).map(lambda l: (*l[0], l[1]))
            .sink(lambda x: save_output(x[1], x[0], x[2], unit),
                  stream_name='save integration {}, {}'.format(unit, y_name)))

# Mask
mask.zip_latest(filename_node).sink(lambda x: fit2d_save(np.flipud(x[0]),
                                                         x[1]))
mask.zip_latest(filename_node).sink(lambda x: np.save(x[1] + '_mask.npy',
                                                      x[0]))
# PDF
# F(Q)
# S(Q)
'''
# Visualization
# background corrected img
em_background_corrected_img = ToEventStream(bg_corrected_img,
                                            ('image',)).starsink(
    LiveImage('image', window_title='Background_corrected_img',
              cmap='viridis'))

# polarization corrected img with mask overlayed
em_pol_corrected_img = ToEventStream(
    pol_corrected_img.combine_latest(mask).starmap(overlay_mask),
    ('image',)).starsink(LiveImage('image', window_title='final img',
                                   limit_func=lambda im: (
                                       np.nanpercentile(im, 2.5),
                                       np.nanpercentile(im, 97.5)
                                   ), cmap='viridis'))

# integrated intensities
# stash this into a variable so the nodes can be ``destroy``ed if user doesn't
# want them
waterfalls = {}
for x, n in zip([q, tth], ['q', 'tth']):
    for y, nn in zip([mean,
                      # median, std
                      ],
                     ['mean',
                      # 'median', 'std'
                      ]):
        a = ToEventStream(y.map(np.nan_to_num).combine_latest(x, emit_on=0),
                          (nn, n,))
        waterfalls['{}_{}'.format(n, nn)] = (
            a.starsink(
                LiveWaterfall(n, nn, window_title='{} vs {}'.format(n, nn)),
                stream_name='{} {} vis'.format(n, nn))
        )

# F(Q)
ToEventStream(fq, ('q', 'fq')).starsink(LiveWaterfall('q', 'fq'),
                                        stream_name='F(Q) vis')

# PDF
ToEventStream(pdf, ('r', 'gr')).starsink(LiveWaterfall('r', 'gr'),
                                         stream_name='G(r) vis')

# Zscore
ToEventStream(z_score, ('z_score',)).starsink(
    LiveImage('z_score', cmap='viridis', window_title='z score',
              limit_func=lambda im: (-2, 2)), stream_name='z score vis')

source.visualize('/home/christopher/mystream.png', source_node=True)
