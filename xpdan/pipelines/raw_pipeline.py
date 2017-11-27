from streamz import Stream
import operator as op

from skbeam.core.utils import q_to_twotheta
from bluesky.callbacks.broker import LiveImage
# from matplotlib.colors import LogNorm, SymLogNorm

from xpdan.tools import *
from xpdan.calib import img_calibration, _save_calib_param
from xpdan.dev_utils import _timestampstr

vis_d = {'Background Corrected Image': LiveImage(
    'img', cmap='viridis',
    window_title='Background Corrected Image'),
    'Masked Corrected Image': LiveImage(
        'img', cmap='viridis',
        window_title='Masked Corrected Image',
        limit_func=lambda im: (
            np.nanpercentile(im, 1),
            np.nanpercentile(im, 99)),
        # norm=LogNorm()
    ),
    'Z Scored Image': LiveImage(
        'img', cmap='viridis',
        window_title='Z Scored Image',
        # norm=SymLogNorm(1)
    )
}
vis = True

mask_kwargs = {}
fq_kwargs = {}
pdf_kwargs = {}

raw_foreground = Stream(stream_name='raw foreground')
raw_foreground_dark = Stream(stream_name='raw foreground dark')

raw_background = Stream(stream_name='raw background')
raw_background_dark = Stream(stream_name='raw background dark')

composition = Stream(stream_name='composition')

dark_corrected_foreground = (raw_foreground.
                             zip_latest(raw_foreground_dark).
                             map(op.sub))

dark_corrected_background = (raw_background.
                             zip_latest(raw_background_dark).
                             map(op.sub))

background_corrected_img = (dark_corrected_foreground.
                            zip_latest(dark_corrected_background).
                            map(op.sub,
                                stream_name='background corrected img'))
if vis:
    background_corrected_img.sink(vis_d['Background Corrected Image'].update)

is_calibration_img = Stream(stream_name='Is Calibration')

gated_cal = (background_corrected_img.
             zip_latest(is_calibration_img).
             filter(lambda a: bool(a[1])).
             pluck(0, stream_name='Gate calibration'))

wavelength = Stream(stream_name='wavelength')
calibrant = Stream(stream_name='calibrant')
detector = Stream(stream_name='detector')

gen_geo_cal = (gated_cal.
               zip_latest(wavelength,
                          calibrant,
                          detector).
               map(img_calibration))

h_timestamp = Stream(stream_name='start timestamp').map(_timestampstr)

gen_geo_cal.zip_latest(h_timestamp).sink(_save_calib_param)
gen_geo = gen_geo_cal.pluck(1)

geometry = (Stream(stream_name='geometry').
            map(load_geo).
            union(gen_geo, stream_name='Combine gen and load cal'))

pol_corrected_img = (background_corrected_img.
                     zip_latest(geometry).
                     map(lambda a, **kwargs: polarization_correction(*a,
                                                                     **kwargs),
                         .99,
                         stream_name='corrected image')
                     )
mask = (pol_corrected_img.
        zip_latest(geometry).
        map(mask_img, stream_name='mask', **mask_kwargs))

(pol_corrected_img.
 zip_latest(mask).
 map(lambda a: overlay_mask(*a)).
 sink(vis_d['Masked Corrected Image'].update))

binner = (mask.zip_latest(pol_corrected_img, geometry).map(generate_binner))

img_binner = pol_corrected_img.zip_latest(binner)

iq = img_binner.map(integrate, stream_name='I(Q)')

tth = iq.pluck(0).zip_latest(wavelength).map(lambda a: q_to_twotheta(*a),
                                             stream_name='tth')

iq_comp = iq.zip_latest(composition)

fq = iq_comp.map(lambda a, **kwargs: fq_getter(x=a[0], y=a[1],
                                               composition=a[2],
                                               **kwargs),
                 stream_name='fq',
                 **fq_kwargs)

pdf = iq_comp.map(lambda a, **kwargs: fq_getter(x=a[0], y=a[1],
                                                composition=a[2],
                                                **kwargs),
                  stream_name='pdf',
                  **pdf_kwargs)

z_score = img_binner.map(z_score_image, stream_name='z score')
z_score.sink(vis_d['Z Scored Image'].update)

raw_foreground.visualize('/home/christopher/mystream.png', source_node=True)
