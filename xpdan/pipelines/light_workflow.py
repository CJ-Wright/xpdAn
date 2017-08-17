"""Example for XPD data"""
from operator import add, sub, truediv

import shed.event_streams as es
from streams.core import Stream

from databroker import db
from xpdan.db_utils import query_dark, query_background, temporal_prox
from xpdan.tools import (better_mask_img, iq_to_pdf, pull_array,
                         generate_binner, z_score_image, integrate,
                         polarization_correction, load_geo, event_count)
from xpdan.glbl import an_glbl
import os
from xpdan.db_utils import _timestampstr
import tifffile
from skbeam.io.fit2d import fit2d_save
from skbeam.io.save_powder_output import save_output

source = Stream(name='Raw')

# foreground logic
fg_dark_stream = es.QueryUnpacker(db, es.Query(db, source,
                                               query_function=query_dark,
                                               query_decider=temporal_prox,
                                               name='Query for FG Dark'))

dark_sub_fg = es.map(sub,
                     es.zip(source,
                            fg_dark_stream),
                     input_info={'img1': ('pe1_image', 0),
                                 'img2': ('pe1_image', 1)},
                     output_info=[('img', {'dtype': 'array',
                                           'source': 'testing'})],
                     name='Dark Subtracted Foreground',
                     analysis_stage='dark_sub'
                     )

bg_query_stream = es.Query(db, source,
                           query_function=query_background,
                           query_decider=temporal_prox,
                           name='Query for Background')

bg_stream = es.QueryUnpacker(db, bg_query_stream)
bg_dark_stream = es.QueryUnpacker(db, es.Query(db, bg_stream,
                                               query_function=query_dark,
                                               query_decider=temporal_prox,
                                               name='Query for BG Dark'))

# Perform dark subtraction on everything
dark_sub_bg = es.map(sub,
                     es.zip(bg_stream, bg_dark_stream),
                     input_info={'img1': ('pe1_image', 0),
                                 'img2': ('pe1_image', 1)},
                     output_info=[('img', {'dtype': 'array',
                                           'source': 'testing'})])

# bundle the backgrounds into one stream
bg_bundle = es.BundleSingleStream(dark_sub_bg, bg_query_stream,
                                  name='Background Bundle')

# sum the backgrounds
summed_bg = es.accumulate(add, bg_bundle, start=pull_array,
                          state_key='img1',
                          input_info={'img2': 'img'},
                          output_info=[('img', {
                              'dtype': 'array',
                              'source': 'testing'})])

count_bg = es.accumulate(event_count, bg_bundle, start=1,
                         state_key='count',
                         output_info=[('count', {
                             'dtype': 'int',
                             'source': 'testing'})])

ave_bg = es.map(truediv, es.zip(summed_bg, count_bg),
                input_info={'0': ('img', 0), '1': ('count', 1)},
                output_info=[('img', {
                    'dtype': 'array',
                    'source': 'testing'})],
                # name='Average Background'
                )

# combine the fg with the summed_bg
fg_bg = es.combine_latest(dark_sub_fg, ave_bg, emit_on=dark_sub_fg)

# subtract the background images
fg_sub_bg = es.map(sub,
                   fg_bg,
                   input_info={'img1': ('img', 0),
                               'img2': ('img', 1)},
                   output_info=[('img', {'dtype': 'array',
                                         'source': 'testing'})],
                   # name='Background Corrected Foreground'
                   )

# make/get calibration stream
cal_md_stream = es.Eventify(source, start_key='calibration_md',
                            output_info=[('calibration_md',
                                          {'dtype': 'dict',
                                           'source': 'workflow'})],
                            md=dict(name='Calibration'))
cal_stream = es.map(load_geo, cal_md_stream,
                    input_info={'cal_params': 'calibration_md'},
                    output_info=[('geo',
                                  {'dtype': 'object', 'source': 'workflow'})])

# polarization correction
# SPLIT INTO TWO NODES
pfactor = .99
p_corrected_stream = es.map(polarization_correction,
                            es.zip_latest(fg_sub_bg, cal_stream),
                            input_info={'img': ('img', 0),
                                        'geo': ('geo', 1)},
                            output_info=[('img', {'dtype': 'array',
                                                  'source': 'testing'})],
                            polarization_factor=pfactor)

# generate masks
mask_kwargs = {'bs_width': None}
mask_stream = es.map(better_mask_img,
                     es.zip_latest(p_corrected_stream,
                                   cal_stream),
                     input_info={'img': ('img', 0),
                                 'geo': ('geo', 1)},
                     output_info=[('mask', {'dtype': 'array',
                                            'source': 'testing'})],
                     **mask_kwargs)

# generate binner stream
binner_stream = es.map(generate_binner,
                       cal_stream,
                       input_info={'geo': 'geo'},
                       output_info=[('binner', {'dtype': 'function',
                                                'source': 'testing'})],
                       img_shape=(2048, 2048))

iq_stream = es.map(integrate,
                   es.zip_latest(p_corrected_stream,
                                 binner_stream),
                   input_info={'img': ('img', 0),
                               'binner': ('binner', 1)},
                   output_info=[('iq', {'dtype': 'array',
                                        'source': 'testing'})])

# z-score the data
z_score_stream = es.map(z_score_image,
                        es.zip_latest(p_corrected_stream,
                                      binner_stream),
                        input_info={'img': ('img', 0),
                                    'binner': ('binner', 1)},
                        output_info=[('z_score_img', {'dtype': 'array',
                                                      'source': 'testing'})])

pdf_stream = es.map(iq_to_pdf, es.zip(iq_stream, source))

# write to human readable files

# base string
light_template = os.path.join(
    an_glbl['tiff_base'],
    '{sample_name}/{folder_tag}/{{{analysis_stage}}}/'
    '{{human_timestamp}}{{auxiliary}}{{{{ext}}}}')

eventify_raw = es.eventify(source)


# format base string with data from experiment
# sample_name, folder_tag
def templater1_func(doc, template):
    d = {'sample_name': doc.get('sample_name', ''),
         'folder_tag': doc.get('folder_tag', '')}
    return template.format(**d)


template_stream_1 = es.map(templater1_func, eventify_raw, light_template,
                           full_event=True,
                           output_info=[('template', {'dtype': 'str'})])


# format with auxiliary and time
def templater2_func(doc, template, aux=None):
    if aux is None:
        aux = ['temperature', 'diff_x', 'diff_y', 'eurotherm']
    return template.format(
        # Change to include name as well
        auxiliary='_'.join([doc['data'].get(a, '') for a in aux]),
        human_timestamp=_timestampstr(doc['time'])
    )


template_stream_2 = es.map(templater2_func,
                           es.zip_latest(source,
                                         template_stream_1),
                           output_info=[('template', {'dtype': 'str'})])


# further format with data from analysis stage
def templater3_func(doc, template):
    return template.format(doc.get('analysis_stage', 'raw'))


eventifies = [eventify_raw] + [es.eventify(s) for s in
                               [dark_sub_fg,
                                mask_stream,
                                iq_stream,
                                pdf_stream]]

templater_streams_3 = [es.map(templater3_func,
                              es.zip_latest(template_stream_2, e),
                              full_event=True,
                              output_info=[('template',
                                            {'dtype': 'str'})]
                              ) for e in eventifies]


# write and format with ext
def writer_templater_tiff(img, template):
    template.format(ext='.tiff')
    tifffile.imsave(template, img)
    return template


def writer_templater_mask(mask, template):
    template.format(ext='.msk')
    fit2d_save(mask, template)
    return template


def writer_templater_chi(x, y, template):
    template.format(ext='_Q.chi')
    save_output(x, y, template, 'Q', ext='')
    return template


# TODO: need tth writer
# https://github.com/scikit-beam/scikit-beam/blob/master/skbeam/core/utils.py#L1054


def writer_templater_pdf(x, y, template):
    template.format(ext='.gr')
    pdf_saver(x, y, template)
    return template


def writer_templater_fq(q, fq, template):
    template.format(ext='.fq')
    fq_saver(r, pdf, template)
    return template


iis = [
    {'img': ('pe1_image', 0), 'template': ('template', 1)},
    {'img': ('img', 0), 'template': ('template', 1)},
    {'mask': ('mask', 0), 'template': ('template', 1)},
    {'x': ('q', 0), 'y': ('iq', 1), 'template': ('template', 1)},
    {'x': ('r', 0), 'y': ('pdf', 1), 'template': ('template', 1)},
]

writer_streams = [
    es.map(writer_templater,
           es.zip_latest(s1, s2),
           input_info=ii,
           output_info=[('final_filename',
                         {'dtype': 'str'})],
           ) for s1, s2, ii, writer_templater in
    zip(
        [source, dark_sub_fg, mask_stream,
         iq_stream, pdf_stream],
        templater_streams_3,
        iis,
        [writer_templater_tiff, writer_templater_tiff, writer_templater_mask,
         writer_templater_chi, writer_templater_pdf]
    )]