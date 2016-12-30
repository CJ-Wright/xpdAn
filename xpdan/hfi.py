"""Header Function Interfaces for data processing"""
##############################################################################
#
# xpdan            by Billinge Group
#                   Simon J. L. Billinge sb2896@columbia.edu
#                   (c) 2016 trustees of Columbia University in the City of
#                        New York.
#                   All rights reserved
#
# File coded by:    Christopher J. Wright
#
# See AUTHORS.txt for a list of people who contributed.
# See LICENSE.txt for license information.
#
##############################################################################
from redsky.streamer import db_store_single_resource_single_file
import traceback
from time import time
from uuid import uuid4

import numpy as np

from .tools import *
from pyFAI import AzimuthalIntegrator
import sys
import inspect
from pprint import pprint
from itertools import chain
import types


# -1. Wavelength Calibration (Pending @sghose)
def spoof_wavelength_calibration_hfi(stream, *args,
                                     calibration_field='wavelength',
                                     **kwargs):
    output_field_name = 'wavelength'
    _, start_doc = next(stream)
    run_start_uid = str(uuid4())
    new_start_doc = dict(
        uid=run_start_uid, time=time(),
        parents=[start_doc['uid']],
        hfi=spoof_wavelength_calibration_hfi.__name__,
        provenance=dict(
            hfi_module=inspect.getmodule(spoof_wavelength_calibration_hfi).__name__,
            hfi=spoof_wavelength_calibration_hfi.__name__,
            process_module='spoof',
            process='spoof',
            kwargs=kwargs,
            args=args),
    )  # More provenance to be defined (eg environment)
    results = start_doc[calibration_field]
    yield 'start', new_start_doc

    img_dict = dict(source='testing', dtype='float', )
    new_descriptor = dict(uid=str(uuid4()), time=time(),
                          run_start=run_start_uid,
                          data_keys={output_field_name: img_dict})
    yield 'descriptor', new_descriptor

    exit_md = None
    new_event = dict(uid=str(uuid4()), time=time(), timestamps={},
                     descriptor=new_descriptor['uid'],
                     data={output_field_name: results},
                     seq_num=0)
    yield 'event', new_event

    if exit_md is None:
        exit_md = {'exit_status': 'success'}
    new_stop = dict(uid=str(uuid4()), time=time(),
                    run_start=run_start_uid, **exit_md)
    yield 'stop', new_stop


# 0. Detector Calibration
def spoof_detector_calibration_hfi(stream, *args,
                                   calibration_field='calibration_md',
                                   **kwargs):
    output_field_name = 'detector_calibration'
    _, start_doc = next(stream)
    run_start_uid = str(uuid4())
    new_start_doc = dict(
        uid=run_start_uid, time=time(),
        parents=[start_doc['uid'], ],
        hfi=spoof_detector_calibration_hfi.__name__,
        provenance=dict(
            hfi_module=inspect.getmodule(
                spoof_detector_calibration_hfi).__name__,
            hfi=spoof_detector_calibration_hfi.__name__,
            process_module='spoof',
            process='spoof',
            kwargs=kwargs,
            args=args),
    )  # More provenance to be defined (eg environment)
    results = start_doc[calibration_field]
    yield 'start', new_start_doc

    img_dict = dict(source='testing', dtype='object', )
    new_descriptor = dict(uid=str(uuid4()), time=time(),
                          run_start=run_start_uid,
                          data_keys={output_field_name: img_dict})
    yield 'descriptor', new_descriptor

    exit_md = None
    new_event = dict(uid=str(uuid4()), time=time(), timestamps={},
                     descriptor=new_descriptor['uid'],
                     data={output_field_name: results},
                     seq_num=0)
    yield 'event', new_event

    if exit_md is None:
        exit_md = {'exit_status': 'success'}
    new_stop = dict(uid=str(uuid4()), time=time(),
                    run_start=run_start_uid, **exit_md)
    yield 'stop', new_stop


# 1. Dark Subtraction
def dark_subtraction_hfi(streams, *args, image_name='pe1_image', **kwargs):
    process = subtract
    output_field_name = 'img'
    light_stream, dark_stream = streams
    run_start_uid = str(uuid4())
    new_start_doc = dict(
        uid=run_start_uid, time=time(),
        parents=[next(s)[1]['uid'] for s in streams],
        hfi=dark_subtraction_hfi.__name__,
        provenance=dict(
            hfi_module=inspect.getmodule(dark_subtraction_hfi).__name__,
            hfi=dark_subtraction_hfi.__name__,
            process_module=inspect.getmodule(process).__name__,
            process=process.__name__,
            kwargs=kwargs,
            args=args),
    )  # More provenance to be defined (eg environment)
    yield 'start', new_start_doc

    _, light_descriptor, _, _ = [n for s in streams for n in next(s)]

    img_dict = dict(source='testing', dtype='array', )
    if 'shape' in light_descriptor['data_keys'][image_name].keys():
        img_dict.update(dict(shape=light_descriptor['data_keys'][image_name][
            'shape']))
    new_descriptor = dict(uid=str(uuid4()), time=time(),
                          run_start=run_start_uid,
                          data_keys={output_field_name: img_dict})
    yield 'descriptor', new_descriptor

    exit_md = None
    _, dark_event = next(dark_stream)
    dark_image = dark_event['data'][image_name]
    for i, (name, ev) in enumerate(light_stream):
        if name == 'stop':
            break
        if name != 'event':
            raise Exception
        try:
            results = process(ev['data'][image_name], dark_image)
        except Exception as e:
            exit_md = dict(exit_status='failure', reason=repr(e),
                           traceback=traceback.format_exc())
            break

        new_event = dict(uid=str(uuid4()), time=time(), timestamps={},
                         descriptor=new_descriptor['uid'],
                         data={output_field_name: results},
                         seq_num=i)
        yield 'event', new_event

    if exit_md is None:
        exit_md = {'exit_status': 'success'}
    new_stop = dict(uid=str(uuid4()), time=time(),
                    run_start=run_start_uid, **exit_md)
    yield 'stop', new_stop


# 2. Polarization Correction
def polarization_correction_hfi(streams, *args,
                                image_name='img', **kwargs):
    output_field_name = 'img'
    process = correct_polarization
    image_stream, calibration_stream = streams
    starts = [next(s) for s in streams]
    parents = [s['uid'] for n, s in starts]
    run_start_uid = str(uuid4())
    new_start_doc = dict(uid=run_start_uid, time=time(),
                         parents=parents,
                         hfi=polarization_correction_hfi.__name__,
                         provenance=dict(
                             hfi_module=sys.modules[__name__].__name__,
                             hfi=polarization_correction_hfi.__name__,
                             process_module=inspect.getmodule(
                                 process).__name__,
                             process=process.__name__,
                             kwargs=kwargs,
                             args=args),
                         )  # More provenance to be defined (eg environment)
    yield 'start', new_start_doc

    _, light_descriptor, _, _ = [n for s in streams for n in next(s)]
    img_dict = dict(source='testing', dtype='array', )
    if 'shape' in light_descriptor['data_keys'][image_name].keys():
        img_dict.update(dict(shape=light_descriptor['data_keys'][image_name][
            'shape']))
    new_descriptor = dict(uid=str(uuid4()), time=time(),
                          run_start=run_start_uid,
                          data_keys={output_field_name: img_dict})
    yield 'descriptor', new_descriptor

    exit_md = None
    calibration = next(calibration_stream)[1]['data']['detector_calibration']
    pprint(calibration)
    geo = AzimuthalIntegrator()
    geo.setPyFAI(**calibration)
    for i, (name, ev) in enumerate(image_stream):
        if name == 'stop':
            break
        if name != 'event':
            raise Exception
        try:
            results = process(geo, ev['data'][image_name],
                              kwargs['polarization_factor'])
        except Exception as e:
            exit_md = dict(exit_status='failure', reason=repr(e),
                           traceback=traceback.format_exc())
            break

        new_event = dict(uid=str(uuid4()), time=time(), timestamps={},
                         descriptor=new_descriptor['uid'],
                         data={'img': results},
                         seq_num=i)
        yield 'event', new_event

    if exit_md is None:
        exit_md = {'exit_status': 'success'}
    new_stop = dict(uid=str(uuid4()), time=time(),
                    run_start=run_start_uid, **exit_md)
    yield 'stop', new_stop


# 3. Masking
def margin_mask_hfi(name_doc_stream_pair,
                    *args,
                    image_name='img',
                    mask_name_doc_stream_pair=None,
                    **kwargs):
    process = margin
    _, start = next(name_doc_stream_pair)
    mask_start = None
    if mask_name_doc_stream_pair is not None:
        _, mask_start = next(mask_name_doc_stream_pair)
    run_start_uid = str(uuid4())
    parents = [s['uid'] for s in [start]]
    if mask_start is not None:
        parents = [s['uid'] for s in [start, mask_start]]
    new_start_doc = dict(uid=run_start_uid, time=time(),
                         parents=parents,
                         hfi=margin_mask_hfi.__name__,
                         provenance=dict(
                             hfi_module=inspect.getmodule(
                                 margin_mask_hfi).__name__,
                             hfi=margin_mask_hfi.__name__,
                             process_module=inspect.getmodule(
                                 process).__name__,
                             process=process.__name__,
                             kwargs=kwargs,
                             args=args))
    yield 'start', new_start_doc

    _, descriptor = next(name_doc_stream_pair)
    if 'shape' in descriptor['data_keys'][image_name].keys():
        img_dict = dict(source='testing', dtype='array',
                        shape=descriptor['data_keys'][image_name][
                            'shape'])
    else:
        img_dict = dict(source='testing', dtype='array', )

    new_descriptor = dict(uid=str(uuid4()), time=time(),
                          run_start=run_start_uid,
                          data_keys=dict(
                              mask=img_dict))
    yield 'descriptor', new_descriptor

    exit_md = None
    for i, (name, ev) in enumerate(name_doc_stream_pair):
        if name == 'stop':
            break
        if name != 'event':
            raise Exception
        try:
            results = process(ev['data'][image_name].shape, kwargs['edge'])
        except Exception as e:
            exit_md = dict(exit_status='failure', reason=repr(e),
                           traceback=traceback.format_exc())
            break

        new_event = dict(uid=str(uuid4()), time=time(), timestamps={},
                         descriptor=new_descriptor['uid'],
                         data={'mask': results},
                         seq_num=i)
        yield 'event', new_event

    if exit_md is None:
        exit_md = {'exit_status': 'success'}
    new_stop = dict(uid=str(uuid4()), time=time(),
                    run_start=run_start_uid, **exit_md)
    yield 'stop', new_stop


def lower_threshold_hfi(name_doc_stream_pair, *args,
                        image_name='img',
                        **kwargs):
    process = np.greater
    _, start = next(name_doc_stream_pair)
    run_start_uid = str(uuid4())
    new_start_doc = dict(uid=run_start_uid, time=time(),
                         parents=[s['uid'] for s in [start]],
                         hfi=lower_threshold_hfi.__name__,
                         provenance={'module': sys.modules[__name__],
                                     'hfi': lower_threshold_hfi.__name__,
                                     'args': args,
                                     'kwargs': kwargs,
                                     'process': process.__name__
                                     })
    yield 'start', new_start_doc

    _, descriptor = next(name_doc_stream_pair)
    new_descriptor = dict(uid=str(uuid4()), time=time(),
                          run_start=run_start_uid,
                          data_keys={'mask': dict(source='testing',
                                                  dtype='array'),
                                     })
    yield 'descriptor', new_descriptor

    exit_md = None
    for i, (name, ev) in enumerate(name_doc_stream_pair):
        if name == 'stop':
            break
        if name != 'event':
            raise Exception
        try:
            results = process(ev['data'][image_name],
                              kwargs['threshold'])
        except Exception as e:
            exit_md = dict(exit_status='failure', reason=repr(e),
                           traceback=traceback.format_exc())
            break

        new_event = dict(uid=str(uuid4()), time=time(), timestamps={},
                         descriptor=new_descriptor['uid'],
                         data={'mask': results},
                         seq_num=i)
        yield 'event', new_event

    if exit_md is None:
        exit_md = {'exit_status': 'success'}
    new_stop = dict(uid=str(uuid4()), time=time(),
                    run_start=run_start_uid, **exit_md)
    yield 'stop', new_stop


def upper_threshold_hfi(name_doc_stream_pair, *args,
                        image_name='img',
                        **kwargs):
    process = np.less
    _, start = next(name_doc_stream_pair)
    run_start_uid = str(uuid4())
    new_start_doc = dict(uid=run_start_uid, time=time(),
                         parents=[s['uid'] for s in [start]],
                         hfi=upper_threshold_hfi.__name__,
                         provenance={'module': sys.modules[__name__],
                                     'hfi': upper_threshold_hfi.__name__,
                                     'args': args,
                                     'kwargs': kwargs,
                                     'process': process.__name__
                                     })
    yield 'start', new_start_doc

    _, descriptor = next(name_doc_stream_pair)
    new_descriptor = dict(uid=str(uuid4()), time=time(),
                          run_start=run_start_uid,
                          data_keys={'mask': dict(source='testing',
                                                  dtype='array'),
                                     })
    yield 'descriptor', new_descriptor

    exit_md = None
    for i, (name, ev) in enumerate(name_doc_stream_pair):
        if name == 'stop':
            break
        if name != 'event':
            raise Exception
        try:
            results = process(ev['data'][image_name],
                              kwargs['threshold'])
        except Exception as e:
            exit_md = dict(exit_status='failure', reason=repr(e),
                           traceback=traceback.format_exc())
            break

        new_event = dict(uid=str(uuid4()), time=time(), timestamps={},
                         descriptor=new_descriptor['uid'],
                         data={'img': results},
                         seq_num=i)
        yield 'event', new_event

    if exit_md is None:
        exit_md = {'exit_status': 'success'}
    new_stop = dict(uid=str(uuid4()), time=time(),
                    run_start=run_start_uid, **exit_md)
    yield 'stop', new_stop


def master_mask_hfi(image_stream, calibration_stream, *args, mask_stream=None,
                    image_name='img', calibration_name='calibration',
                    mask_name='mask',
                    **kwargs):
    process = mask_img
    streams = [image_stream, calibration_stream]
    if mask_stream:
        streams.append(mask_stream)
    run_start_uid = str(uuid4())
    new_start_doc = dict(
        uid=run_start_uid, time=time(),
        parents=[next(s)[1]['uid'] for s in streams],
        hfi=margin_mask_hfi.__name__,
        provenance=dict(
            hfi_module=inspect.getmodule(master_mask_hfi).__name__,
            hfi=margin_mask_hfi.__name__,
            process_module=inspect.getmodule(process).__name__,
            process=process.__name__,
            kwargs=kwargs,
            args=args)
    )
    yield 'start', new_start_doc

    descriptors = [next(s) for n, s in streams]
    if 'shape' in descriptors[0]['data_keys'][image_name].keys():
        mask_dict = dict(
            source='testing', dtype='array',
            shape=descriptors[0]['data_keys'][image_name]['shape'])
    else:
        mask_dict = dict(source='testing', dtype='array', )

    new_descriptor = dict(uid=str(uuid4()), time=time(),
                          run_start=run_start_uid,
                          data_keys=dict(
                              mask=mask_dict))
    yield 'descriptor', new_descriptor

    exit_md = None
    geo = AzimuthalIntegrator()
    geo.setPyFAI(**next(calibration_stream)['data'][calibration_name])
    # Determine if we have one or many masks
    if mask_stream:
        mask_doc_name1, mask_doc1 = next(mask_stream)
        mask_doc_name2, mask_doc2 = next(mask_stream)
        if mask_doc_name2 == 'stop':
            tmsk = mask_doc1['data'][mask_name]
        else:
            tmsk = chain([(mask_doc_name1, mask_doc1),
                          (mask_doc_name2, mask_doc2)], mask_stream)
    else:
        tmsk = None
    for i, (name, ev) in enumerate(image_stream):
        if isinstance(tmsk, types.GeneratorType):
            kwargs['tmsk'] = next(tmsk)[1]['data'][mask_name]
        else:
            kwargs['tmsk'] = tmsk
        if name == 'stop':
            break
        if name != 'event':
            raise Exception
        try:
            results = process(ev['data'][image_name], geo, **kwargs)
        except Exception as e:
            exit_md = dict(exit_status='failure', reason=repr(e),
                           traceback=traceback.format_exc())
            break

        new_event = dict(uid=str(uuid4()), time=time(), timestamps={},
                         descriptor=new_descriptor['uid'],
                         data={'mask': results},
                         seq_num=i)
        yield 'event', new_event

    if exit_md is None:
        exit_md = {'exit_status': 'success'}
    new_stop = dict(uid=str(uuid4()), time=time(),
                    run_start=run_start_uid, **exit_md)
    yield 'stop', new_stop


# 4. Integration
def integrate_hfi(image_stream, calibration_stream, *args, mask_stream=None,
                  image_name='img', calibration_name='calibration',
                  mask_name='mask',
                  **kwargs):
    geo = AzimuthalIntegrator()
    process = geo.integrate1d
    streams = [image_stream, calibration_stream]
    if mask_stream:
        streams.append(mask_stream)
    run_start_uid = str(uuid4())
    new_start_doc = dict(
        uid=run_start_uid, time=time(),
        parents=[next(s)[1]['uid'] for s in streams],
        hfi=margin_mask_hfi.__name__,
        provenance=dict(
            hfi_module=inspect.getmodule(master_mask_hfi).__name__,
            hfi=margin_mask_hfi.__name__,
            process_module=inspect.getmodule(process).__name__,
            process=process.__name__,
            kwargs=kwargs,
            args=args)
    )
    yield 'start', new_start_doc

    descriptors = [next(s) for n, s in streams]
    if 'shape' in descriptors[0]['data_keys'][image_name].keys():
        mask_dict = dict(
            source='testing', dtype='array',
            shape=descriptors[0]['data_keys'][image_name]['shape'])
    else:
        mask_dict = dict(source='testing', dtype='array', )

    new_descriptor = dict(uid=str(uuid4()), time=time(),
                          run_start=run_start_uid,
                          data_keys=dict(
                              mask=mask_dict))
    yield 'descriptor', new_descriptor

    exit_md = None
    geo.setPyFAI(**next(calibration_stream)['data'][calibration_name])
    # Determine if we have one or many masks
    if mask_stream:
        mask_doc_name1, mask_doc1 = next(mask_stream)
        mask_doc_name2, mask_doc2 = next(mask_stream)
        if mask_doc_name2 == 'stop':
            tmsk = ~mask_doc1['data'][mask_name]
        else:
            tmsk = chain([(mask_doc_name1, mask_doc1),
                          (mask_doc_name2, mask_doc2)], mask_stream)
    else:
        tmsk = None
    for i, (name, ev) in enumerate(image_stream):
        if isinstance(tmsk, types.GeneratorType):
            kwargs['mask'] = ~next(tmsk)[1]['data'][mask_name]
        else:
            kwargs['mask'] = tmsk
        if name == 'stop':
            break
        if name != 'event':
            raise Exception
        try:
            results = process(ev['data'][image_name], **kwargs)
        except Exception as e:
            exit_md = dict(exit_status='failure', reason=repr(e),
                           traceback=traceback.format_exc())
            break

        new_event = dict(uid=str(uuid4()), time=time(), timestamps={},
                         descriptor=new_descriptor['uid'],
                         data={'mask': results},
                         seq_num=i)
        yield 'event', new_event

    if exit_md is None:
        exit_md = {'exit_status': 'success'}
    new_stop = dict(uid=str(uuid4()), time=time(),
                    run_start=run_start_uid, **exit_md)
    yield 'stop', new_stop

# 5. Background Subtraction
# 6. G(r) calculation
