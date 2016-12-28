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


# 1. Dark Subtraction
def dark_subtraction_hfi(streams, *args, image_name='pe1_image', **kwargs):
    process = subtract
    light_name_doc_stream_pair, dark_name_doc_stream_pair = streams
    starts = [next(s) for s in streams]
    parents = [s['uid'] for n, s in starts]
    run_start_uid = str(uuid4())
    new_start_doc = dict(uid=run_start_uid, time=time(),
                         parents=parents,
                         hfi=dark_subtraction_hfi.__name__,
                         provenance=dict(
                             hfi_module=inspect.getmodule(
                                 dark_subtraction_hfi).__name__,
                             hfi=dark_subtraction_hfi.__name__,
                             process_module=inspect.getmodule(
                                 process).__name__,
                             process=process.__name__,
                             kwargs=kwargs,
                             args=args),
                         )  # More provenance to be defined (eg environment)
    yield 'start', new_start_doc

    _, light_descriptor = next(light_name_doc_stream_pair)
    _, dark_descriptor = next(dark_name_doc_stream_pair)

    if 'shape' in light_descriptor['data_keys'][image_name].keys():
        img_dict = dict(source='testing', dtype='array',
                        shape=light_descriptor['data_keys'][image_name][
                            'shape'])
    else:
        img_dict = dict(source='testing', dtype='array', )

    new_descriptor = dict(uid=str(uuid4()), time=time(),
                          run_start=run_start_uid,
                          data_keys=dict(
                              img=img_dict))
    yield 'descriptor', new_descriptor

    exit_md = None
    _, dark_event = next(dark_name_doc_stream_pair)
    dark_image = dark_event['data'][image_name]
    for i, (name, ev) in enumerate(light_name_doc_stream_pair):
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
                         data={'img': results},
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
    process = correct_polarization
    name_doc_stream_pair, calibration_name_doc_stream_pair = streams
    starts = [next(s) for s in streams]
    parents = [s['uid'] for n, s in starts]
    run_start_uid = str(uuid4())
    new_start_doc = dict(uid=run_start_uid, time=time(),
                         parents=parents,
                         hfi=polarization_correction_hfi.__name__,
                         provenance=dict(
                             hfi_module=sys.modules[__name__].__name__,
                             hfi=dark_subtraction_hfi.__name__,
                             process_module=inspect.getmodule(
                                 process).__name__,
                             process=process.__name__,
                             kwargs=kwargs,
                             args=args),
                         )  # More provenance to be defined (eg environment)
    yield 'start', new_start_doc

    _, descriptor = next(name_doc_stream_pair)
    _, cal_desc = next(calibration_name_doc_stream_pair)
    new_descriptor = dict(uid=str(uuid4()), time=time(),
                          run_start=run_start_uid,
                          data_keys={'img': dict(source='testing',
                                                 dtype='array'),
                                     })  # TODO: include shape somehow
    yield 'descriptor', new_descriptor

    exit_md = None
    calibration = next(calibration_name_doc_stream_pair)['data']['calibration']
    geo = AzimuthalIntegrator()
    geo.setPyFAI(**calibration)
    for i, (name, ev) in enumerate(name_doc_stream_pair):
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

# 4. Integration
# 5. Background Subtraction
# 6. G(r) calculation
