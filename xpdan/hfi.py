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


# 1. Dark Subtraction
def dark_subtraction_hfi(streams, *args, image_name='pe1_image', **kwargs):
    process = np.subtract
    light_name_doc_stream_pair, dark_name_doc_stream_pair = streams
    starts = [next(s) for s in streams]
    parents = [s['uid'] for n, s in starts]
    run_start_uid = str(uuid4())
    new_start_doc = dict(uid=run_start_uid, time=time(),
                         parents=parents,
                         hfi=dark_subtraction_hfi.__name__,
                         provenance={'hfi_module':
                                         sys.modules[__name__].__name__,
                                     'hfi': dark_subtraction_hfi.__name__,
                                     'process_module': inspect.getmodule(
                                         process),
                                     'process': process.__name__,
                                     'kwargs': kwargs,
                                     'args': args,

                                     },
                         )  # More provenance to be defined
    yield 'start', new_start_doc

    _, light_descriptor = next(light_name_doc_stream_pair)
    _, dark_descriptor = next(dark_name_doc_stream_pair)
    new_descriptor = dict(
        uid=str(uuid4()), time=time(),
        run_start=run_start_uid,
        data_keys=dict(
            img=dict(source='testing',
                     dtype='array',
                     shape=light_descriptor['data_keys'][image_name][
                         'shape'])))
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
