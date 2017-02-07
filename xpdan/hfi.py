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
import inspect
import itertools
import traceback
import types
from itertools import chain, zip_longest
from operator import sub
from time import time
from uuid import uuid4

from pyFAI import AzimuthalIntegrator

from xpdan.glbl import an_glbl
from .tools import *


def defensive_filestore_call_hfi(stream, db):
    for name, doc in stream:
        if name == 'event' and not all([v for v in doc['filled'].values()]):
            db.fill_event(doc)
        yield name, doc


# -1. Wavelength Calibration (Pending @sghose)
def spoof_wavelength_calibration_hfi(stream: types.GeneratorType, *args,
                                     calibration_field: str ='wavelength',
                                     **kwargs) -> types.GeneratorType:
    """Create wavelength calibration stream based off of data in raw data
    header.

    Parameters
    ----------
    stream: generator
        Stream of raw data from ``db.restream(hdr)``
    calibration_field: str, optional
        The name of the field where the calibration data is stored. Defaults to
        wavelength

    Yields
    -------
    name, document:
        The name and documents for the wavelength calibration

    Warnings
    --------
    This performs no analysis of the data, the wavelength must already be in
    the raw data header.
    """
    hfi = spoof_wavelength_calibration_hfi
    _, start_doc = next(stream)
    run_start_uid = str(uuid4())
    new_start_doc = dict(
        uid=run_start_uid, time=time(),
        parents=[start_doc['uid']],
        hfi=hfi.__name__,
        provenance=dict(
            hfi_module=inspect.getmodule(hfi).__name__,
            hfi=hfi.__name__,
            process_module='spoof',
            process='spoof',
            kwargs=kwargs,
            args=args),
    )  # More provenance to be defined (eg environment)
    results = start_doc[calibration_field]
    yield 'start', new_start_doc

    data_keys_dict = {'wavelength': dict(source='testing', dtype='float', )}
    new_descriptor = dict(uid=str(uuid4()), time=time(),
                          run_start=run_start_uid,
                          data_keys=data_keys_dict)
    yield 'descriptor', new_descriptor

    exit_md = None
    new_event = dict(uid=str(uuid4()), time=time(), timestamps={},
                     descriptor=new_descriptor['uid'],
                     data={'wavelength': results},
                     seq_num=0)
    yield 'event', new_event

    if exit_md is None:
        exit_md = {'exit_status': 'success'}
    new_stop = dict(uid=str(uuid4()), time=time(),
                    run_start=run_start_uid, **exit_md)
    yield 'stop', new_stop


# 0. Detector Calibration
def spoof_detector_calibration_hfi(stream: types.GeneratorType, *args,
                                   calibration_field: str ='calibration_md',
                                   **kwargs) -> types.GeneratorType:
    """Create detector calibration stream based off of data in raw data
        header.

    Parameters
    ----------
    stream: generator
        Stream of raw data from ``db.restream(hdr)``
    calibration_field: str, optional
        The name of the field where the calibration data is stored. Defaults to
        calibration_md

    Yields
    -------
    name, document:
        The name and documents for the detector calibration

    Warnings
    --------
    This performs no analysis of the data, the detector calibration must
    already be in the raw data header.
    """
    hfi = spoof_detector_calibration_hfi
    _, start_doc = next(stream)
    run_start_uid = str(uuid4())
    new_start_doc = dict(
        uid=run_start_uid, time=time(),
        parents=[start_doc['uid']],
        hfi=hfi.__name__,
        provenance=dict(
            hfi_module=inspect.getmodule(hfi).__name__,
            hfi=hfi.__name__,
            process_module='spoof',
            process='spoof',
            kwargs=kwargs,
            args=args),
        # This could be done by `get_parent` but it is easy to do it now
        calibration_collection_uid=start_doc['calibration_collection_uid']
    )  # More provenance to be defined (eg environment)
    results = start_doc[calibration_field]
    yield 'start', new_start_doc

    data_keys_dict = {'detector_calibration': dict(source='testing',
                                                   dtype='object', )}
    new_descriptor = dict(uid=str(uuid4()), time=time(),
                          run_start=run_start_uid,
                          data_keys=data_keys_dict)
    yield 'descriptor', new_descriptor

    exit_md = None
    new_event = dict(uid=str(uuid4()), time=time(), timestamps={},
                     descriptor=new_descriptor['uid'],
                     data={'detector_calibration': results},
                     seq_num=0)
    yield 'event', new_event

    if exit_md is None:
        exit_md = {'exit_status': 'success'}
    new_stop = dict(uid=str(uuid4()), time=time(),
                    run_start=run_start_uid, **exit_md)
    yield 'stop', new_stop


# 1. Dark Subtraction
def polarization_correction_hfi(streams, *args,
                                image_name='img', **kwargs):
    """Create detector calibration stream based off of data in raw data
        header.

    Parameters
    ----------
    streams: tuple of generators
        Streams of raw data and detector calibration data from
        ``(db.restream(hdr), db.restream(calibration_hdr))``
    image_name: str, optional
        The name of the field where the image data is stored. Defaults to
        'img'
    Yields
    -------
    name, document:
        The name and documents for the polarization correction

    """
    process = correct_polarization
    hfi = polarization_correction_hfi
    image_stream, calibration_stream = streams
    run_start_uid = str(uuid4())
    new_start_doc = dict(
        uid=run_start_uid, time=time(),
        parents=[next(s)[1]['uid'] for s in streams],
        hfi=hfi.__name__,
        provenance=dict(
            hfi_module=inspect.getmodule(hfi).__name__,
            hfi=hfi.__name__,
            process_module=inspect.getmodule(process).__name__,
            process=process.__name__,
            kwargs=kwargs,
            args=args),
    )  # More provenance to be defined (eg environment)
    yield 'start', new_start_doc

    _, light_descriptor, _, _ = [n for s in streams for n in next(s)]

    data_keys_dict = {'img': dict(source='testing', dtype='array', )}
    if 'shape' in light_descriptor['data_keys'][image_name].keys():
        data_keys_dict['img'].update(
            shape=light_descriptor['data_keys'][image_name]['shape'])

    new_descriptor = dict(uid=str(uuid4()), time=time(),
                          run_start=run_start_uid,
                          data_keys=data_keys_dict)
    yield 'descriptor', new_descriptor

    exit_md = None
    calibration = next(calibration_stream)[1]['data']['detector_calibration']
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


# 2. Polarization Correction
def margin_mask_hfi(image_stream,
                    *args,
                    image_name='img',
                    mask_stream=None,
                    mask_name='mask',
                    **kwargs):
    """Create margin mask stream based off of data in raw data
        header.

    Parameters
    ----------
    image_stream: generator
        Streams of raw data data from ``db.restream(hdr)``
    image_name: str, optional
        The name of the field where the image data is stored. Defaults to 'img'
    mask_stream: generator, optional
        Stream of mask data which is used as a base for subsequent masks.
        Defaults to no additional mask data
    mask_name: str, optional
        The name of the field where the mask data is stored. Defaults to 'mask'

    Yields
    -------
    name, document:
        The name and documents for the margin mask

    """
    process = margin
    hfi = margin_mask_hfi
    streams = [image_stream]
    if mask_stream is not None:
        streams.append(mask_stream)
    run_start_uid = str(uuid4())
    new_start_doc = dict(
        uid=run_start_uid, time=time(),
        parents=[next(s)[1]['uid'] for s in streams],
        hfi=hfi.__name__,
        provenance=dict(
            hfi_module=inspect.getmodule(hfi).__name__,
            hfi=hfi.__name__,
            process_module=inspect.getmodule(process).__name__,
            process=process.__name__,
            kwargs=kwargs,
            args=args),
    )  # More provenance to be defined (eg environment)
    yield 'start', new_start_doc

    descriptors = [next(s) for s in streams]
    img_descriptor = descriptors[0][1]
    data_keys_dict = {'mask': dict(source='testing', dtype='array', )}

    if 'shape' in img_descriptor['data_keys'][image_name].keys():
        data_keys_dict['mask'].update(
            shape=img_descriptor['data_keys'][image_name][
                'shape'])

    new_descriptor = dict(uid=str(uuid4()), time=time(),
                          run_start=run_start_uid,
                          data_keys=data_keys_dict)
    yield 'descriptor', new_descriptor

    exit_md = None
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
            results = process(ev['data'][image_name].shape, kwargs['edge'])
            if tmsk:
                results *= tmsk
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


def dark_subtraction_hfi(streams, *args, image_name='pe1_image',
                         dark_event_number=0, **kwargs):
    """Create detector calibration stream based off of data in raw data
        header.

    Parameters
    ----------
    streams: tuple of generators
        Streams of raw data and dark data from
        ``(db.restream(hdr), db.restream(dark_hdr))``
    image_name: str, optional
        The name of the field where the image data is stored. Defaults to
        'pe1_image'
    dark_event_number: int, optional
        Which dark event to use for subtraction, defaults to the first dark
    Yields
    -------
    name, document:
        The name and documents for the dark subtraction

    """
    process = sub
    hfi = dark_subtraction_hfi
    light_stream, dark_stream = streams
    run_start_uid = str(uuid4())

    # We track the kwargs, so just tack it on
    kwargs.update(dark_event_number=dark_event_number)
    new_start_doc = dict(
        uid=run_start_uid, time=time(),
        parents=[next(s)[1]['uid'] for s in streams],
        hfi=hfi.__name__,
        provenance=dict(
            hfi_module=inspect.getmodule(hfi).__name__,
            hfi=hfi.__name__,
            process_module=inspect.getmodule(process).__name__,
            process=process.__name__,
            kwargs=kwargs,
            args=args),
    )  # More provenance to be defined (eg environment)
    yield 'start', new_start_doc

    _, light_descriptor, _, _ = [n for s in streams for n in next(s)]

    data_keys_dict = {'img': dict(source='testing', dtype='array', )}
    if 'shape' in light_descriptor['data_keys'][image_name].keys():
        data_keys_dict['img'].update(
            shape=light_descriptor['data_keys'][image_name]['shape'])

    new_descriptor = dict(uid=str(uuid4()), time=time(),
                          run_start=run_start_uid,
                          data_keys=data_keys_dict)
    yield 'descriptor', new_descriptor

    exit_md = None
    _, dark_event = next(itertools.islice(dark_stream,
                                          kwargs['dark_event_number'],
                                          kwargs['dark_event_number'] + 1))
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
                         data={'img': results},
                         seq_num=i)
        yield 'event', new_event

    if exit_md is None:
        exit_md = {'exit_status': 'success'}
    new_stop = dict(uid=str(uuid4()), time=time(),
                    run_start=run_start_uid, **exit_md)
    yield 'stop', new_stop


# 3. Masking
def spoof_mask_hfi(streams, *args, mask_name='mask',
                   image_name='pe1_image', **kwargs):
    process = decompress_mask
    hfi = spoof_mask_hfi
    run_start_uid = str(uuid4())
    parent_starts = [next(s)[1] for s in streams]
    new_start_doc = dict(
        uid=run_start_uid, time=time(),
        parents=[s['uid'] for s in parent_starts],
        hfi=hfi.__name__,
        provenance=dict(
            hfi_module=inspect.getmodule(hfi).__name__,
            hfi=hfi.__name__,
            process_module=inspect.getmodule(process).__name__,
            process=process.__name__,
            kwargs=kwargs,
            args=args),
    )  # More provenance to be defined (eg environment)
    yield 'start', new_start_doc

    _, desc = [n for s in streams for n in next(s)]

    data_keys_dict = {'mask': dict(source='testing', dtype='array', )}
    if 'shape' in desc['data_keys'][image_name].keys():
        data_keys_dict['mask'].update(
            shape=desc['data_keys'][image_name]['shape'])

    new_descriptor = dict(uid=str(uuid4()), time=time(),
                          run_start=run_start_uid,
                          data_keys=data_keys_dict)
    yield 'descriptor', new_descriptor

    exit_md = None
    mask_md = parent_starts[0].get(mask_name, None)
    for i, (name, ev) in enumerate(streams[0]):
        if name == 'stop':
            break
        if name != 'event':
            raise Exception
        try:
            img = ev['data'][image_name]
            if mask_md is None:
                mask = np.ones(img.shape)
            else:
                # unpack here
                mask = decompress_mask(*mask_md, img.shape)
            results = mask
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

'''
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

'''


def master_mask_hfi(streams, *args, mask_stream=None,
                    image_name='img', calibration_name='detector_calibration',
                    mask_name='mask',
                    **kwargs):
    """Create detector calibration stream based off of data in raw data
        header.

    Parameters
    ----------
    streams: tuple of generators
        Streams of raw data and detector calibration data from
        ``db.restream(hdr), db.restream(calibration_hdr)``
    image_name: str, optional
        The name of the field where the image data is stored. Defaults to 'img'
    mask_stream: generator, optional
        Stream of mask data which is used as a base for subsequent masks.
        Defaults to no additional mask data
    mask_name: str, optional
        The name of the field where the mask data is stored. Defaults to 'mask'

    Yields
    -------
    name, document:
        The name and documents for the margin mask
    """
    process = mask_img
    hfi = master_mask_hfi
    image_stream, calibration_stream = streams
    if mask_stream:
        streams.append(mask_stream)
    run_start_uid = str(uuid4())
    new_start_doc = dict(
        uid=run_start_uid, time=time(),
        parents=[next(s)[1]['uid'] for s in streams],
        hfi=hfi.__name__,
        provenance=dict(
            hfi_module=inspect.getmodule(hfi).__name__,
            hfi=hfi.__name__,
            process_module=inspect.getmodule(process).__name__,
            process=process.__name__,
            kwargs=kwargs,
            args=args),
    )  # More provenance to be defined (eg environment)
    yield 'start', new_start_doc

    descriptors = [next(s)[1] for s in streams]
    data_key_dict = {'mask': dict(source='testing', dtype='array', )}
    if 'shape' in descriptors[0]['data_keys'][image_name].keys():
        data_key_dict['mask'].update(
            shape=descriptors[0]['data_keys'][image_name]['shape'])

    new_descriptor = dict(uid=str(uuid4()), time=time(),
                          run_start=run_start_uid,
                          data_keys=data_key_dict)
    yield 'descriptor', new_descriptor

    exit_md = None
    geo = AzimuthalIntegrator()
    geo.setPyFAI(**next(calibration_stream)[1]['data'][calibration_name])

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
def integrate_hfi(streams, *args,
                  image_name='img', calibration_name='detector_calibration',
                  mask_name='mask',
                  **kwargs):
    geo = AzimuthalIntegrator()
    hfi = integrate_hfi
    process = geo.integrate1d
    if len(streams) == 2:
        image_stream, calibration_stream = streams
        mask_stream = None
    elif len(streams) == 3:
        image_stream, calibration_stream, mask_stream = streams
    else:
        raise RuntimeError('Need either 2 streams or 3 streams')
    run_start_uid = str(uuid4())
    new_start_doc = dict(
        uid=run_start_uid, time=time(),
        parents=[next(s)[1]['uid'] for s in streams],
        hfi=hfi.__name__,
        provenance=dict(
            hfi_module=inspect.getmodule(hfi).__name__,
            hfi=hfi.__name__,
            process_module=inspect.getmodule(process).__name__,
            process=process.__name__,
            kwargs=kwargs,
            args=args),
    )  # More provenance to be defined (eg environment)
    yield 'start', new_start_doc

    descriptors = [next(s)[1] for s in streams]
    data_key_dict = {'iq': dict(source='testing', dtype='array',
                                shape=kwargs['npt']),
                     'q': dict(source='testing', dtype='array',
                               shape=kwargs['npt'])}  # FIXME: add more

    new_descriptor = dict(uid=str(uuid4()), time=time(),
                          run_start=run_start_uid, data_keys=data_key_dict)
    yield 'descriptor', new_descriptor

    exit_md = None
    geo.setPyFAI(**next(calibration_stream)[1]['data'][calibration_name])
    # Determine if we have one or many masks
    if mask_stream is not None:
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
        if name == 'stop':
            break
        if name != 'event':
            raise Exception
        if hasattr(tmsk, '__next__'):
            kwargs['mask'] = ~next(tmsk)[1]['data'][mask_name]
        elif tmsk is not None:
            kwargs['mask'] = ~tmsk
        try:
            results = process(ev['data'][image_name], **kwargs)
        except Exception as e:
            exit_md = dict(exit_status='failure', reason=repr(e),
                           traceback=traceback.format_exc())
            break

        new_event = dict(uid=str(uuid4()), time=time(), timestamps={},
                         descriptor=new_descriptor['uid'],
                         data={'iq': results[1],
                               'q': results[0]},
                         seq_num=i)
        yield 'event', new_event

    if exit_md is None:
        exit_md = {'exit_status': 'success'}
    new_stop = dict(uid=str(uuid4()), time=time(),
                    run_start=run_start_uid, **exit_md)
    yield 'stop', new_stop


# 5. Background Subtraction
def background_subtraction_hfi(streams, *args, background_event_number=0,
                               data_name='iq', scale=1.,
                               **kwargs):
    process = sub
    hfi = background_subtraction_hfi
    fore_stream, background_stream = streams
    run_start_uid = str(uuid4())

    # We track the kwargs, so just tack it on
    kwargs.update(dark_event_number=background_event_number,
                  scale=scale)
    new_start_doc = dict(
        uid=run_start_uid, time=time(),
        parents=[next(s)[1]['uid'] for s in streams],
        hfi=hfi.__name__,
        provenance=dict(
            hfi_module=inspect.getmodule(hfi).__name__,
            hfi=hfi.__name__,
            process_module=inspect.getmodule(process).__name__,
            process=process.__name__,
            kwargs=kwargs,
            args=args),
    )  # More provenance to be defined (eg environment)
    yield 'start', new_start_doc

    _, fore_descriptor, _, bg_descirptor = [n for s in streams for n in next(s)]

    data_keys_dict = {'img': dict(source='testing', dtype='array', )}
    if 'shape' in fore_descriptor['data_keys'][data_name].keys():
        data_keys_dict['img'].update(
            shape=fore_descriptor['data_keys'][data_name]['shape'])

    new_descriptor = dict(uid=str(uuid4()), time=time(),
                          run_start=run_start_uid,
                          data_keys=data_keys_dict)
    yield 'descriptor', new_descriptor

    exit_md = None
    _, dark_event = next(itertools.islice(fore_stream,
                                          kwargs['dark_event_number'],
                                          kwargs['dark_event_number'] + 1))
    dark_image = dark_event['data'][data_name]
    for i, (name, ev) in enumerate(fore_stream):
        if name == 'stop':
            break
        if name != 'event':
            raise Exception

        try:
            results = process(ev['data'][data_name], dark_image * scale)
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
# 6. G(r) calculation


def get_dark(stream, db):
    n, start = next(stream)
    dark_uid = start.get(an_glbl.dark_field_key, None)
    dark_hdr = db(**{'dark_collection_uid': dark_uid, 'is_dark': True})[0]
    yield from db.restream(dark_hdr, fill=True)


def terminate(*args):
    for n in zip_longest(*args):
        yield n[0]
