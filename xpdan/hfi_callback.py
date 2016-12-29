from bluesky.callbacks.core import CallbackBase
import inspect
from time import time
from uuid import uuid4
from xpdan.tools import *
from itertools import islice
import traceback
from pprint import pprint


def for_call_methods(decorator, *decorator_args, **decorator_kwargs):
    """Decorate the ``__call__`` method of a class

    Parameters
    ----------
    decorator
    decorator_args
    decorator_kwargs

    Returns
    -------

    Notes
    -----
    This was designed to decorate the ``__call__`` method of ``CallbackBase``
    so that we can take all of its outputs and throw them into mds/fs if needed

    """

    def decorate(cls):
        for name, fn in inspect.getmembers(cls):
            if name == '__call__':
                setattr(cls, name, decorator(*decorator_args,
                                             **decorator_kwargs)(fn))
        return cls

    return decorate


class AnalysisCallbackBase(CallbackBase):
    def __init__(self, *args, **kwargs):
        super().__init__()
        self.exit_md = None
        self.new_descriptor = None
        self.run_start_uid = str(uuid4())
        self.args = args
        self.kwargs = kwargs

    def stop(self, doc):
        if self.exit_md is None:
            self.exit_md = {'exit_status': 'success'}
        new_stop = dict(uid=str(uuid4()), time=time(),
                        run_start=self.run_start_uid, **self.exit_md)
        return 'stop', new_stop


class DarkSubtractionCallback(AnalysisCallbackBase):
    __name__ = 'DarkSubtractionCallback'

    def __init__(self, exp_db, dark_header=None, dark_name='pe1_image',
                 dark_key='dark_uid',
                 image_name='pe1_image',
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dark_key = dark_key
        self.image_name = image_name
        self.dark_name = dark_name
        self.dark_header = dark_header
        self.exp_db = exp_db
        self.process = subtract
        if dark_header:
            self.dark_stream = exp_db.restream(dark_header, fill=True)
            _, self.dark_start = next(self.dark_stream)
            _, self.dark_desc = next(self.dark_stream)
            _, self.dark_event = next(self.dark_stream)
            self.dark_image = self.dark_event['data'][self.dark_name]
        else:
            self.dark_stream = None

    def start(self, doc):
        if self.dark_header is None:
            # Go get the dark
            self.dark_header = self.exp_db(dark_uid=doc[self.dark_key],
                                           is_dark_img=True)[0]
            self.dark_stream = self.exp_db.restream(self.dark_header,
                                                    fill=True)
            _, self.dark_start = next(self.dark_stream)
            _, self.dark_desc = next(self.dark_stream)
            _, self.dark_event = next(self.dark_stream)
            self.dark_image = self.dark_event['data'][self.dark_name]

        starts = [doc, self.dark_start]
        parents = [s['uid'] for s in starts]
        new_start_doc = dict(uid=self.run_start_uid, time=time(),
                             parents=parents,
                             hfi=self.__name__,
                             provenance=dict(
                                 hfi_module=inspect.getmodule(self).__name__,
                                 hfi=self.__name__,
                                 process_module=inspect.getmodule(
                                     self.process).__name__,
                                 process=self.process.__name__,
                                 kwargs=self.kwargs,
                                 args=self.args),
                             )  # More provenance to be defined (eg environment)
        return 'start', new_start_doc

    def descriptor(self, doc):
        if 'shape' in doc['data_keys'][self.image_name].keys():
            img_dict = dict(source='testing', dtype='array',
                            shape=doc['data_keys'][self.image_name][
                                'shape'])
        else:
            img_dict = dict(source='testing', dtype='array', )

        new_descriptor = dict(
            uid=str(uuid4()), time=time(),
            run_start=self.run_start_uid,
            data_keys=dict(
                img=img_dict))
        self.new_descriptor = new_descriptor
        return 'descriptor', new_descriptor

    def event(self, doc):
        try:
            results = self.process(doc['data'][self.image_name],
                                   self.dark_image)
        except Exception as e:
            self.exit_md = dict(exit_status='failure', reason=repr(e),
                                traceback=traceback.format_exc())
            # GO DIRECTLY TO STOP. DO NOT PASS GO, DO NOT COLLECT $200
            return self.stop(None)

        new_event = dict(uid=str(uuid4()), time=time(), timestamps={},
                         descriptor=self.new_descriptor['uid'],
                         data={'img': results},
                         seq_num=doc['seq_num'])
        return 'event', new_event


class PolarizationCorrectionCallback(AnalysisCallbackBase):
    def __init__(self, an_db, calibration_header,
                 image_name='pe1_image',
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.calibration_header = calibration_header
        self.an_db = an_db
        self.image_name = image_name
        self.process = correct_polarization
        if calibration_header:
            self.calib_stream = an_db.restream(calibration_header, fill=True)
            self.calib_start = next(self.calib_stream)
            self.calib_desc = next(self.calib_stream)
            self.calib_event = next(self.calib_stream)
            self.calib_image = self.calib_event['data'][self.calib_name]
        else:
            self.calib_stream = None

    def start(self, doc):
        starts = [doc, self.calib_start]
        parents = [s['uid'] for n, s in starts]
        new_start_doc = dict(
            uid=self.run_start_uid, time=time(),
            parents=parents,
            hfi=self.__name__,
            provenance=dict(
                hfi_module=inspect.getmodule(self).__name__,
                hfi=self.__name__,
                process_module=inspect.getmodule(
                    self.process).__name__,
                process=self.process.__name__,
                kwargs=self.kwargs,
                args=self.args),
        )  # More provenance to be defined (eg environment)
        return 'start', new_start_doc

    def descriptor(self, doc):
        if 'shape' in doc['data_keys'][self.image_name].keys():
            img_dict = dict(source='testing', dtype='array',
                            shape=doc['data_keys'][self.image_name][
                                'shape'])
        else:
            img_dict = dict(source='testing', dtype='array', )

        new_descriptor = dict(
            uid=str(uuid4()), time=time(),
            run_start=self.run_start_uid,
            data_keys=dict(
                img=img_dict))
        self.new_descriptor = new_descriptor
        return 'descriptor', new_descriptor

    def event(self, doc):
        try:
            results = self.process(doc['data'][self.image_name],
                                   self.dark_image)
        except Exception as e:
            self.exit_md = dict(exit_status='failure', reason=repr(e),
                                traceback=traceback.format_exc())
            # GO DIRECTLY TO STOP. DO NOT PASS GO, DO NOT COLLECT $200
            return self.stop(None)

        new_event = dict(uid=str(uuid4()), time=time(), timestamps={},
                         descriptor=self.new_descriptor['uid'],
                         data={'img': results},
                         seq_num=doc['seq_num'])
        return 'event', new_event


class MarginMaskCallback(AnalysisCallbackBase):
    __name__ = 'MarginMaskCallback'

    def __init__(self, *args, image_name='img', **kwargs):
        pass
