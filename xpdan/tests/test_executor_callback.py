from redsky.savers import NPYSaver
from redsky.streamer import db_store_single_resource_single_file, \
    db_store_single_resource_single_file_non_gen

from ..hfi_callback import AnalysisCallbackBase
from numpy.testing import assert_array_equal
from pprint import pprint
from uuid import uuid4
from bluesky.callbacks.core import CallbackBase
import inspect
from time import time
from uuid import uuid4
from xpdan.tools import *
from itertools import islice
import traceback
from pprint import pprint


class TimesTwo(AnalysisCallbackBase):
    __name__ = 'TimesTwo'

    def __init__(self, *args, image_name='pe1_image', **kwargs):
        super().__init__(*args, **kwargs)
        self.image_name = image_name
        self.process = lambda x: 2 * x

    def start(self, doc):
        parents = [doc['uid']]
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
            results = self.process(doc['data'][self.image_name])
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


class PlusOne(AnalysisCallbackBase):
    def __init__(self, *args, image_name='img', **kwargs):
        super().__init__(*args, **kwargs)
        self.image_name = image_name
        self.process = lambda x: x + 1

    def start(self, doc):
        parents = [doc['uid']]
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
            results = self.process(doc['data'][self.image_name])
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

    __name__ = 'PlusOne'


class Squared(AnalysisCallbackBase):
    def __init__(self, *args, image_name='img', **kwargs):
        super().__init__(*args, **kwargs)
        self.image_name = image_name
        self.process = lambda x: x**2

    def start(self, doc):
        parents = [doc['uid']]
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
            results = self.process(doc['data'][self.image_name])
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
    __name__ = 'Squared'


class SampleExecutor(object):
    def __init__(self):
        self.tt = TimesTwo()
        self.po = PlusOne()
        self.s = Squared()

    def __call__(self, name, doc):
        a = self.tt(name, doc)
        b = self.po(*a)
        c = self.s(*b)
        return c


def test_dark_subtraction_hfi(exp_db, an_db, tmp_dir, img_size):
    hdr = exp_db[-1]

    DecCallback = SampleExecutor()

    for n2, z2 in exp_db.restream(hdr, fill=True):
        n, z = DecCallback(n2, z2)
        pprint(n)
        pprint(z)
        print()
