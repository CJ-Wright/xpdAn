from bluesky.callbacks import CallbackBase
from bluesky.callbacks.core import CallbackBase


class StartStopCallback(CallbackBase):
    def start(self, doc):
        print('START ANALYSIS ON {}'.format(doc['uid']))

    def stop(self, doc):
        print('FINISH ANALYSIS ON {}'.format(doc['run_start']))


class PrinterCallback(CallbackBase):
    def __init__(self):
        self.analysis_stage = None

    def start(self, doc):
        self.analysis_stage = doc[1]['analysis_stage']

    def event(self, doc):
        print('file saved at {}'.format(doc[0]['data']['filename']))
        super().event(doc)
