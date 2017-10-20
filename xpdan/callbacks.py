from bluesky.callbacks.core import CallbackBase


class StartStopCallback(CallbackBase):
    def start(self, doc):
        print('START ANALYSIS ON {}'.format(doc['uid']))

    def stop(self, doc):
        if doc:
            print('FINISH ANALYSIS ON {}'.format(doc['run_start']))
