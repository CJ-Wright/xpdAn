"""module includes Callback classes for xpdacq/xpdan"""

import os
import datetime
import numpy as np
from bluesky.callbacks.core import CallbackBase
import doct
import tifffile


# supplementary functions
def _timestampstr(timestamp):
    """convert timestamp to strftime formate"""
    timestring = datetime.datetime.fromtimestamp(float(timestamp)).strftime(
        '%Y%m%d-%H%M%S')
    return timestring


class Exporter(CallbackBase):
    """Exporting data from given header(s).

    It is a variation of bluesky.callback.broker.LiveTiffExporter class
    It incorporate metadata and data from individual data points in
    the filenames.

    Parameters
    ----------
    field : str
        a data key, e.g., 'image'
    data_dir_template : str
        A templated for directory where images will be saved to.
        It is expressed with curly brackets, which will be filled in with
        the attributes of 'start', 'event', and (for image stacks) 'i',
        a sequential number.
        e.g., "/xpdUser/tiff_base/{start.sample_name}/"
    save_func: function
        The function which saves the data, must have signature
        f(data, filename)
    data_fields : list, optional
        a list of strings for data fields want to be included. default
        is an empty list (not include any readback metadata in filename).
    dryrun : bool, optional
        default to False; if True, do not write any files
    overwrite : bool, optional
        default to False, raising an OSError if file exists
    db : Broker, optional
        The databroker instance to use, if not provided use databroker
        singleton
    """

    def __init__(self, field, data_dir_template, save_func,
                 data_fields=None, dryrun=False,
                 overwrite=False, db=None):
        if data_fields is None:
            data_fields = []
        if db is None:
            # Read-only db
            from databroker.databroker import DataBroker as db

        self.db = db

        # required args
        self.field = field
        self.data_dir_template = data_dir_template
        # optioanal args 
        self.data_fields = data_fields  # list of keys for md to include
        self.dryrun = dryrun
        self.overwrite = overwrite
        self.filenames = []
        self._start = None
        # standard, do need to expose it to user
        self.event_template = '{event.seq_num:03d}_{i}.tif'

    def _generate_filename(self, doc):
        """method to generate filename based on template

        It operates at event level, i.e., doc is event document
        """
        # convert time
        timestr = _timestampstr(doc['time'])
        # readback value for certain list of data keys
        data_val_list = []
        for key in self.data_fields:
            val = doc.get(key, None)
            if val is not None:
                data_val_list.append(val)
        data_val_trunk = '_'.join(data_val_list)

        # event sequence
        base_dir = self.data_dir_template.format(start=self._start,
                                                 event=doc)
        event_info = self.event_template.format(i=doc['seq_num'],
                                                start=self._start,
                                                event=doc)

        # full path + complete filename
        filename = '_'.join([timestr, data_val_trunk, event_info])
        total_filename = os.path.join(base_dir, filename)

        return total_filename

    def start(self, doc):
        """method for start document"""
        self.filenames = []
        # Convert doc from dict into dottable dict, more convenient
        # in Python format strings: doc.key == doc['key']
        self._start = doct.Document('start', doc)

        super().start(doc)

    def event(self, doc):
        """tiff-saving operation applied at event level"""
        if self.field not in doc['data']:
            raise KeyError('required field = {} is not in header'
                           .format(self.field))

        self.db.fill_event(doc)  # modifies in place
        data = np.asarray(doc['data'][self.field])

        filename = self._generate_filename(doc)

        self.save_func(data, filename)

    def stop(self, doc):
        """method for stop document"""
        self._start = None
        self.filenames = []
        super().stop(doc)
