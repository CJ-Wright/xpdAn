import time
from uuid import uuid4

from xpdan.startup.start import mds, fs


def analysis_run_engine(hdrs, run_function, md=None, subscription=None,
                        **kwargs):
    """
    Properly run an analysis function on a group of headers while recording
    the data into analysisstore

    Parameters
    ----------
    hdrs: list of MDS headers or a MDS header
        The headers or header to be analyzed, note that the headers are each
        used in the analysis, if you wish to run multiple headers through a
        pipeline you must
    run_function: generator
        This generator processes each event in the headers, it returns the
         filestore resource uids as a list of strings, the data names as a
         list of strings and the data keys as a dict
    md: dict
        Metadata to be added to the analysis header
    subscription: function or list of functions
        Run after processing the event, eg graphing output data
    kwargs: dict
        Additional arguments passed directly to the run_function

    Returns
    -------

    """
    if not isinstance(hdrs, list):
        hdrs = [hdrs]
    # write analysisstore header
    run_start = mds.insert_run_start(
        uid=str(uuid4()),
        time=time.time(),
        provenance={'function_name': run_function.__name__,
                    'hdr_uids': [hdr['start']['uid'] for hdr in hdrs],
                    'kwargs': kwargs},
        **md)

    data_hdr = None
    exit_md = {'exit_status': 'failure'}
    # run the analysis function
    try:
        rf = run_function(*hdrs, **kwargs)
        for i, res, data_names, data_keys, data in enumerate(rf):
            if not data_hdr:
                data_hdr = dict(run_start=run_start,
                                data_keys=data_keys,
                                time=time.time(), uid=str(uuid4()))
                descriptor = mds.insert_descriptor(**data_hdr)
            mds.insert_event(
                descriptor=descriptor,
                uid=str(uuid4()),
                time=time.time(),
                data={k: v for k, v in zip(data_names, res)},
                timestamps={},
                seq_num=i)
            if not isinstance(subscription, list):
                subscription = [subscription]
            for subs in subscription:
                subs(data)
        exit_md['exit_status'] = 'success'
    except Exception as e:
        # Analysis failed!
        exit_md['exit_status'] = 'failure'
        exit_md['reason'] = e
    finally:
        mds.insert_run_stop(run_start=run_start,
                            uid=str(uuid4()),
                            time=time.time(), **exit_md)
        return run_start


# TODO: smarter person(s) than me should split this into two decorators
def mds_fs_dec(data_names, data_sub_keys, save_func=None, save_loc=None,
               spec=None, resource_kwargs={}, datum_kwargs={}, **dec_kwargs):
    """
    Decorator for saving analysis outputs to MDS/FS

    Parameters
    ----------
    data_names: list of str
        The names for each data
    data_sub_keys: list of dicts
        The list of data_keys
    save_func: function or list of functions
        The function used to save the raw data for each data key there must be
        a function in the list. Must be the same length as the return of the
        decorated function.
    save_loc: str
        The save location for the data on disk
    spec: str
        The FS spec string
    resource_kwargs:
        kwargs passed to fs.insert_datum

    Returns
    -------
    stuff

    Notes
    -----
    The way I see this there are 3 possible outcomes where data could be
    analyzed and then "saved".
    1. The data is generated and then saved on disk because it is huge, it is
    then inserted into FS. This translates to, `data_keys` has a
    `external='Filestore:` key-value pair so we should return the uid
    2. The data is generated and then saved in MDS because it is not huge.
    This translates to there is no `external` for this data key and no
    `save_func`, just store the python object directly, we should return the
    python object itself.
    3. The data already exists in FS, we just want to link it to some other
    data. Translation, there is (maybe) no `save_func` (or is there a dummy
    save func which hands back the uid) but we still need to give back
    the FS uid, because there is an `external` key.
    """

    def wrap(f):
        def wrapper(*args, **kwargs):
            data_keys = {k: v for k, v in zip(data_names, data_sub_keys)}
            # Run the function
            a = f(*args, **kwargs)  # snowflake retrieval/processing gen here
            for outs in a:
                returns = []
                for b, s in zip(outs, save_func):
                    if s is None:
                        returns.append(b)
                    uid = str(uuid4())
                    # make save name
                    save_name = save_loc + uid
                    # Save using the save function
                    s(b, save_name, **dec_kwargs)
                    # Insert into FS
                    uid = str(uuid4())
                    fs_res = fs.insert_resource(spec, save_name,
                                                resource_kwargs)
                    fs.insert_datum(fs_res, uid, datum_kwargs)
                # TODO: need to unpack a little better
                yield returns, data_names, data_keys, outs

        return wrapper

    return wrap
