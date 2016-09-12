from xpdan.startup.start import db, mds, fs
from uuid import uuid4
import time


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
    # write analysisstore header
    run_start = mds.insert_run_start(
        uid=str(uuid4()),
        time=time.time(),
        provenance={'function_name': run_function.__name__,
                    'hdr_uids': [hdr['uid'] for hdr in hdrs],
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
