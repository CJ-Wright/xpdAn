from ..pipelines import integration_pipeline, db_integrate
from pprint import pprint
import pytest


kwargs_list = [{}, dict(polarization_kwargs={'polarization_factor': .99}),
          dict(integration_kwargs={'npt': 2200}),
          dict(polarization_kwargs={'polarization_factor': .99},
               integration_kwargs={'npt': 2200}), ]


@pytest.mark.parametrize('kwargs', kwargs_list)
def test_integration_pipeline(exp_db, mk_glbl, kwargs):
    img_stream = exp_db.restream(exp_db[-1], fill=True)
    p = integration_pipeline(img_stream, glbl=mk_glbl, **kwargs)
    for n, d in p:
        print(n)
        pprint(d)
        if n == 'start':
            assert d['hfi'] == 'integrate_hfi'
        if n == 'event':
            if 'integration_kwargs' in kwargs.keys():
                assert d['data']['iq'].shape == (
                    kwargs['integration_kwargs']['npt'], )
                assert d['data']['q'].shape == (
                    kwargs['integration_kwargs']['npt'], )
        if n == 'stop':
            assert d['exit_status'] == 'success'


@pytest.mark.parametrize('kwargs', kwargs_list)
def test_db_integrate(exp_db, mk_glbl, kwargs):
    uid = db_integrate(exp_db[-1], glbl=mk_glbl, **kwargs)

    assert uid is not None
    for n, d in mk_glbl.an_db.restream(mk_glbl.an_db[uid], fill=True):
        print(n)
        pprint(d)
        if n == 'start':
            assert d['hfi'] == 'integrate_hfi'
        if n == 'event':
            if 'integration_kwargs' in kwargs.keys():
                assert d['data']['iq'].shape == (
                    kwargs['integration_kwargs']['npt'], )
                assert d['data']['q'].shape == (
                    kwargs['integration_kwargs']['npt'], )
        if n == 'stop':
            assert d['exit_status'] == 'success'
