from xpdan.pipelines import db_integrate
from ..pipelines import integration_pipeline
from pprint import pprint


def test_integration_pipeline(exp_db, mk_glbl):
    img_stream = exp_db.restream(exp_db[-1], fill=True)
    p = integration_pipeline(img_stream, glbl=mk_glbl,
                             polarization_kwargs={'polarization_factor': .99},
                             integration_kwargs={'npt': 2200})
    for n, d in p:
        print(n)
        pprint(d)
        if n == 'start':
            assert d['hfi'] == 'integrate_hfi'
        if n == 'event':
            assert d['data']['iq'].shape == (2200,)
            assert d['data']['q'].shape == (2200, )
        if n == 'stop':
            assert d['exit_status'] == 'success'


def test_db_integrate(exp_db, mk_glbl):
    uid = db_integrate(exp_db[-1], glbl=mk_glbl,
                       integration_kwargs={'npt': 2200})

    assert uid is not None
    for n, d in mk_glbl.an_db.restream(mk_glbl.an_db[uid], fill=True):
        print(n)
        pprint(d)
        if n == 'start':
            assert d['hfi'] == 'integrate_hfi'
        if n == 'event':
            assert d['data']['iq'].shape == (2200,)
            assert d['data']['q'].shape == (2200, )
        if n == 'stop':
            assert d['exit_status'] == 'success'