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

        # if n == 'stop':
        #     assert d['exit_status'] == 'success'
