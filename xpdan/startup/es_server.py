import fire
from bluesky.utils import install_qt_kicker
from xpdan.vend.callbacks.core import RunRouter
from xpdan.vend.callbacks.zmq import RemoteDispatcher
from xpdconf.conf import glbl_dict
from databroker_elasticsearch import callback_from_name


def run_server(
    prefix=None,
    outbound_proxy_address=glbl_dict["outbound_proxy_address"],
    server_name="xpd",
):
    """Start up the visualization server

    Parameters
    ----------
    prefix : bytes or list of bytes, optional
        The Publisher channels to listen to. Defaults to
        ``[b"an", b"raw"]``
    outbound_proxy_address : str, optional
        The address and port of the zmq proxy. Defaults to
        ``glbl_dict["outbound_proxy_address"]``
    server_name : str
        The name of the elasticsearch server, this is used with
        ``databroker_elasticsearch.callback_from_name`` to produce the
        elasticsearch callback
    """
    # TODO: allow handing in of callbacks (for direct python use)
    d = RemoteDispatcher(outbound_proxy_address, prefix=prefix)

    d.subscribe(callback_from_name(server_name))
    print("Starting Elastic Search Server")
    d.start()


def run_main():
    fire.Fire(run_server)


if __name__ == "__main__":
    run_main()
