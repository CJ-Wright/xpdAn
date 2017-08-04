from streams import Stream
import shed.event_streams as es
from ..tools import query_dark, temporal_prox


def create_dark_subtraction_pipeline(db, source):
    dark_query_stream = es.Query(db, source,
                                 query_function=query_dark,
                                 query_decider=temporal_prox,
                                 name='Query for Dark')
    dark_stream = es.QueryUnpacker(db, dark_query_stream)
    return dark_query_stream, dark_stream


