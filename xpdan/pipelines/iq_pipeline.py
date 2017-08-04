from streams import Stream
import shed.event_streams as es
from ..tools import query_dark, temporal_prox
from .pipeline_creators import create_dark_subtraction_pipeline
from databroker.databroker import DataBroker as db

source = Stream()

fg_dq, fg_dark = create_dark_subtraction_pipeline(db, source)
