
from .components import TraceComponentStateChange, TraceNewComponent
from .connections import TraceConnection, TraceConnectionClosed
from .etl_workflow import TraceEtlState
from .ports import TracePort, TracePortClosed
from .records import TraceRecord, TraceRecordDispatch

ALL_TRACE_EVENTS = [
    TraceComponentStateChange, TraceNewComponent,
    TraceConnection, TraceConnectionClosed,
    TraceEtlState,
    TracePort, TracePortClosed,
    TraceRecord, TraceRecordDispatch,
]