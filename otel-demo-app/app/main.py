import logging
import random
from time import sleep
import psutil

from fastapi import FastAPI

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.metrics import set_meter_provider, get_meter

from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter


# Resource definition
resource = Resource(attributes={"service.name": "anomaly-detection"})

# ----- TRACING -----
trace.set_tracer_provider(TracerProvider(resource=resource))
tracer = trace.get_tracer(__name__)
trace_exporter = OTLPSpanExporter(endpoint="http://otel-collector:4318/v1/traces")
span_processor = BatchSpanProcessor(trace_exporter)
trace.get_tracer_provider().add_span_processor(span_processor)

# ----- METRICS -----
metric_exporter = OTLPMetricExporter(endpoint="http://otel-collector:4318/v1/metrics")
reader = PeriodicExportingMetricReader(metric_exporter)
meter_provider = MeterProvider(resource=resource, metric_readers=[reader])
set_meter_provider(meter_provider)
meter = get_meter("anomaly-metrics")

# Define metric callbacks for CPU and memory usage
def cpu_usage_callback():
    return [
        psutil.cpu_percent(interval=None)
    ]

def memory_usage_callback():
    mem = psutil.virtual_memory()
    return [mem.used]

# Create observable metrics (CPU and memory usage)
meter.create_observable_gauge("app_cpu_usage_percent", unit="%", callbacks=[cpu_usage_callback])
meter.create_observable_gauge("app_memory_usage_bytes", unit="By", callbacks=[memory_usage_callback])

# ----- LOGGING -----
log_exporter = OTLPLogExporter(endpoint="http://otel-collector:4318/v1/logs")
logger_provider = LoggerProvider(resource=resource)
logger_provider.add_log_record_processor(BatchLogRecordProcessor(log_exporter))

otel_handler = LoggingHandler(level=logging.INFO, logger_provider=logger_provider)
logging.basicConfig(level=logging.INFO, handlers=[otel_handler])
logger = logging.getLogger("anomaly-logger")

# ----- FASTAPI -----
app = FastAPI()
FastAPIInstrumentor().instrument_app(app)

@app.get("/")
def read_root():
    with tracer.start_as_current_span("root-span"):
        logger.info("Handling root request")
        sleep(random.uniform(0.1, 0.3))
        return {"message": "Hello from OpenTelemetry instrumented FastAPI with logs!"}
