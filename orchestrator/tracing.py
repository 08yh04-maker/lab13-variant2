from opentelemetry import trace
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.resources import Resource
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from fastapi import FastAPI


def init_tracing(app: FastAPI, service_name: str = "orchestrator"):
    # Создаём Resource
    resource = Resource.create({"service.name": service_name})

    # Создаём TracerProvider
    provider = TracerProvider(resource=resource)

    # Настраиваем Jaeger exporter
    jaeger_exporter = JaegerExporter(
        agent_host_name="localhost",
        agent_port=6831,
    )

    # Добавляем span processor
    provider.add_span_processor(BatchSpanProcessor(jaeger_exporter))

    # Устанавливаем глобальный провайдер
    trace.set_tracer_provider(provider)

    # Инструментируем FastAPI
    FastAPIInstrumentor.instrument_app(app)

    # Инструментируем requests
    RequestsInstrumentor().instrument()

    return trace.get_tracer(__name__)