import dataclasses
import functools
import os
import typing

import opentelemetry.trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor


def get_endpoint():
    # test value for debugging with local tunnel:
    # http://localhost:4318/v1/traces
    return os.environ.get("OTEL_ENDPOINT")


def init():
    if (endpoint := get_endpoint()) is None:
        print("skipping trace initialization")
        return
    tracer = TracerProvider(
        resource=Resource(
            attributes={
                "service.name": "embed-works.ufys"
            }
        )
    )
    opentelemetry.trace.set_tracer_provider(tracer)
    tracer.add_span_processor(
        BatchSpanProcessor(
            OTLPSpanExporter(endpoint=endpoint)
        )
    )


def prefix_dict(prefix: str, dict_: dict[str, typing.Any]) -> dict[str, typing.Any]:
    return {
        f"{prefix}.{key}": value
        for key, value
        in dict_.items()
    }


def flatten_attributes(attributes: dict) -> dict[str, typing.Any]:
    results = {}
    # importing here to prevent circular dependency
    from worker import Worker
    for key, value in attributes.items():
        if isinstance(value, Worker):
            continue
        if dataclasses.is_dataclass(value):
            results.update(prefix_dict(key, flatten_attributes(dataclasses.asdict(value))))
            continue
        if isinstance(value, dict):
            results.update(prefix_dict(key, flatten_attributes(value)))
            continue
        if isinstance(value, list):
            results.update(prefix_dict(key, flatten_attributes(dict(enumerate(value)))))
            continue
    return results


def trace_function(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        arg_attrs = {
            key: value
            for key, value
            in zip(func.__code__.co_varnames, args)
        }
        with opentelemetry.trace.get_tracer(__name__).start_as_current_span(
            func.__name__,
            attributes=flatten_attributes(arg_attrs | kwargs)
        ):
            return func(*args, **kwargs)

    return wrapper
