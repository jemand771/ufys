import dataclasses

import flask.json.provider
from flask import Flask, request
from flask.json import jsonify
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor

import telemetry
import util
import worker
from model import MinioNotConnected, UfysError, UfysRequest, UfysResponse

APP = Flask(__name__)
WORKER = worker.Worker(worker.ConfigStore.from_env())

telemetry.init(service_name="embed-works.ufys")
FlaskInstrumentor().instrument_app(APP)
RequestsInstrumentor().instrument()


class CustomJsonProvider(flask.json.provider.DefaultJSONProvider):
    def dumps(self, o, **kwargs):
        if not isinstance(o, list) or not all(dataclasses.is_dataclass(c) for c in o):
            return super().dumps(o, **kwargs)
        return super().dumps(
            [
                dataclasses.asdict(c) | dict(_class=c.__class__.__name__)
                for c in o
            ], **kwargs
        )


APP.json = CustomJsonProvider(APP)


@APP.post("/video")
def get_video_url():
    req = util.dataclass_from_dict(UfysRequest, request.json)
    resp = WORKER.handle_request(req)
    assert resp
    success = any(isinstance(c, UfysResponse) for c in resp)
    return jsonify(resp), 200 if success else 500


@APP.errorhandler(AssertionError)
def handle_assertionerror(ex):
    return jsonify(
        [
            UfysError(
                code="assertion-error",
                message="an unknown error, thought to be impossible, has occured"
            )
        ]
    ), 500


@APP.errorhandler(MinioNotConnected)
def handle_minio_not_connected(ex):
    return jsonify([UfysError(code="minio-error", message="an internal backend service is unavailable")]), 500


if __name__ == '__main__':
    # this server is for development only, do not use in production
    APP.run(host="0.0.0.0", port=5004, debug=True, threaded=True)
