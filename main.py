import dataclasses
from json import JSONEncoder

import yt_dlp
from flask import Flask, request
from flask.json import jsonify
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor

import telemetry
import util
import worker
from model import MinioNotConnected, UfysError, UfysRequest

APP = Flask(__name__)
WORKER = worker.Worker(worker.ConfigStore.from_env())

telemetry.init()
FlaskInstrumentor().instrument_app(APP)
RequestsInstrumentor().instrument()


class ResponseEncoder(JSONEncoder):
    def default(self, o):
        if dataclasses.is_dataclass(o):
            return dataclasses.asdict(o)
        return super().default(o)


APP.json_provider_class = ResponseEncoder


@APP.post("/video")
def get_video_url():
    req = util.dataclass_from_dict(UfysRequest, request.json)
    resp = WORKER.handle_request(req)
    return jsonify(resp)


@APP.errorhandler(AssertionError)
def handle_assertionerror(ex):
    return jsonify(UfysError(code="unknown-error")), 500


@APP.errorhandler(yt_dlp.utils.DownloadError)
def handle_downloaderror(ex):
    return jsonify(UfysError(code="download-error")), 500


@APP.errorhandler(MinioNotConnected)
def handle_minio_not_connected(ex):
    return jsonify(UfysError(code="minio-error")), 500


if __name__ == '__main__':
    # this server is for development only, do not use in production
    APP.run(host="0.0.0.0", port=5004, debug=True, threaded=True)
