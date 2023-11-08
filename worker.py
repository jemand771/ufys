import functools
import mimetypes
import multiprocessing.pool
import pathlib

# noinspection PyPackageRequirements
import minio
import minio.commonconfig
import minio.lifecycleconfig
import yt_dlp
from urllib3.exceptions import MaxRetryError

import telemetry
from config import ConfigStore
from handlers.asciinema import AsciinemaRequestHandler
from handlers.base import RequestHandler
from handlers.instagram import InstagramRequestHandler
from handlers.ytdl import YTDLRequestHandler
from model import MinioNotConnected, UfysError, UfysRequest, UfysResponse


class Worker:
    config: ConfigStore
    minio: "minio.Minio | None" = None

    def __init__(self, config: ConfigStore = None):
        self.config = config or ConfigStore()
        self.handlers = [
            class_(self) for class_ in [
                InstagramRequestHandler,
                AsciinemaRequestHandler,
                YTDLRequestHandler,
            ]
        ]
        try:
            self.minio = minio.Minio(
                endpoint=self.config.MINIO_ENDPOINT,
                access_key=self.config.MINIO_ACCESS_KEY,
                secret_key=self.config.MINIO_SECRET_KEY,
                secure=self.config.MINIO_SECURE
            )
            if not self.minio.bucket_exists(self.config.MINIO_BUCKET):
                print(f"warning: bucket {config.MINIO_BUCKET} doesn't exist")
                self.minio = None
        except TypeError:
            print("warning: minio not connected (configuration error)")
        except AttributeError:
            print("warning: minio not connected (bucket error)")
        except MaxRetryError:
            print("warning: minio not connected (timeout)")
        # TODO set access policy
        # setting up anonymous access looks painful (json string), and only partially auto-configuring the bucket
        # might yield unexpected results. I'll either re-add this or remove it entirely
        # else:
        #     self.minio.set_bucket_lifecycle(
        #         self.config.MINIO_BUCKET,
        #         minio.lifecycleconfig.LifecycleConfig(
        #             [
        #                 minio.lifecycleconfig.Rule(
        #                     status=minio.commonconfig.ENABLED,
        #                     rule_filter=minio.commonconfig.Filter(prefix=""),
        #                     expiration=minio.lifecycleconfig.Expiration(days=1)
        #                 )
        #             ]
        #         )
        #     )
        # try to extract using custom format extractor

    @telemetry.trace_function
    def handle_request(self, req: UfysRequest) -> list[UfysResponse | UfysError]:

        handlers_to_run = [handler for handler in self.handlers if handler.can_handle(req)]
        if not handlers_to_run:
            return [UfysError(code="no-handler", message="could not find a suitable handler for this request")]

        with multiprocessing.pool.ThreadPool(len(handlers_to_run)) as pool:
            results = pool.map(functools.partial(self.dispatch_handler, req=req), handlers_to_run)

        successful_results = [result for result in results if isinstance(result, UfysResponse)]
        return successful_results or results

    @staticmethod
    def dispatch_handler(handler: RequestHandler, req: UfysRequest) -> UfysResponse | UfysError:
        try:
            # TODO automatic retries
            return handler.handle_request(req)
        except UfysError as e:
            return e
        # TODO should be part of ytdlhandler
        except yt_dlp.utils.DownloadError:
            return UfysError(code="download-error", message="yt-dlp failed to download this video")
        except AssertionError:
            return UfysError(
                code="assertion-error",
                message="an unknown error, thought to be impossible, has occured"
            )

    @telemetry.trace_function
    def reupload(self, path: pathlib.Path, hash_: str):
        if self.minio is None:
            raise MinioNotConnected()
        mime, _ = mimetypes.guess_type(path)
        result = self.minio.fput_object(
            bucket_name=self.config.MINIO_BUCKET,
            object_name=hash_ + path.suffix,
            file_path=str(path),
            content_type=mime
        )
        return result.location or self.get_upload_location(result.object_name)

    def get_upload_location(self, object_name):
        protocol = "https" if self.config.MINIO_SECURE else "http"
        return f"{protocol}://{self.config.MINIO_ENDPOINT}/{self.config.MINIO_BUCKET}/{object_name}"
