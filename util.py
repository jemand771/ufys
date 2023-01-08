import contextlib
import os


def dataclass_from_dict(cls, dictlike):
    return cls(
        **{
            key: value
            for key, value
            in dictlike.items()
            if key in cls.__dataclass_fields__  # type: ignore
        }
    )


@contextlib.contextmanager
def chdir(path):
    original = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(original)
