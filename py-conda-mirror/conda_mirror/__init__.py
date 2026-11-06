import importlib.metadata
import warnings
from dataclasses import dataclass
from pathlib import Path

from .conda_mirror import mirror_py

try:
    __version__ = importlib.metadata.version(__name__)
except importlib.metadata.PackageNotFoundError as e:
    warnings.warn(f"Could not determine version of {__name__}\n{e!s}", stacklevel=2)
    __version__ = "unknown"


@dataclass
class S3Config:
    endpoint_url: str | None = None
    region: str | None = None
    force_path_style: bool | None = None


@dataclass
class S3Credentials:
    access_key_id: str
    secret_access_key: str
    session_token: str | None = None


def mirror(
    source: str | Path,
    destination: str | Path,
    subdirs: list[str] | None = None,
    max_retries: int = 10,
    max_parallel: int = 32,
    s3_config_source: S3Config | None = None,
    s3_config_destination: S3Config | None = None,
    s3_credentials_source: S3Credentials | None = None,
    s3_credentials_destination: S3Credentials | None = None,
    include: list[str] | None = None,
    exclude: list[str] | None = None,
):
    print(mirror_py())
