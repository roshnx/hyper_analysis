from __future__ import annotations

import os
from typing import Iterator, List, Optional

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


def get_s3_client(profile: Optional[str] = None, region: Optional[str] = None):
    """Create a boto3 S3 client.

    Honors optional AWS profile and region.
    """
    if profile:
        session = boto3.Session(profile_name=profile, region_name=region)
    else:
        session = boto3.Session(region_name=region)
    return session.client("s3", config=Config(signature_version="s3v4"))


def list_prefixes(
    client,
    bucket: str,
    prefix: str,
    delimiter: str = "/",
    request_payer: str = "requester",
) -> List[str]:
    """List common prefixes ("folders") under a given prefix using Delimiter.

    Returns a list of child prefixes (each ending with '/').
    """
    paginator = client.get_paginator("list_objects_v2")
    prefixes: List[str] = []
    try:
        for page in paginator.paginate(
            Bucket=bucket,
            Prefix=prefix,
            Delimiter=delimiter,
            RequestPayer=request_payer,
        ):
            for cp in (page.get("CommonPrefixes", []) or []):  # mypy/typing guard if key missing
                p = cp.get("Prefix")
                if p:
                    prefixes.append(p)
    except ClientError as e:
        raise e
    return prefixes


def iter_objects(
    client,
    bucket: str,
    prefix: str,
    request_payer: str = "requester",
) -> Iterator[dict]:
    """Iterate all S3 objects under a prefix."""
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(
        Bucket=bucket,
        Prefix=prefix,
        RequestPayer=request_payer,
    ):
        for obj in page.get("Contents", []) or []:
            yield obj


essential_buckets = {
    "market_data": "hyperliquid-archive",
    "asset_ctxs": "hyperliquid-archive",
    "fills": "hl-mainnet-node-data",
}


def market_data_key(date: str, hour: str, datatype: str, coin: str) -> str:
    # Example: market_data/20230916/9/l2Book/SOL.lz4
    return f"market_data/{date}/{hour}/{datatype}/{coin}.lz4"


def asset_ctxs_key(date: str) -> str:
    # Example: asset_ctxs/20230916.csv.lz4
    return f"asset_ctxs/{date}.csv.lz4"


def fills_prefix(date: Optional[str] = None) -> str:
    base = "node_fills_by_block"
    if date:
        return f"{base}/{date}/"
    return f"{base}/"


def ensure_dir_for_file(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)


def download_s3_file(
    client,
    bucket: str,
    key: str,
    dest_path: str,
    request_payer: str = "requester",
) -> str:
    """Download a single S3 object to dest_path.

    Returns the dest_path.
    """
    ensure_dir_for_file(dest_path)
    extra = {"RequestPayer": request_payer}
    client.download_file(bucket, key, dest_path, ExtraArgs=extra)
    return dest_path
