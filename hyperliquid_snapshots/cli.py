from __future__ import annotations

import argparse
import os
from typing import List, Optional

from .s3_utils import (
    asset_ctxs_key,
    download_s3_file,
    essential_buckets,
    fills_prefix,
    get_s3_client,
    iter_objects,
    list_prefixes,
    market_data_key,
)
from .decompress import decompress_lz4_file


def _unique(seq: List[str]) -> List[str]:
    return sorted(set(seq))


def list_market_data_levels(
    client,
    profile: Optional[str],
    region: Optional[str],
    bucket: str,
    date: Optional[str],
    hour: Optional[str],
    datatype: str,
    level: str,
) -> List[str]:
    # Dates
    if level == "dates":
        prefixes = list_prefixes(client, bucket=bucket, prefix="market_data/")
        # Expect prefixes like market_data/20230916/
        return [p.rstrip("/").split("/")[-1] for p in prefixes]

    if not date:
        raise SystemExit("--date is required for level != dates")

    # Hours
    if level == "hours":
        prefixes = list_prefixes(client, bucket=bucket, prefix=f"market_data/{date}/")
        return [p.rstrip("/").split("/")[-1] for p in prefixes]

    # Coins (files under datatype)
    if level == "coins":
        if not hour:
            raise SystemExit("--hour is required for level=coins")
        prefix = f"market_data/{date}/{hour}/{datatype}/"
        coins: List[str] = []
        for obj in iter_objects(client, bucket=bucket, prefix=prefix):
            key = obj.get("Key", "")
            if not key.lower().endswith(".lz4"):
                continue
            name = os.path.basename(key)[:-4]  # strip .lz4
            if name:
                coins.append(name)
        return _unique(coins)

    if level == "objects":
        # full objects under the best-known prefix for provided inputs
        if hour:
            prefix = f"market_data/{date}/{hour}/{datatype}/"
        else:
            prefix = f"market_data/{date}/"
        return [obj["Key"] for obj in iter_objects(client, bucket=bucket, prefix=prefix)]

    raise SystemExit(f"Unknown level: {level}")


def list_asset_ctxs_levels(client, bucket: str, date: Optional[str], level: str) -> List[str]:
    if level == "dates":
        prefixes = list_prefixes(client, bucket=bucket, prefix="asset_ctxs/")
        # This may return empty prefixes if files sit directly; fall back to listing objects
        if prefixes:
            return [p.rstrip("/").split("/")[-1] for p in prefixes]
        # Fallback: scan objects and infer dates from filenames
        dates: List[str] = []
        for obj in iter_objects(client, bucket=bucket, prefix="asset_ctxs/"):
            key = obj.get("Key", "")
            base = os.path.basename(key)
            if base.endswith(".csv.lz4"):
                d = base.split(".")[0]
                dates.append(d)
        return _unique(dates)
    if level == "objects":
        pre = "asset_ctxs/" if not date else f"asset_ctxs/{date}"
        return [obj["Key"] for obj in iter_objects(client, bucket=bucket, prefix=pre)]
    raise SystemExit("Only --level dates or objects is supported for asset_ctxs")


def list_fills_levels(client, bucket: str, date: Optional[str], level: str) -> List[str]:
    if level == "objects":
        pre = fills_prefix(date)
        return [obj["Key"] for obj in iter_objects(client, bucket=bucket, prefix=pre)]
    if level == "dates":
        prefixes = list_prefixes(client, bucket=bucket, prefix="node_fills_by_block/")
        return [p.rstrip("/").split("/")[-1] for p in prefixes]
    raise SystemExit("Only --level dates or objects is supported for fills")


def cmd_list(args: argparse.Namespace) -> None:
    client = get_s3_client(profile=args.profile, region=args.region)
    dataset = args.dataset
    bucket = essential_buckets[dataset]
    if dataset == "market_data":
        items = list_market_data_levels(
            client,
            args.profile,
            args.region,
            bucket,
            args.date,
            args.hour,
            args.datatype,
            args.level,
        )
    elif dataset == "asset_ctxs":
        items = list_asset_ctxs_levels(client, bucket=bucket, date=args.date, level=args.level)
    else:
        items = list_fills_levels(client, bucket=bucket, date=args.date, level=args.level)

    for it in items:
        print(it)


def cmd_fetch(args: argparse.Namespace) -> None:
    client = get_s3_client(profile=args.profile, region=args.region)
    dataset = args.dataset
    bucket = essential_buckets[dataset]
    out_root = args.out
    os.makedirs(out_root, exist_ok=True)

    downloaded: List[str] = []

    if dataset == "market_data":
        if not args.coin:
            raise SystemExit("--coin is required for dataset=market_data")
        datatype = args.datatype
        # Determine hours to pull
        hours: List[str]
        if args.hour:
            hours = [str(h) for h in args.hour]
        else:
            # list available hours
            hours = list_market_data_levels(
                client,
                args.profile,
                args.region,
                bucket,
                args.date,
                None,
                datatype,
                level="hours",
            )
        for hour in hours:
            key = market_data_key(args.date, hour, datatype, args.coin)
            dest = os.path.join(out_root, key)
            if args.dry_run:
                print(f"DRY RUN: would download s3://{bucket}/{key} -> {dest}")
                continue
            try:
                download_s3_file(client, bucket=bucket, key=key, dest_path=dest)
                downloaded.append(dest)
                if args.decompress:
                    out = decompress_lz4_file(dest, remove_src=args.rm_lz4)
                    print(f"Decompressed: {out}")
                print(f"Downloaded: {dest}")
            except Exception as e:
                print(f"WARN: failed {key}: {e}")

    elif dataset == "asset_ctxs":
        key = asset_ctxs_key(args.date)
        dest = os.path.join(out_root, key)
        if args.dry_run:
            print(f"DRY RUN: would download s3://{bucket}/{key} -> {dest}")
        else:
            download_s3_file(client, bucket=bucket, key=key, dest_path=dest)
            downloaded.append(dest)
            if args.decompress:
                out = decompress_lz4_file(dest, remove_src=args.rm_lz4)
                print(f"Decompressed: {out}")
            print(f"Downloaded: {dest}")

    else:  # fills
        prefix = fills_prefix(args.date)
        for obj in iter_objects(client, bucket=bucket, prefix=prefix):
            key = obj.get("Key", "")
            if not key:
                continue
            dest = os.path.join(out_root, key)
            if args.dry_run:
                print(f"DRY RUN: would download s3://{bucket}/{key} -> {dest}")
                continue
            try:
                download_s3_file(client, bucket=bucket, key=key, dest_path=dest)
                downloaded.append(dest)
                # some fills may not be lz4; only decompress if suffix is .lz4
                if args.decompress and dest.lower().endswith(".lz4"):
                    out = decompress_lz4_file(dest, remove_src=args.rm_lz4)
                    print(f"Decompressed: {out}")
                print(f"Downloaded: {dest}")
            except Exception as e:
                print(f"WARN: failed {key}: {e}")

    if args.summary and downloaded:
        print("\nSummary:")
        for d in downloaded:
            print(d)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="hyper-pipeline",
        description="Fetch Hyperliquid historical data for a specific instrument",
    )
    p.add_argument("--profile", help="AWS profile name", default=None)
    p.add_argument("--region", help="AWS region (optional)", default=None)

    sub = p.add_subparsers(dest="cmd", required=True)

    # list
    lp = sub.add_parser("list", help="List available dates/hours/coins/objects")
    lp.add_argument("--dataset", choices=["market_data", "asset_ctxs", "fills"], required=True)
    lp.add_argument("--level", choices=["dates", "hours", "coins", "objects"], default="dates")
    lp.add_argument("--date", help="YYYYMMDD for market_data/asset_ctxs")
    lp.add_argument("--hour", help="hour (e.g., 9) for market_data")
    lp.add_argument("--datatype", default="l2Book", help="market_data datatype (default l2Book)")
    lp.set_defaults(func=cmd_list)

    # fetch
    fp = sub.add_parser("fetch", help="Download and optionally decompress data")
    fp.add_argument("--dataset", choices=["market_data", "asset_ctxs", "fills"], required=True)
    fp.add_argument("--date", required=True, help="YYYYMMDD")
    fp.add_argument("--hour", action="append", help="Repeatable: hour(s) to fetch (market_data)")
    fp.add_argument("--datatype", default="l2Book", help="market_data datatype (default l2Book)")
    fp.add_argument("--coin", help="Instrument coin symbol for market_data (e.g., SOL, BTC)")
    fp.add_argument("--out", default=os.path.join(".", "data"), help="Output root directory")
    fp.add_argument("--decompress", action="store_true", help="Decompress .lz4 after download")
    fp.add_argument("--rm-lz4", action="store_true", help="Remove .lz4 after successful decompression")
    fp.add_argument("--dry-run", action="store_true", help="Print actions without downloading")
    fp.add_argument("--summary", action="store_true", help="Print summary of downloaded files")
    fp.set_defaults(func=cmd_fetch)

    return p


def main(argv: Optional[List[str]] = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)
