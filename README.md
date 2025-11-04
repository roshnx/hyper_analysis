# Hyperliquid Historical Data Fetch Pipeline (Windows-friendly)

This lightweight Python CLI helps you list and fetch Hyperliquid historical data for a specific instrument (coin) from S3 requester-pays buckets.

Supported datasets:
- market_data: L2 book snapshots per coin
- asset_ctxs: asset contexts (by day)
- fills: node fills by block (broad, not coin-filtered)

S3 locations:
- L2 books: s3://hyperliquid-archive/market_data/[date]/[hour]/[datatype]/[coin].lz4
- Asset contexts: s3://hyperliquid-archive/asset_ctxs/[date].csv.lz4
- Fills: s3://hl-mainnet-node-data/node_fills_by_block

Note: These buckets are requester-pays. You will be charged for data transfer.

## Setup (Windows PowerShell)

1) Install Python 3.10+ and AWS CLI v2
- Python: https://www.python.org/downloads/
- AWS CLI: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html

2) Configure AWS credentials
```powershell
aws configure
# Or use a named profile
aws configure --profile myprofile