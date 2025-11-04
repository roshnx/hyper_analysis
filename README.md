# Hyperliquid Historical Data Fetch Pipeline 

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


## hyperamm: Getting prices from the HyperSwap V3 pool

The `hyperamm/` folder contains utilities to scan pool events (Swap/Mint/Burn/Initialize), reconstruct liquidity, and compute prices directly from on-chain pool state.

Key files:
- `hyperamm/hyperswap_pool_data.py` — pool event scanner and liquidity post-processing
- `hyperamm/liquidity_stair_intervals.csv` — liquidity profile represented as piecewise-constant “stairs” across price
- `data/pool_data/pool_events.csv` — raw pool events (swaps) with sqrtPriceX96, amounts, etc.
- `data/pool_data/tx_costs.csv` — matched gas costs per transaction

### Price from pool state (sqrtPriceX96)

For a V3 pool with token0 decimals `dec0` and token1 decimals `dec1`, the price of token1 per token0 is:

```
price_1_per_0 = (sqrtPriceX96 / 2**96)**2 * 10**(dec0 - dec1)
```

Example for WHYPE/USDT with `dec0 = 18`, `dec1 = 6` (USDT per WHYPE):

```python
dec0, dec1 = 18, 6
df_events['sqrtPriceX96'] = df_events['sqrtPriceX96'].astype('int64')
df_events['price_usdt_per_whype'] = (df_events['sqrtPriceX96'] / (2**96))**2 * (10**(dec0 - dec1))
```

The inverse (token0 per token1) is `1 / price_1_per_0`.

### Execution price from swap amounts

For individual swaps, compute the realized execution price from quantities:

```python
# WHYPE has 18 decimals, USDT has 6 decimals
amount0 = pd.to_numeric(df_events['amount0'], errors='coerce') / 1e18  # WHYPE
amount1 = pd.to_numeric(df_events['amount1'], errors='coerce') / 1e6   # USDT
df_events['price_usdt_per_whype_qty'] = (amount1 / amount0).abs()
```

Optionally adjust for gas (convert gas paid in HYPE/WHYPE to USDT using the execution price):

```python
def calc_effective_price(row):
	a0 = float(row['amount0']) / 1e18  # WHYPE
	a1 = float(row['amount1']) / 1e6   # USDT
	gas_usdt = None
	if pd.notna(row.get('gas_paid_hype')):
		gas_usdt = float(row['gas_paid_hype']) * row['price_usdt_per_whype_qty']
	if gas_usdt is not None and a0 != 0:
		return abs((a1 + gas_usdt) / a0)
	return abs(a1 / a0) if a0 != 0 else None

df_events['effective_price'] = df_events.apply(calc_effective_price, axis=1)
```

### Liquidity profile and initialized ticks

The repo generates a stair-step liquidity profile and tick-level breakdowns. Useful CSVs:

- `hyperamm/liquidity_stair_intervals.csv` — columns include `price_L`, `price_U`, and `L_active`; use this for plotting active liquidity vs price
- `hyperamm/v3_tick_token_amounts.csv` — precomputed token0/token1 amounts per tick range (when available)

A minimal plot (focusing on a price band) looks like:

```python
import pandas as pd
import matplotlib.pyplot as plt

df_liq = pd.read_csv('hyperamm/liquidity_stair_intervals.csv')
band = df_liq[(df_liq['price_L'] >= 20) & (df_liq['price_L'] <= 40)]
plt.step(band['price_L'], band['L_active'], where='post')
plt.xlabel('USDT per WHYPE'); plt.ylabel('Active Liquidity'); plt.grid(True)
plt.show()
```

### Quickstart for AMM analysis

1) Install dependencies
```powershell
pip install -r requirements.txt
```
2) Ensure pool CSVs are present
   - Events: `data/pool_data/pool_events.csv`
   - Gas: `data/pool_data/tx_costs.csv`
3) Explore in the notebook
   - Open `notebooks/spots_pool_arb.ipynb`
   - Run cells to compute `price_usdt_per_whype`, execution prices, and plot liquidity



1) Install Python 3.10+ and AWS CLI v2
- Python: https://www.python.org/downloads/
- AWS CLI: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html

2) Configure AWS credentials
```powershell
aws configure
# Or use a named profile
aws configure --profile myprofile