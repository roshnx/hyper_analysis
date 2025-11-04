# snapshot_liquidity.py
# pip install web3 pandas matplotlib pillow

from web3 import Web3
import pandas as pd
import matplotlib.pyplot as plt
import math
import time
from pathlib import Path

RPC  = "https://hyperliquid.drpc.org/"
POOL = Web3.to_checksum_address("0x337b56d87a6185cd46af3ac2cdf03cbc37070c30")
BLOCK = "latest"
MIN_TICK, MAX_TICK = -887272, 887272   # shrink around current tick later if you want

ABI_POOL = [
    {"inputs":[],"name":"slot0","outputs":[
        {"internalType":"uint160","name":"sqrtPriceX96","type":"uint160"},
        {"internalType":"int24","name":"tick","type":"int24"},
        {"internalType":"uint16","name":"observationIndex","type":"uint16"},
        {"internalType":"uint16","name":"observationCardinality","type":"uint16"},
        {"internalType":"uint16","name":"observationCardinalityNext","type":"uint16"},
        {"internalType":"uint8","name":"feeProtocol","type":"uint8"},
        {"internalType":"bool","name":"unlocked","type":"bool"}
    ],"stateMutability":"view","type":"function"},
    {"inputs":[],"name":"tickSpacing","outputs":[{"internalType":"int24","name":"","type":"int24"}],"stateMutability":"view","type":"function"},
    {"inputs":[{"internalType":"int16","name":"wordPosition","type":"int16"}],"name":"tickBitmap","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},
    {"inputs":[{"internalType":"int24","name":"tick","type":"int24"}],"name":"ticks","outputs":[
        {"internalType":"uint128","name":"liquidityGross","type":"uint128"},
        {"internalType":"int128","name":"liquidityNet","type":"int128"},
        {"internalType":"uint256","name":"feeGrowthOutside0X128","type":"uint256"},
        {"internalType":"uint256","name":"feeGrowthOutside1X128","type":"uint256"},
        {"internalType":"int56","name":"tickCumulativeOutside","type":"int56"},
        {"internalType":"uint160","name":"secondsPerLiquidityOutsideX128","type":"uint160"},
        {"internalType":"uint32","name":"secondsOutside","type":"uint32"},
        {"internalType":"bool","name":"initialized","type":"bool"}],
     "stateMutability":"view","type":"function"},
    {"inputs":[],"name":"token0","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},
    {"inputs":[],"name":"token1","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},
]

ABI_ERC20 = [
    {"inputs":[],"name":"symbol","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},
    {"inputs":[],"name":"decimals","outputs":[{"internalType":"uint8","name":"","type":"uint8"}],"stateMutability":"view","type":"function"},
]

def tick_to_price(tick: int, dec0: int, dec1: int) -> float:
    # token1 per token0
    return (1.0001 ** tick) * (10 ** (dec0 - dec1))

def get_token_meta(w3, addr):
    c = w3.eth.contract(address=addr, abi=ABI_ERC20)
    try: sym = c.functions.symbol().call()
    except: sym = "UNK"
    dec = c.functions.decimals().call()
    return sym, dec

def fetch_liquidity_rows(w3: Web3, pool_addr: str, block="latest",
                         min_tick=MIN_TICK, max_tick=MAX_TICK) -> tuple[pd.DataFrame, dict]:
    pool = w3.eth.contract(address=pool_addr, abi=ABI_POOL)

    # meta + current
    tick_spacing = pool.functions.tickSpacing().call()
    token0 = pool.functions.token0().call()
    token1 = pool.functions.token1().call()
    sym0, dec0 = get_token_meta(w3, token0)
    sym1, dec1 = get_token_meta(w3, token1)
    sqrtPriceX96, curr_tick, *_ = pool.functions.slot0().call(block_identifier=block)

    # discover initialized ticks
    init_net = {}
    min_w = math.floor(min_tick / tick_spacing / 256)
    max_w = math.floor(max_tick / tick_spacing / 256)
    for w in range(min_w, max_w + 1):
        bitmap = pool.functions.tickBitmap(w).call(block_identifier=block)
        if bitmap == 0:
            continue
        bb = bitmap
        while bb:
            lsb = bb & -bb
            bit = (lsb.bit_length() - 1)
            index = w * 256 + bit
            t = index * tick_spacing
            rec = pool.functions.ticks(t).call(block_identifier=block)
            if rec[7]:
                init_net[t] = int(rec[1])
            bb ^= lsb

    ticks = sorted(init_net.keys())
    # walk ticks: cumulative liquidityNet gives L_active per [t_i, t_{i+1})
    rows = []
    L_active = 0
    for i, tL in enumerate(ticks):
        L_active += init_net[tL]
        if i + 1 < len(ticks):
            tU = ticks[i+1]
            rows.append({
                "tick_L": tL,
                "tick_U": tU,
                "price_L": tick_to_price(tL, dec0, dec1),
                "price_U": tick_to_price(tU, dec0, dec1),
                "L_active": L_active
            })
    df = pd.DataFrame(rows)
    info = {
        "sym0": sym0, "dec0": dec0,
        "sym1": sym1, "dec1": dec1,
        "curr_tick": int(curr_tick),
        "curr_price": (sqrtPriceX96 / (2**96))**2 * (10 ** (dec0 - dec1))
    }
    return df, info

def plot_snapshot(df: pd.DataFrame, info: dict, pool_addr: str, outfile: str | None = None,
                  xlog=True, xlim=None):
    plt.figure(figsize=(11,5))
    # step curve
    plt.step(df["price_L"], df["L_active"], where="post", label="Active Liquidity L")

    # current price marker
    cp = info["curr_price"]
    plt.axvline(cp, linestyle="--", linewidth=1.5, label=f"Current price ≈ {cp:.6g}")

    if xlog:
        plt.xscale("log")
    if xlim:
        plt.xlim(xlim)

    plt.xlabel(f"Price ({info['sym1']} per {info['sym0']})")
    plt.ylabel("Active Liquidity (L)")
    plt.title(f"Liquidity profile — Pool {pool_addr}\nCurrent tick: {info['curr_tick']}")
    plt.grid(True, which="both", alpha=0.4)
    plt.legend()
    plt.tight_layout()
    if outfile:
        Path(outfile).parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(outfile, dpi=160)
    plt.close()

if __name__ == "__main__":
    w3 = Web3(Web3.HTTPProvider(RPC))
    assert w3.is_connected(), "RPC not reachable"

    df, info = fetch_liquidity_rows(w3, POOL, block=BLOCK)
    # (Optional) zoom to a price window around current price (e.g., ×25 on each side)
    zoom = None
    zoom = (info["curr_price"]*(0.7), info["curr_price"]*(1.3))

    plot_snapshot(df, info, POOL, outfile="frames/liquidity_snapshot.png", xlog=True, xlim=zoom)
    print("Saved frames/liquidity_snapshot.png")
