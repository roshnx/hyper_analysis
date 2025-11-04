# pip install web3 pandas
from web3 import Web3
from web3._utils.events import get_event_data
from datetime import datetime, timezone
import pandas as pd
import time
import os

# -------------------- CONFIG --------------------
RPC  = "https://hyperliquid.drpc.org/"      # HyperEVM
POOL = Web3.to_checksum_address("0x337b56d87a6185cd46af3ac2cdf03cbc37070c30")

# Use either explicit blocks OR a UTC time window. If you set TIMES, the code will map to blocks.
FROM_BLOCK = None         # e.g. 5355754
TO_BLOCK   = None         # e.g. 5374053

FROM_UTC = "2025-06-10 18:00:00"  # October 21, 6 PM UTC
TO_UTC   = "2025-06-10 19:00:00"  # 1 hour window

# Which events to pull (start with only "Swap" for speed)
SCAN_TOPICS = ["Swap", "Mint", "Burn", "Initialize"]  # add Mint/Burn/Initialize to locate v3 liquidity ranges

CHUNK = 100  # Small chunks to stay under rate limit
DELAY_BETWEEN_CHUNKS = 1.0  # 1 second between chunks (60 chunks per minute max)
# ------------------------------------------------

w3 = Web3(Web3.HTTPProvider(RPC))

# ---------- Minimal ABIs ----------
ERC20_ABI = [
    {"constant":True,"inputs":[],"name":"symbol","outputs":[{"name":"","type":"string"}],"stateMutability":"view","type":"function"},
    {"constant":True,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"stateMutability":"view","type":"function"},
    {"constant":True,"inputs":[],"name":"name","outputs":[{"name":"","type":"string"}],"stateMutability":"view","type":"function"},
]

POOL_ABI = [
    {"inputs":[],"name":"token0","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},
    {"inputs":[],"name":"token1","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},
    {"inputs":[],"name":"fee","outputs":[{"internalType":"uint24","name":"","type":"uint24"}],"stateMutability":"view","type":"function"},
    {"inputs":[],"name":"slot0","outputs":[
        {"internalType":"uint160","name":"sqrtPriceX96","type":"uint160"},
        {"internalType":"int24","name":"tick","type":"int24"},
        {"internalType":"uint16","name":"observationIndex","type":"uint16"},
        {"internalType":"uint16","name":"observationCardinality","type":"uint16"},
        {"internalType":"uint16","name":"observationCardinalityNext","type":"uint16"},
        {"internalType":"uint8","name":"feeProtocol","type":"uint8"},
        {"internalType":"bool","name":"unlocked","type":"bool"}],"stateMutability":"view","type":"function"},
    # Events
    {"anonymous":False,"inputs":[
        {"indexed":True,"internalType":"address","name":"sender","type":"address"},
        {"indexed":True,"internalType":"address","name":"recipient","type":"address"},
        {"indexed":False,"internalType":"int256","name":"amount0","type":"int256"},
        {"indexed":False,"internalType":"int256","name":"amount1","type":"int256"},
        {"indexed":False,"internalType":"uint160","name":"sqrtPriceX96","type":"uint160"},
        {"indexed":False,"internalType":"uint128","name":"liquidity","type":"uint128"},
        {"indexed":False,"internalType":"int24","name":"tick","type":"int24"}],
     "name":"Swap","type":"event"},
    {"anonymous":False,"inputs":[
        {"indexed":True,"internalType":"address","name":"sender","type":"address"},
        {"indexed":True,"internalType":"address","name":"owner","type":"address"},
        {"indexed":False,"internalType":"int24","name":"tickLower","type":"int24"},
        {"indexed":False,"internalType":"int24","name":"tickUpper","type":"int24"},
        {"indexed":False,"internalType":"uint128","name":"amount","type":"uint128"},
        {"indexed":False,"internalType":"uint256","name":"amount0","type":"uint256"},
        {"indexed":False,"internalType":"uint256","name":"amount1","type":"uint256"}],
     "name":"Mint","type":"event"},
    {"anonymous":False,"inputs":[
        {"indexed":True,"internalType":"address","name":"owner","type":"address"},
        {"indexed":True,"internalType":"address","name":"recipient","type":"address"},
        {"indexed":False,"internalType":"int24","name":"tickLower","type":"int24"},
        {"indexed":False,"internalType":"int24","name":"tickUpper","type":"int24"},
        {"indexed":False,"internalType":"uint128","name":"amount","type":"uint128"},
        {"indexed":False,"internalType":"uint256","name":"amount0","type":"uint256"},
        {"indexed":False,"internalType":"uint256","name":"amount1","type":"uint256"}],
     "name":"Burn","type":"event"},
    {"anonymous":False,"inputs":[
        {"indexed":True,"internalType":"address","name":"sender","type":"address"},
        {"indexed":True,"internalType":"address","name":"recipient","type":"address"},
        {"indexed":False,"internalType":"uint256","name":"amount0","type":"uint256"},
        {"indexed":False,"internalType":"uint256","name":"amount1","type":"uint256"}],
     "name":"Collect","type":"event"},
    {"anonymous":False,"inputs":[
        {"indexed":True,"internalType":"address","name":"sender","type":"address"},
        {"indexed":True,"internalType":"address","name":"recipient","type":"address"},
        {"indexed":False,"internalType":"uint256","name":"amount0","type":"uint256"},
        {"indexed":False,"internalType":"uint256","name":"amount1","type":"uint256"},
        {"indexed":False,"internalType":"uint256","name":"paid0","type":"uint256"},
        {"indexed":False,"internalType":"uint256","name":"paid1","type":"uint256"}],
     "name":"Flash","type":"event"},
    {"anonymous":False,"inputs":[
        {"indexed":False,"internalType":"uint160","name":"sqrtPriceX96","type":"uint160"},
        {"indexed":False,"internalType":"int24","name":"tick","type":"int24"}],
     "name":"Initialize","type":"event"},
]

pool = w3.eth.contract(address=POOL, abi=POOL_ABI)

# ---------- sanity checks ----------
def retry_call(func, max_retries=5, initial_delay=1.0):
    """Wrapper to retry any RPC call with exponential backoff"""
    retry_delay = initial_delay
    for attempt in range(max_retries):
        try:
            return func()
        except Exception as e:
            if "rate limited" in str(e).lower() and attempt < max_retries - 1:
                time.sleep(retry_delay)
                retry_delay *= 2
            else:
                raise

chain_id = retry_call(lambda: w3.eth.chain_id)
code = retry_call(lambda: w3.eth.get_code(POOL))
assert code not in (b"", b"\x00"), "POOL is not a contract on this chain/RPC"

# ---------- metadata ----------
def erc20_meta(addr):
    c = w3.eth.contract(address=addr, abi=ERC20_ABI)
    try: sym = retry_call(lambda: c.functions.symbol().call())
    except: sym = "UNK"
    try: dec = retry_call(lambda: c.functions.decimals().call())
    except: dec = 18
    return sym, dec

TOKEN0 = retry_call(lambda: pool.functions.token0().call())
TOKEN1 = retry_call(lambda: pool.functions.token1().call())
fee = retry_call(lambda: pool.functions.fee().call())
slot0 = retry_call(lambda: pool.functions.slot0().call())
sqrtP = slot0[0]
sym0, dec0 = erc20_meta(TOKEN0)
sym1, dec1 = erc20_meta(TOKEN1)
price1_per_0 = (sqrtP * sqrtP) / (1 << 192) * (10 ** (dec0 - dec1))

# ---------- Get pool reserves ----------
# Query the actual token balances held by the pool at a specific block
def get_pool_reserves(block_number=None):
    """Get the token reserves in the pool at a specific block (or latest if None)"""
    erc20_balance_abi = [{
        "constant": True,
        "inputs": [{"name": "_owner", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "balance", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function"
    }]
    
    # Use the module-level TOKEN0 and TOKEN1 constants
    addr0 = Web3.to_checksum_address(TOKEN0)
    addr1 = Web3.to_checksum_address(TOKEN1)
    token0_contract = w3.eth.contract(address=addr0, abi=erc20_balance_abi)
    token1_contract = w3.eth.contract(address=addr1, abi=erc20_balance_abi)
    
    # If block_number specified, query historical state
    if block_number:
        balance0_raw = retry_call(lambda: token0_contract.functions.balanceOf(POOL).call(block_identifier=block_number))
        time.sleep(0.6)  # Rate limiting
        balance1_raw = retry_call(lambda: token1_contract.functions.balanceOf(POOL).call(block_identifier=block_number))
    else:
        balance0_raw = retry_call(lambda: token0_contract.functions.balanceOf(POOL).call())
        time.sleep(0.6)  # Rate limiting
        balance1_raw = retry_call(lambda: token1_contract.functions.balanceOf(POOL).call())
    
    # Convert to human-readable amounts
    balance0 = balance0_raw / (10 ** dec0)
    balance1 = balance1_raw / (10 ** dec1)
    
    return balance0, balance1, balance0_raw, balance1_raw

# ---------- event topics ----------
def sig(s): return w3.keccak(text=s).hex()
ALL_TOPICS = {
    "Swap": sig("Swap(address,address,int256,int256,uint160,uint128,int24)"),
    "Mint": sig("Mint(address,address,int24,int24,uint128,uint256,uint256)"),
    "Burn": sig("Burn(address,address,int24,int24,uint128,uint256,uint256)"),
    "Collect": sig("Collect(address,address,uint256,uint256)"),
    "Flash": sig("Flash(address,address,uint256,uint256,uint256,uint256)"),
    "Initialize": sig("Initialize(uint160,int24)"),
}
SCAN_TOPIC_HASHES = [ALL_TOPICS[n] for n in SCAN_TOPICS]

# ---------- block helpers (optional) ----------
def block_ts(b): return w3.eth.get_block(b).timestamp

def block_for_time(ts_utc: int, lo=1, hi=None):
    if hi is None: hi = w3.eth.block_number
    while lo < hi:
        mid = (lo + hi) // 2
        if block_ts(mid) < ts_utc:
            lo = mid + 1
        else:
            hi = mid
    return lo

if FROM_BLOCK is None or TO_BLOCK is None:
    if FROM_UTC and TO_UTC:
        t0 = int(datetime.strptime(FROM_UTC, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp())
        t1 = int(datetime.strptime(TO_UTC,   "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp())
        FROM_BLOCK = block_for_time(t0)
        TO_BLOCK   = block_for_time(t1)
    else:
        latest = w3.eth.block_number
        FROM_BLOCK = max(0, latest - 1_000_000)
        TO_BLOCK   = latest

# ---------- decoders ----------
event_abi_by_name = {e["name"]: e for e in POOL_ABI if e.get("type")=="event"}
decoders = {name: (lambda log, abi=event_abi_by_name[name]: get_event_data(w3.codec, abi, log)) for name in event_abi_by_name}

def effective_gas_price(rcpt, tx):
    return rcpt.get("effectiveGasPrice") or tx.get("gasPrice", 0)

# ---------- scanner (topic-filtered) ----------
def scan_pool_events(from_b, to_b, topic_hashes):
    events, tx_costs = [], {}
    cur = from_b
    latest = w3.eth.block_number if to_b == "latest" else to_b
    while cur <= latest:
        end = min(cur + CHUNK - 1, latest)
        # fetch per-topic in separate calls (keeps responses small & reliable)
        total_logs = 0
        for topic0 in topic_hashes:
            flt = {
                "fromBlock": cur,
                "toBlock": end,
                "address": POOL,
                "topics": [topic0]
            }
            # Retry logic for rate limiting (100 req/min = ~1.67 req/sec)
            max_retries = 5
            retry_delay = 2.0
            logs = None
            for attempt in range(max_retries):
                try:
                    logs = w3.eth.get_logs(flt)
                    time.sleep(0.6)  # Rate limiting: ~1.67 req/sec
                    break
                except Exception as e:
                    error_str = str(e).lower()
                    if ("rate limited" in error_str or "connection" in error_str or "remote" in error_str) and attempt < max_retries - 1:
                        time.sleep(retry_delay)
                        retry_delay *= 2
                    else:
                        raise
            
            if logs is None:
                logs = []
            
            total_logs += len(logs)
            # derive event name from topic
            name = next((k for k, v in ALL_TOPICS.items() if v == topic0), None)
            if not name:
                continue
            for lg in logs:
                ev = decoders[name](lg)
                txh = lg["transactionHash"]
                if txh not in tx_costs:
                    # Retry logic for transaction/receipt fetches (100 req/min limit)
                    max_retries = 5
                    retry_delay = 2.0
                    for attempt in range(max_retries):
                        try:
                            tx = w3.eth.get_transaction(txh)
                            time.sleep(0.6)  # Rate limiting
                            rc = w3.eth.get_transaction_receipt(txh)
                            time.sleep(0.6)  # Rate limiting
                            blk = w3.eth.get_block(rc["blockNumber"])
                            time.sleep(0.6)  # Rate limiting
                            break
                        except Exception as e:
                            error_str = str(e).lower()
                            if ("rate limited" in error_str or "connection" in error_str or "remote" in error_str) and attempt < max_retries - 1:
                                time.sleep(retry_delay)
                                retry_delay *= 2
                            else:
                                raise
                    
                    # Only get reserves AFTER transaction (before = previous tx's after)
                    time.sleep(0.6)  # Rate limiting
                    reserve0_after, reserve1_after, _, _ = get_pool_reserves(rc["blockNumber"])
                    
                    tx_costs[txh] = {
                        "tx_hash": txh.hex(),
                        "from": tx["from"],
                        "block": rc["blockNumber"],
                        "timestamp": blk["timestamp"],
                        "gasUsed": rc["gasUsed"],
                        "effectiveGasPrice": effective_gas_price(rc, tx),
                        "gasPaidWei": rc["gasUsed"] * effective_gas_price(rc, tx),
                        "status": rc["status"],
                        "reserve0_after": reserve0_after,
                        "reserve1_after": reserve1_after,
                    }
                    print(f"  Processed tx {len(tx_costs)}: {txh.hex()[:12]}...")
                    
                row = {
                    "event": name,
                    "tx_hash": txh.hex(),
                    "block": lg["blockNumber"],
                    "timestamp": tx_costs[txh]["timestamp"],
                    "log_index": lg["logIndex"],
                    "token_0_balance_after": tx_costs[txh]["reserve0_after"],
                    "token_1_balance_after": tx_costs[txh]["reserve1_after"],
                }
                for k, v in ev["args"].items():
                    row[k] = v.hex() if isinstance(v, bytes) else v
                events.append(row)
        print(f"Chunk {cur}-{end}: {total_logs} events, {len(tx_costs)} unique txs so far")
        time.sleep(DELAY_BETWEEN_CHUNKS)
        cur = end + 1
    return events, list(tx_costs.values())

# ---------- run ----------
print(f"Scanning blocks {FROM_BLOCK} to {TO_BLOCK}...")
events, txs = scan_pool_events(FROM_BLOCK, TO_BLOCK, SCAN_TOPIC_HASHES)

# ---------- save ----------
df_events = pd.DataFrame(events).sort_values(["block","log_index"]) if events else pd.DataFrame(events)
df_txs = pd.DataFrame(txs).sort_values(["block"]) if txs else pd.DataFrame(txs)

# Calculate "before" from previous transaction's "after"
if not df_events.empty:
    df_events["token_0_balance_before"] = df_events["token_0_balance_after"].shift(1)
    df_events["token_1_balance_before"] = df_events["token_1_balance_after"].shift(1)
    # First transaction has no previous, fill with NaN or query it separately if needed`

out_dir = os.path.join("data", "pool_data")
os.makedirs(out_dir, exist_ok=True)

df_events.to_csv(os.path.join(out_dir, "pool_events.csv"), index=False)
df_txs.to_csv(os.path.join(out_dir, "tx_costs.csv"), index=False)

print(f"COMPLETED: events={len(events)}, unique_txs={len(txs)}, saved to pool_events.csv & tx_costs.csv")

# ---------- liquidity by tick/range (Uniswap v3-style) ----------
# Liquidity in v3 is provided between tickLower and tickUpper via Mint/Burn events.
# We build liquidityNet per tick and then cumulative-sum to get active liquidity by tick.
try:
    if not df_events.empty and set(df_events["event"].unique()) & {"Mint", "Burn"}:
        liq_changes = {}
        for _, r in df_events.iterrows():
            if r["event"] == "Mint":
                tL, tU, amt = int(r["tickLower"]), int(r["tickUpper"]), int(r["amount"])
                liq_changes[tL] = liq_changes.get(tL, 0) + amt
                liq_changes[tU] = liq_changes.get(tU, 0) - amt
            elif r["event"] == "Burn":
                tL, tU, amt = int(r["tickLower"]), int(r["tickUpper"]), int(r["amount"])
                # Burn removes liquidity between [tL, tU)
                liq_changes[tL] = liq_changes.get(tL, 0) - amt
                liq_changes[tU] = liq_changes.get(tU, 0) + amt

        if liq_changes:
            ticks_sorted = sorted(liq_changes.keys())
            active = 0
            rows_tick = []
            for tk in ticks_sorted:
                active += liq_changes[tk]
                # price of token1 per token0 at this tick
                # Uniswap v3 tick definition: price = 1.0001**tick * 10**(dec0 - dec1)
                price_1_per_0 = (1.0001 ** tk) * (10 ** (dec0 - dec1))
                rows_tick.append({
                    "tick": tk,
                    "active_liquidity": active,
                    "price1_per_0": price_1_per_0,
                })
            df_liq_ticks = pd.DataFrame(rows_tick)
            df_liq_ticks.to_csv(os.path.join(out_dir, "liquidity_by_tick.csv"), index=False)

            # Also derive contiguous ranges between successive ticks with constant liquidity
            rows_ranges = []
            for i in range(len(ticks_sorted) - 1):
                tL = ticks_sorted[i]
                tU = ticks_sorted[i + 1]
                # active liquidity after applying change at tL
                # find active_liquidity at tL in df_liq_ticks
                active_here = int(df_liq_ticks.loc[df_liq_ticks.tick == tL, "active_liquidity"].iloc[0])
                p_low = (1.0001 ** tL) * (10 ** (dec0 - dec1))
                p_high = (1.0001 ** tU) * (10 ** (dec0 - dec1))
                rows_ranges.append({
                    "tickLower": tL,
                    "tickUpper": tU,
                    "active_liquidity": active_here,
                    "price_low_1_per_0": p_low,
                    "price_high_1_per_0": p_high,
                })
            df_liq_ranges = pd.DataFrame(rows_ranges)
            df_liq_ranges = df_liq_ranges.sort_values("active_liquidity", ascending=False)
            df_liq_ranges.to_csv(os.path.join(out_dir, "liquidity_ranges_top.csv"), index=False)

            print("Liquidity by tick saved to liquidity_by_tick.csv")
            print("Top ranges (by active liquidity) saved to liquidity_ranges_top.csv")
        else:
            print("No Mint/Burn-derived liquidity changes found in the selected window.")
    else:
        print("Mint/Burn events not present in df_events — update SCAN_TOPICS to include 'Mint' and 'Burn' and rescan.")
except Exception as e:
    print(f"[warn] Failed to compute liquidity map: {e}")
