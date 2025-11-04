# record_gif.py
# pip install pillow imageio web3 pandas matplotlib

import time, imageio.v2 as imageio
from pathlib import Path
from data import Web3, RPC, POOL, fetch_liquidity_rows, plot_snapshot

FRAMES_DIR = Path("frames")
GIF_PATH    = Path("liquidity_timelapse.gif")
INTERVAL_S  = 10            # snapshot every 10 seconds
TOTAL_MIN   = 5             # record for 5 minutes
FPS         = 4             # playback speed for GIF

def main():
    w3 = Web3(Web3.HTTPProvider(RPC))
    assert w3.is_connected()

    FRAMES_DIR.mkdir(parents=True, exist_ok=True)
    frames = []
    nshots = int((TOTAL_MIN * 60) // INTERVAL_S)

    # Fix a zoom window around the *initial* current price (helps visual stability)
    df0, info0 = fetch_liquidity_rows(w3, POOL)
    price0 = info0["curr_price"]
    zoom = (price0*0.7, price0*1.3)

    for i in range(nshots):
        df, info = fetch_liquidity_rows(w3, POOL)
        fn = FRAMES_DIR / f"liquidity_{i:04d}.png"
        plot_snapshot(df, info, POOL, outfile=str(fn), xlog=True, xlim=zoom)
        frames.append(imageio.imread(fn))
        print(f"[{i+1}/{nshots}] frame @ {time.strftime('%H:%M:%S')} saved:", fn)
        time.sleep(INTERVAL_S)

    imageio.mimsave(GIF_PATH, frames, fps=FPS, loop=0)
    print("GIF saved →", GIF_PATH)

if __name__ == "__main__":
    main()
