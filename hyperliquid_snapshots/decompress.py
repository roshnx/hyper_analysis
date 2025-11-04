from __future__ import annotations

import os
import shutil
import subprocess
from typing import Optional


def _try_python_lz4(src_path: str, dst_path: str) -> bool:
    try:
        import lz4.frame as lz4f  # type: ignore
    except Exception:
        return False
    # Stream copy to avoid large memory use
    with open(src_path, "rb") as fin, open(dst_path, "wb") as fout:
        dctx = lz4f.LZ4FrameDecompressor()
        while True:
            chunk = fin.read(1024 * 1024)
            if not chunk:
                break
            data = dctx.decompress(chunk)
            if data:
                fout.write(data)
    return True


def _try_unlz4_tool(src_path: str, dst_path: str) -> bool:
    # Prefer unlz4 if available
    tool = shutil.which("unlz4") or shutil.which("lz4")  # lz4 -d fallback
    if not tool:
        return False
    if os.path.abspath(dst_path) == os.path.abspath(src_path):
        # unlz4 can replace in place with --rm, but we want explicit output
        # Use temp then move
        tmp_out = dst_path + ".tmp"
        args = (
            [tool, "-d", src_path, tmp_out]
            if os.path.basename(tool).lower() == "lz4"
            else [tool, src_path, tmp_out]
        )
        subprocess.run(args, check=True)
        os.replace(tmp_out, dst_path)
    else:
        args = (
            [tool, "-d", src_path, dst_path]
            if os.path.basename(tool).lower() == "lz4"
            else [tool, src_path, dst_path]
        )
        subprocess.run(args, check=True)
    return True


def default_output_path(src_path: str) -> str:
    # Remove one .lz4 suffix if present
    if src_path.lower().endswith(".lz4"):
        return src_path[: -len(".lz4")]
    return src_path + ".out"


def decompress_lz4_file(
    src_path: str,
    dst_path: Optional[str] = None,
    remove_src: bool = False,
) -> str:
    """Decompress an .lz4 file to destination. Returns the output path.

    Tries Python lz4 first, then falls back to unlz4/lz4 tool if present.
    """
    if not os.path.exists(src_path):
        raise FileNotFoundError(src_path)
    dst = dst_path or default_output_path(src_path)
    os.makedirs(os.path.dirname(dst), exist_ok=True)

    if not _try_python_lz4(src_path, dst):
        if not _try_unlz4_tool(src_path, dst):
            raise RuntimeError(
                "No lz4 decompressor available. Install Python package 'lz4' or system 'unlz4'."
            )

    if remove_src:
        try:
            os.remove(src_path)
        except OSError:
            pass
    return dst
