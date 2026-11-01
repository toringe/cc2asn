"""Temporary deploy diagnostic boot (removed immediately after)."""
import os
import stat
import sys
from pathlib import Path


def main(event, ctx):
    info = {
        "AWS_LAMBDA_EXEC_WRAPPER": os.environ.get("AWS_LAMBDA_EXEC_WRAPPER"),
        "bootstrap_exists": os.path.exists("/opt/bootstrap"),
        "lambda_runtime_api": os.environ.get("AWS_LAMBDA_RUNTIME_API"),
        "opt_listing": sorted(os.listdir("/opt")) if os.path.isdir("/opt") else None,
    }
    bootstrap = Path("/opt/bootstrap")
    if bootstrap.exists():
        info["bootstrap_mode"] = stat.S_IMODE(bootstrap.stat().st_mode)
        info["bootstrap_head"] = bootstrap.read_bytes()[:200].decode("utf-8", "replace")
    print("BOOT_PROBE " + repr(info))
    sys.stdout.flush()
    return info
