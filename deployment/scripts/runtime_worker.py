#!/usr/bin/env python3
from __future__ import annotations

import os
import subprocess
import sys
import time


def run_runtime_smoke() -> bool:
    result = subprocess.run(
        [sys.executable, "deployment/scripts/smoke_test.py", "--component", "runtime"],
        check=False,
    )
    return result.returncode == 0


def main() -> int:
    startup_retries = int(os.getenv("RUNTIME_BOOT_RETRIES", "30"))
    startup_sleep_s = int(os.getenv("RUNTIME_BOOT_SLEEP_SECONDS", "2"))
    heartbeat_s = int(os.getenv("RUNTIME_HEARTBEAT_SECONDS", "30"))

    for attempt in range(1, startup_retries + 1):
        if run_runtime_smoke():
            print(f"Runtime worker connected to Ray on attempt {attempt}/{startup_retries}")
            break
        print(f"Runtime worker waiting for Ray ({attempt}/{startup_retries})")
        time.sleep(startup_sleep_s)
    else:
        print("Runtime worker failed to connect to Ray during startup")
        return 1

    while True:
        if not run_runtime_smoke():
            print("Runtime worker heartbeat: Ray connectivity check failed")
        else:
            print("Runtime worker heartbeat: Ray connectivity ok")
        time.sleep(heartbeat_s)


if __name__ == "__main__":
    raise SystemExit(main())
