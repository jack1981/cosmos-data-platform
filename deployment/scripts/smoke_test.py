#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
import urllib.error
import urllib.request


def check_http(url: str, timeout: int) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            status = getattr(response, "status", 200)
            if status >= 400:
                print(f"[FAIL] HTTP check failed: {url} returned {status}")
                return False
    except urllib.error.URLError as exc:
        print(f"[FAIL] HTTP check failed: {url} ({exc})")
        return False

    print(f"[OK] HTTP check passed: {url}")
    return True


def _resolve_ray_target(ray_mode: str, ray_address: str) -> str | None:
    value = ray_address.strip()
    if value:
        if value.lower() == "none":
            return None
        if value.lower() == "auto":
            return "auto" if ray_mode == "k8s" else None
        return value

    if ray_mode == "k8s":
        return "ray://ray-head:10001"

    return None


def check_runtime(ray_mode: str, ray_address: str) -> bool:
    try:
        import ray
    except Exception as exc:  # pragma: no cover - runtime-dependent
        print(f"[FAIL] Ray import failed: {exc}")
        return False

    target = _resolve_ray_target(ray_mode, ray_address)
    kwargs: dict[str, object] = {
        "ignore_reinit_error": True,
        "log_to_driver": False,
    }
    if target:
        kwargs["address"] = target

    try:
        ray.init(**kwargs)
        alive_nodes = [node for node in ray.nodes() if node.get("Alive")]
        if not alive_nodes:
            print("[FAIL] Runtime check failed: Ray returned no alive nodes")
            return False
        print(
            "[OK] Runtime Ray connectivity passed "
            f"(mode={ray_mode}, address={target or 'local-new-cluster'}, alive_nodes={len(alive_nodes)})"
        )
        return True
    except Exception as exc:  # pragma: no cover - runtime-dependent
        print(
            "[FAIL] Runtime Ray connectivity failed "
            f"(mode={ray_mode}, address={target or 'local-new-cluster'}): {exc}"
        )
        return False
    finally:
        try:
            ray.shutdown()
        except Exception:
            pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Cosmos Xenna deployment smoke checks")
    parser.add_argument(
        "--component",
        choices=["all", "api", "web", "runtime"],
        default="all",
        help="Which component to test",
    )
    parser.add_argument("--api-url", default="http://127.0.0.1:8000/healthz")
    parser.add_argument("--web-url", default="http://127.0.0.1:3000/")
    parser.add_argument("--timeout", type=int, default=8)
    parser.add_argument("--ray-mode", default=os.getenv("RAY_MODE", "local"))
    parser.add_argument("--ray-address", default=os.getenv("RAY_ADDRESS", "auto"))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    requested = args.component

    checks = {
        "api": lambda: check_http(args.api_url, args.timeout),
        "web": lambda: check_http(args.web_url, args.timeout),
        "runtime": lambda: check_runtime(args.ray_mode, args.ray_address),
    }

    if requested == "all":
        selected = ["api", "web", "runtime"]
    else:
        selected = [requested]

    failures = [name for name in selected if not checks[name]()]
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
