#!/usr/bin/env python3
from __future__ import annotations

import subprocess
from collections import defaultdict
from pathlib import Path

# Deterministic exclusion rules used for both baseline and final measurements.
EXCLUDE_PARTS = {
    "node_modules",
    "target",
    ".venv",
    ".venv-management",
    ".venv-notebook",
    "__pycache__",
    ".pytest_cache",
    ".ruff_cache",
    ".git",
}
EXCLUDE_SUFFIXES = {
    ".lock",
    ".db",
    ".pyc",
    ".pyo",
    ".pyd",
    ".so",
    ".whl",
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".svg",
    ".ico",
    ".pdf",
    ".zip",
    ".gz",
}
EXCLUDE_FILES = {
    "package-lock.json",
    "Cargo.lock",
    "uv.lock",
}

LANG_MAP = {
    ".py": "Python",
    ".pyi": "Python",
    ".rs": "Rust",
    ".ts": "TypeScript",
    ".tsx": "TypeScript",
    ".js": "JavaScript",
    ".jsx": "JavaScript",
    ".json": "JSON",
    ".yml": "YAML",
    ".yaml": "YAML",
    ".toml": "TOML",
    ".sh": "Shell",
    ".md": "Markdown",
    ".txt": "Text",
    ".ini": "INI",
    ".css": "CSS",
    ".mjs": "JavaScript",
}



def _git_ls_files() -> list[Path]:
    output = subprocess.check_output(["git", "ls-files"], text=True)
    return [Path(line) for line in output.splitlines() if line.strip()]



def _is_excluded(path: Path) -> bool:
    if any(part in EXCLUDE_PARTS for part in path.parts):
        return True
    if path.name in EXCLUDE_FILES:
        return True
    if any(str(path).startswith(prefix) for prefix in ("third_party/", "vendor/", "vendors/", "vendored/")):
        return True
    if any(path.name.endswith(suffix) for suffix in EXCLUDE_SUFFIXES):
        return True
    if "/generated/" in str(path).replace("\\", "/"):
        return True
    return False



def _count_lines(path: Path) -> int:
    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return 0
    return text.count("\n") + (0 if text.endswith("\n") or not text else 1)



def main() -> int:
    per_language: dict[str, int] = defaultdict(int)
    per_path: list[tuple[str, int]] = []

    for path in _git_ls_files():
        if _is_excluded(path):
            continue
        if not path.exists() or not path.is_file():
            continue

        loc = _count_lines(path)
        if loc <= 0:
            continue

        language = LANG_MAP.get(path.suffix.lower(), "Other")
        per_language[language] += loc
        per_path.append((str(path), loc))

    total = sum(per_language.values())

    print("TOTAL_LOC", total)
    print("LANGUAGE_BREAKDOWN")
    for language, loc in sorted(per_language.items(), key=lambda item: (-item[1], item[0])):
        print(f"{language}\t{loc}")
    print("TOP_FILES")
    for path, loc in sorted(per_path, key=lambda item: (-item[1], item[0]))[:25]:
        print(f"{loc}\t{path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
