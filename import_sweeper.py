#!/usr/bin/env python3
"""
import_sweeper.py

Scan a Python project for import statements and propose a minimal requirements list.

Features:
- Walks a project directory and parses imports via AST (no execution).
- Classifies imports as stdlib, third-party, or local.
- Maps top-level modules to PyPI distribution names using:
  1) importlib.metadata.packages_distributions() when available in the current env
  2) A built-in common mapping fallback
- (Optional) If packages are installed, can include exact versions with --freeze
- Writes two files by default:
    - found_imports.txt : a readable report of what was found
    - requirements_generated.txt : a proposed requirements.txt

Usage:
    python import_sweeper.py --root .
    python import_sweeper.py --root backend --freeze

Notes:
- Local-module detection is heuristic: any top-level import that matches a top-level
  package/module name found under --root is considered local and excluded.
- For MySQL/Postgres, the mapping defaults to psycopg2-binary and PyMySQL when detected.
- You can provide an extra JSON mapping file to override/extend the mapping:
    python import_sweeper.py --root . --extra-mapping mymap.json

Author: ChatGPT
"""

from __future__ import annotations
import argparse
import ast
import json
import os
import sys
from pathlib import Path
from typing import Dict, Iterable, Set, Tuple

try:
    # Python 3.10+
    from importlib import metadata as importlib_metadata  # type: ignore
except Exception:  # pragma: no cover
    import importlib_metadata  # type: ignore


COMMON_MAPPING: Dict[str, str] = {
    # Web / Flask ecosystem
    "flask": "Flask",
    "flask_cors": "Flask-Cors",
    "flask_sqlalchemy": "Flask-SQLAlchemy",
    "flask_migrate": "Flask-Migrate",
    "flask_socketio": "Flask-SocketIO",
    "werkzeug": "Werkzeug",
    "jinja2": "Jinja2",
    "itsdangerous": "itsdangerous",
    "gunicorn": "gunicorn",
    # Databases
    "sqlalchemy": "SQLAlchemy",
    "psycopg2": "psycopg2-binary",
    "psycopg2_binary": "psycopg2-binary",
    "pymysql": "PyMySQL",
    "mysql": "mysqlclient",
    "mysqlclient": "mysqlclient",
    "mysql_connector": "mysql-connector-python",
    "mysql_connector_python": "mysql-connector-python",
    # HTTP / Utils
    "requests": "requests",
    "python_dotenv": "python-dotenv",
    "dotenv": "python-dotenv",
    # MQTT
    "paho": "paho-mqtt",
    # CORS alias sometimes seen
    "cors": "Flask-Cors",
    # Other common libs
    "gevent": "gevent",
    "eventlet": "eventlet",
    "orjson": "orjson",
    "ujson": "ujson",
    "pydantic": "pydantic",
    "fastapi": "fastapi",
    # GCP (very broad, keep manual)
    # "google": "google",  # too generic; users should map submodules if needed
}

BUILTIN_MODULES: Set[str] = set(sys.builtin_module_names)

if hasattr(sys, "stdlib_module_names"):
    # Python 3.10+: reliable list of stdlib top-level names
    STDLIB: Set[str] = set(sys.stdlib_module_names)  # type: ignore[attr-defined]
else:
    # Fallback: minimal seed; we'll still filter locals and use mapping for the rest.
    STDLIB = BUILTIN_MODULES.union({
        "abc","argparse","asyncio","base64","binascii","bisect","calendar","collections","concurrent",
        "contextlib","copy","csv","ctypes","dataclasses","datetime","decimal","email","enum","functools",
        "fractions","gc","getopt","getpass","gettext","glob","gzip","hashlib","heapq","hmac","html","http",
        "io","ipaddress","itertools","json","logging","math","multiprocessing","numbers","operator","os",
        "pathlib","pickle","pkgutil","platform","plistlib","pprint","profile","pstats","queue","random",
        "re","sched","secrets","select","selectors","shelve","shlex","shutil","signal","site","smtplib",
        "socket","sqlite3","ssl","stat","statistics","string","stringprep","struct","subprocess","sys",
        "tempfile","textwrap","threading","time","timeit","tkinter","token","tokenize","trace","traceback",
        "types","typing","unittest","urllib","uuid","venv","warnings","weakref","zipfile","zoneinfo"
    })


def walk_python_files(root: Path) -> Iterable[Path]:
    for dirpath, dirnames, filenames in os.walk(root):
        # Skip hidden dirs and typical virtualenv / build dirs
        parts = Path(dirpath).parts
        if any(p.startswith(".") for p in parts):
            continue
        if any(skip in parts for skip in ("__pycache__", "venv", ".venv", "env", "build", "dist")):
            continue
        for fname in filenames:
            if fname.endswith(".py"):
                yield Path(dirpath) / fname


def parse_imports(pyfile: Path) -> Set[str]:
    """Return set of top-level module names imported in file."""
    src = pyfile.read_text(encoding="utf-8", errors="ignore")
    try:
        tree = ast.parse(src, filename=str(pyfile))
    except SyntaxError:
        return set()

    mods: Set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                top = (alias.name.split(".")[0]).strip()
                if top:
                    mods.add(top)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                top = (node.module.split(".")[0]).strip()
                if top:
                    # skip explicit relative imports "from .x import y" are given as level>0
                    if getattr(node, "level", 0) == 0:
                        mods.add(top)
    return mods


def discover_local_toplevels(root: Path) -> Set[str]:
    """Heuristic: any directory/file at top-level under root that looks like a package/module."""
    local: Set[str] = set()
    for item in root.iterdir():
        if item.is_dir():
            if (item / "__init__.py").exists():
                local.add(item.name)
            else:
                # also consider routes/, models/, utils/ as local packages even without __init__.py
                if item.name in {"routes", "models", "utils", "tasks", "backend"}:
                    local.add(item.name)
        elif item.is_file() and item.suffix == ".py":
            local.add(item.stem)
    return local


def classify(mods: Set[str], local_names: Set[str]) -> Tuple[Set[str], Set[str], Set[str]]:
    """Return (stdlib, third_party_guess, local) sets."""
    stdlib = set()
    third = set()
    local = set()
    for m in mods:
        if m in local_names:
            local.add(m)
        elif m in BUILTIN_MODULES or m in STDLIB:
            stdlib.add(m)
        else:
            third.add(m)
    return stdlib, third, local


def map_to_distributions(third: Set[str], extra_map: Dict[str, str] | None) -> Dict[str, Set[str]]:
    """
    Return mapping: distribution_name -> {top_level_modules...}
    Uses importlib.metadata.packages_distributions() when available (requires packages installed).
    Falls back to COMMON_MAPPING and user extra map.
    """
    top_to_dists: Dict[str, Set[str]] = {}

    # Attempt environment-based mapping
    env_map = {}
    try:
        env_map = importlib_metadata.packages_distributions()  # {top_level: [dist, ...]}
    except Exception:
        env_map = {}

    def add(dist: str, top: str):
        top_to_dists.setdefault(dist, set()).add(top)

    for top in sorted(third):
        # Prefer environment info if available
        env_dists = env_map.get(top) if env_map else None
        chosen = None
        if env_dists:
            # pick the first distribution (most top-levels map 1:1)
            chosen = env_dists[0]
        else:
            # fallback to mapping dictionaries
            key = top.lower().replace("-", "_")
            if extra_map and key in extra_map:
                chosen = extra_map[key]
            elif key in COMMON_MAPPING:
                chosen = COMMON_MAPPING[key]
            else:
                # Guess: use the top-level as the distribution name (best-effort)
                chosen = top

        add(chosen, top)

    return top_to_dists


def freeze_versions(dists: Iterable[str]) -> Dict[str, str]:
    versions: Dict[str, str] = {}
    for dist in sorted(set(dists)):
        try:
            versions[dist] = importlib_metadata.version(dist)
        except importlib_metadata.PackageNotFoundError:
            versions[dist] = ""  # not installed; leave unpinned
        except Exception:
            versions[dist] = ""
    return versions


def main():
    ap = argparse.ArgumentParser(description="Scrape imports and propose requirements.txt")
    ap.add_argument("--root", type=str, default=".", help="Project root to scan")
    ap.add_argument("--freeze", action="store_true",
                    help="If installed, include exact versions via importlib.metadata")
    ap.add_argument("--extra-mapping", type=str, default=None,
                    help="Path to JSON {top_level_module: distribution_name} mapping")
    ap.add_argument("--output-prefix", type=str, default="",
                    help="Prefix for output files (default: none)")
    args = ap.parse_args()

    root = Path(args.root).resolve()
    if not root.exists():
        print(f"[ERROR] Root path not found: {root}", file=sys.stderr)
        sys.exit(2)

    extra_map = None
    if args.extra_mapping:
        try:
            extra_map = json.loads(Path(args.extra_mapping).read_text(encoding="utf-8"))
        except Exception as e:
            print(f"[WARN] Failed to read extra mapping: {e}", file=sys.stderr)

    all_mods: Set[str] = set()
    files_scanned = 0
    for py in walk_python_files(root):
        files_scanned += 1
        mods = parse_imports(py)
        all_mods.update(mods)

    local_names = discover_local_toplevels(root)
    stdlib, third, local = classify(all_mods, local_names)

    dist_map = map_to_distributions(third, extra_map)
    dists = sorted(dist_map.keys())
    versions = freeze_versions(dists) if args.freeze else {d: "" for d in dists}

    # Compose requirements lines
    req_lines = []
    for dist in dists:
        ver = versions.get(dist, "")
        if ver:
            req_lines.append(f"{dist}=={ver}")
        else:
            req_lines.append(dist)

    # Prepare report
    report = []
    report.append(f"Scanned root: {root}")
    report.append(f"Python files scanned: {files_scanned}")
    report.append("")
    report.append(f"Discovered local packages/modules: {', '.join(sorted(local_names)) or '(none)'}")
    report.append("")
    report.append("Stdlib imports:")
    for m in sorted(stdlib):
        report.append(f"  - {m}")
    report.append("")
    report.append("Third-party imports (top-level):")
    for m in sorted(third):
        report.append(f"  - {m}")
    report.append("")
    report.append("Distribution mapping:")
    for dist in dists:
        tops = ", ".join(sorted(dist_map[dist]))
        ver = versions.get(dist, "")
        report.append(f"  - {dist}{'=='+ver if ver else ''}  <-- {tops}")

    # Write outputs
    prefix = args.output_prefix
    out_req = Path(f"{prefix}requirements_generated.txt")
    out_rep = Path(f"{prefix}found_imports.txt")

    out_req.write_text("\\n".join(req_lines) + "\\n", encoding="utf-8")
    out_rep.write_text("\\n".join(report) + "\\n", encoding="utf-8")

    print(f"[OK] Wrote {out_req} and {out_rep}")
    print("Preview requirements:")
    print("-" * 40)
    print("\\n".join(req_lines))

if __name__ == "__main__":
    main()
