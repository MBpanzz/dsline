#!/usr/bin/env bash
# Windows (Git Bash) test lane for dsline — the development-host lane.
#
# Linux/WSL is the other, mandatory lane: see docs/development.md and
# scripts/wsl-test.sh. Every change is verified on both.
#
# Usage (Git Bash):
#   scripts/windows-test.sh all
#
# Subcommands: rust | python | verify | bench | all
#
# Why the environment is pinned here: `maturin develop` needs an environment
# to install into. Without CONDA_PREFIX / VIRTUAL_ENV it fails with
# "Couldn't find a virtualenv or conda environment" — and because the failure
# is easy to miss in a piped log, the lane silently keeps testing the
# previously built extension. `python -m maturin develop` is called with
# CONDA_PREFIX set, and the built extension's timestamp is printed so a stale
# build is visible.
#
# Environment overrides:
#   DSLINE_CONDA_PREFIX  conda env root (default: the miniforge3 base env)
#   DSLINE_PYTHON        interpreter to build/install into and to test with

set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONDA_PREFIX_DIR="${DSLINE_CONDA_PREFIX:-C:/Users/86177/miniforge3}"
PY="${DSLINE_PYTHON:-$CONDA_PREFIX_DIR/python.exe}"

say() { printf '\n=== %s ===\n' "$*"; }
die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

require_ready() {
  [ -x "$PY" ] || die "interpreter not found: $PY (set DSLINE_PYTHON)"
  "$PY" -c "import sys" >/dev/null || die "cannot run $PY"
}

cmd_rust() {
  cd "$REPO"
  # The pyo3 build script must find an interpreter with a linkable
  # python3.lib; the MSYS2 python on PATH has none (LNK1181).
  export PYO3_PYTHON="$PY"
  say "rust: cargo fmt --all --check"
  cargo fmt --all --check
  say "rust: cargo test --workspace (PYO3_PYTHON=$PYO3_PYTHON)"
  cargo test --workspace
}

cmd_python() {
  require_ready
  cd "$REPO"
  say "python: maturin develop${DSLINE_RELEASE:+ --release}"
  MATURIN_ARGS=()
  [ -n "${DSLINE_RELEASE:-}" ] && MATURIN_ARGS+=(--release)
  CONDA_PREFIX="$CONDA_PREFIX_DIR" "$PY" -m maturin develop \
    --manifest-path crates/dsline-python/Cargo.toml "${MATURIN_ARGS[@]}"
  say "python: built extension"
  ls -l "$REPO/python/dsline/_dsline.pyd" || die "extension not built"
  "$PY" -c "import dsline; print('dsline', dsline.__version__, 'from', dsline.__file__)"
  say "python: unittest discover -s tests/python"
  "$PY" -m unittest discover -s tests/python -v
}

cmd_bench() {
  require_ready
  cd "$REPO"
  # Performance numbers must come from an optimized build (see
  # scripts/wsl-test.sh bench and docs/development.md §8): `maturin develop`
  # defaults to debug, where the per-byte work dominates the copy path.
  say "bench: building the extension in release"
  CONDA_PREFIX="$CONDA_PREFIX_DIR" "$PY" -m maturin develop --release \
    --manifest-path crates/dsline-python/Cargo.toml
  say "bench: cross-process comparison (Windows, paging-file section backend)"
  "$PY" benches/cross_process.py "$@"
}

cmd_verify() {
  require_ready
  cd "$REPO"
  say "verify: solver invariants"
  "$PY" verify/check_invariants.py
}

cmd_all() {
  cmd_rust
  cmd_python
  cmd_verify
}

case "${1:-all}" in
  rust) cmd_rust ;;
  python) cmd_python ;;
  verify) cmd_verify ;;
  bench) shift || true; cmd_bench "$@" ;;
  all) cmd_all ;;
  *) die "unknown subcommand '$1' (rust|python|verify|bench|all)" ;;
esac
