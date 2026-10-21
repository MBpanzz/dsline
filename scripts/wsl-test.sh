#!/usr/bin/env bash
# Linux test lane for dsline, run inside WSL (Ubuntu).
#
# Why this lane exists: Linux is the tier-1 platform (ROADMAP.md §13.1) and
# the only place where the transport's real numbers live — MAP_SHARED needs
# no FlushViewOfFile, so the Windows flush cost that dominates
# docs/benchmarks.md does not exist here. Windows stays the development
# host; every change is verified on both.
#
# Usage (from Windows):
#   MSYS_NO_PATHCONV=1 wsl.exe -d Ubuntu-24.04 -- bash /mnt/c/Simpl/dsline/scripts/wsl-test.sh all
# Usage (inside WSL):
#   scripts/wsl-test.sh all
#
# Subcommands: setup | rust | python | verify | bench | all
#
# Environment overrides:
#   DSLINE_REPO         repo root (default: parent of this script)
#   DSLINE_VENV         venv path (default: ~/.venvs/dsline)
#   DSLINE_CARGO_TARGET cargo target dir (default: ~/.cache/dsline-target)
#   DSLINE_INSTALL      "1" to allow setup to install missing toolchains
#
# Slow-network overrides (measured on this machine: the official Rust CDN
# delivered ~31 KB/s, mirrors ~5-10 MB/s):
#   DSLINE_RUSTUP_DIST_SERVER  e.g. https://mirrors.ustc.edu.cn/rust-static
#   DSLINE_RUSTUP_UPDATE_ROOT  defaults to <dist-server>/rustup
#   DSLINE_CARGO_MIRROR        sparse index URL, e.g.
#                              sparse+https://mirrors.ustc.edu.cn/crates.io-index/
#                              (written to ~/.cargo/config.toml if that file
#                               does not exist yet)
#   DSLINE_PIP_INDEX_URL       e.g. https://pypi.tuna.tsinghua.edu.cn/simple

set -euo pipefail

REPO="${DSLINE_REPO:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
VENV="${DSLINE_VENV:-$HOME/.venvs/dsline}"
CARGO_TARGET="${DSLINE_CARGO_TARGET:-$HOME/.cache/dsline-target}"
PY="$VENV/bin/python"
ALLOW_INSTALL="${DSLINE_INSTALL:-0}"

export CARGO_TARGET_DIR="$CARGO_TARGET"
export PATH="$HOME/.cargo/bin:$PATH"
# Rust builds must not land on the Windows filesystem (9p is ~10x slower);
# sources stay on /mnt/c so the lane tests the working tree, not a copy.
unset CARGO_BUILD_TARGET

say() { printf '\n=== %s ===\n' "$*"; }
die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

need_install() {
  [ "$ALLOW_INSTALL" = "1" ] || die "$1 is missing; re-run with DSLINE_INSTALL=1 to install it"
}

cmd_setup() {
  say "setup: rust toolchain"
  if ! command -v cargo >/dev/null 2>&1; then
    need_install "cargo (rustup)"
    if [ -n "${DSLINE_RUSTUP_DIST_SERVER:-}" ]; then
      export RUSTUP_DIST_SERVER="$DSLINE_RUSTUP_DIST_SERVER"
      export RUSTUP_UPDATE_ROOT="${DSLINE_RUSTUP_UPDATE_ROOT:-$DSLINE_RUSTUP_DIST_SERVER/rustup}"
      echo "using rustup mirror: $RUSTUP_DIST_SERVER"
    fi
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
      | sh -s -- -y --profile minimal --default-toolchain stable
    # shellcheck disable=SC1091
    . "$HOME/.cargo/env"
  fi
  cargo --version
  rustc --version

  # The minimal rustup profile omits rustfmt, which `cargo fmt --check` needs.
  if ! cargo fmt --version >/dev/null 2>&1; then
    say "setup: rustfmt component"
    rustup component add rustfmt
  fi
  cargo fmt --version

  if [ -n "${DSLINE_CARGO_MIRROR:-}" ] && [ ! -f "$HOME/.cargo/config.toml" ]; then
    say "setup: cargo registry mirror"
    mkdir -p "$HOME/.cargo"
    cat > "$HOME/.cargo/config.toml" <<EOF
# Written by dsline scripts/wsl-test.sh (DSLINE_CARGO_MIRROR).
# Delete this file to go back to the official crates.io registry.
[source.crates-io]
replace-with = "mirror"

[source.mirror]
registry = "$DSLINE_CARGO_MIRROR"
EOF
    echo "wrote $HOME/.cargo/config.toml -> $DSLINE_CARGO_MIRROR"
  fi

  say "setup: python venv at $VENV"
  if [ ! -x "$PY" ]; then
    need_install "venv module (apt install python3-venv)"
    # --system-site-packages keeps the solver deps (z3, ortools, pyscipopt)
    # that the distro python already has visible to verify/check_invariants.py.
    python3 -m venv --system-site-packages "$VENV"
  fi
  "$PY" -V

  say "setup: maturin"
  if ! "$PY" -c "import maturin" >/dev/null 2>&1; then
    need_install "maturin (pip install maturin)"
    PIP_ARGS=()
    [ -n "${DSLINE_PIP_INDEX_URL:-}" ] && PIP_ARGS+=(--index-url "$DSLINE_PIP_INDEX_URL")
    "$PY" -m pip install --quiet --upgrade pip "${PIP_ARGS[@]}"
    "$PY" -m pip install --quiet "maturin>=1.5,<2" "${PIP_ARGS[@]}"
  fi
  "$PY" -m maturin --version

  say "setup: solver deps (for the verify lane)"
  "$PY" - <<'PYCHECK'
import importlib
missing = [m for m in ("z3", "ortools", "pyscipopt") if not importlib.util.find_spec(m)]
print("solver deps OK" if not missing else "solver deps MISSING: " + ", ".join(missing))
PYCHECK

  say "setup done"
  echo "repo         : $REPO"
  echo "cargo target : $CARGO_TARGET"
  echo "venv python  : $PY"
  echo "XDG_RUNTIME_DIR=${XDG_RUNTIME_DIR:-<unset>}  (named shm regions live here)"
}

require_ready() {
  command -v cargo >/dev/null 2>&1 || die "cargo missing; run: $0 setup"
  [ -x "$PY" ] || die "venv missing; run: $0 setup"
}

cmd_rust() {
  require_ready
  cd "$REPO"
  say "rust: cargo fmt --all --check"
  cargo fmt --all --check
  say "rust: cargo test --workspace"
  cargo test --workspace
}

cmd_python() {
  require_ready
  cd "$REPO"
  say "python: maturin develop${DSLINE_RELEASE:+ --release}"
  MATURIN_ARGS=()
  [ -n "${DSLINE_RELEASE:-}" ] && MATURIN_ARGS+=(--release)
  VIRTUAL_ENV="$VENV" "$VENV/bin/maturin" develop \
    --manifest-path crates/dsline-python/Cargo.toml "${MATURIN_ARGS[@]}"
  say "python: unittest discover -s tests/python"
  "$PY" -m unittest discover -s tests/python -v
}

cmd_verify() {
  require_ready
  cd "$REPO"
  say "verify: solver invariants"
  "$PY" verify/check_invariants.py
}

cmd_bench() {
  require_ready
  cd "$REPO"
  # Benchmark numbers must come from an optimized build: `maturin develop`
  # defaults to debug, where the per-byte checksum alone dominates the copy
  # path (measured: a 4 KiB checksum is ~16 µs in debug). Wheels are built in
  # release, so release is the honest configuration to measure.
  say "bench: building the extension in release"
  VIRTUAL_ENV="$VENV" "$VENV/bin/maturin" develop --release \
    --manifest-path crates/dsline-python/Cargo.toml
  say "bench: cross-process comparison (Linux, MAP_SHARED, no flush)"
  "$PY" benches/cross_process.py "$@"
}

cmd_all() {
  cmd_setup
  cmd_rust
  cmd_python
  cmd_verify
}

case "${1:-all}" in
  setup) cmd_setup ;;
  rust) cmd_rust ;;
  python) cmd_python ;;
  verify) cmd_verify ;;
  bench) shift || true; cmd_bench "$@" ;;
  all) cmd_all ;;
  *) die "unknown subcommand '$1' (setup|rust|python|verify|bench|all)" ;;
esac
