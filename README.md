# dsline

`dsline` is a lightweight local dataflow framework for Python data processing and inter-process communication.

Current development follows `ROADMAP.md`. The first implementation target is a fixed-slot SPSC bytes channel. The current PyO3 `ShmChannel` binding is an in-process prototype over the `dsline-shm` fixed-slot SPSC channel; the real OS shared-memory backend is still intentionally gated behind the 0.0.1 implementation work.

## Status

This repository is at the first test-version stage. It is suitable for API, packaging, protocol, and benchmark harness validation, but it is not yet the final dual-process shared-memory transport.

Current boundaries:

- `ShmChannel` is an in-process prototype.
- `send/recv` for existing bytes uses a copy path.
- `alloc/publish` zero-copy APIs are not exposed.
- MPSC, multi-consumer, crash recovery, and real OS shared memory are still future work.

## Install for development

```bash
python -m pip install "maturin>=1.5,<2"
python -m maturin develop --manifest-path crates/dsline-python/Cargo.toml
```

```python
import dsline

with dsline.ShmChannel("demo", capacity=4, slot_size=64) as ch:
    ch.send(b"hello")
    ch.send(memoryview(b"view"))
    assert ch.recv() == b"hello"
    assert ch.recv() == b"view"
```

Zero-copy wording follows the project rule:

> dsline provides true zero-copy transfer only for eligible shared-memory alloc/publish payloads after the safety gate is complete. Existing `bytes`, `bytearray`, and ndarray values sent through `send/recv` are expected to use a single copy into shared memory.

Implemented prototype pieces:

- `dsline-core`: fixed-slot SPSC bytes ring, checksum, Frame header, metadata TLV encode/decode.
- `dsline-shm`: fixed-slot storage trait, in-memory and file-backed storage backends, region state model, and SPSC bytes channel over `FREE`, `WRITING`, `COMMITTED`, `PINNED`, and `CORRUPTED`.
- `dsline-python`: PyO3 `ShmChannel` binding over the `dsline-shm` prototype, Python exception exports, and `send()` support for bytes-like inputs including `bytes`, `bytearray`, and `memoryview`.

## CLI

```bash
python -m dsline info
python -m dsline bench shm --message-size 4096 --count 100000 --json
```

After installation, the console script is also available:

```bash
dsline info
dsline bench shm --message-size 4096 --count 100000 --json
```

## Verification

```bash
cargo fmt --all --check
cargo test
python -m maturin build --manifest-path crates/dsline-python/Cargo.toml --interpreter python
python -m unittest discover -s tests/python
```

## License

MIT License.
