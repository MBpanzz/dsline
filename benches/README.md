# Benchmarks

The first benchmark target is fixed-slot SPSC bytes throughput compared with `multiprocessing.Queue`.

Current scripts:

- `shm_spsc_bytes.py`: measures the current `dsline.ShmChannel` in-process prototype against an in-process `multiprocessing.Queue` baseline. This is a harness and output-format benchmark, not yet the 0.0.1 dual-process shared-memory benchmark.

Example:

```bash
python -m dsline info
python -m dsline bench shm --message-size 4096 --count 100000 --capacity 1024
dsline bench shm --message-size 4096 --count 100000 --json
python benches/shm_spsc_bytes.py --message-size 4096 --count 100000 --capacity 1024
```
