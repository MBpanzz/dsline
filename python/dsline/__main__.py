import argparse

from ._bench import format_results, run_pipeline_bench, run_shm_bench
from ._info import format_info, get_info
from ._shm import cleanup, dir_path, inspect, list_regions, remove


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="dsline")
    subcommands = parser.add_subparsers(dest="command", required=True)

    # ── info ──
    info = subcommands.add_parser("info")
    info.add_argument("--json", action="store_true", help="Emit JSON.")

    # ── shm: named-channel region management ──
    shm = subcommands.add_parser(
        "shm",
        help="Manage named-channel regions (runtime directory).",
        description=(
            "Named channels on Windows live in paging-file sections (no files); "
            "elsewhere they live as dsline-*.bin files in the runtime directory."
        ),
    )
    shm_subcommands = shm.add_subparsers(dest="shm_command", required=True)

    shm_dir = shm_subcommands.add_parser("dir", help="Print the runtime directory.")
    shm_dir.add_argument("--json", action="store_true", help="Emit JSON.")

    list_parser = shm_subcommands.add_parser("list", help="List region files.")
    list_parser.add_argument("--json", action="store_true", help="Emit JSON.")

    inspect_parser = shm_subcommands.add_parser("inspect", help="Inspect one channel's region file.")
    inspect_parser.add_argument("name")
    inspect_parser.add_argument("--json", action="store_true", help="Emit JSON.")

    cleanup_parser = shm_subcommands.add_parser(
        "cleanup", help="Delete region files older than --older-than."
    )
    cleanup_parser.add_argument(
        "--older-than",
        required=True,
        metavar="DURATION",
        help="Age threshold, e.g. 90s, 30m, 2h, 1d.",
    )
    cleanup_parser.add_argument("--json", action="store_true", help="Emit JSON.")

    remove_parser = shm_subcommands.add_parser("remove", help="Delete one channel's region file.")
    remove_parser.add_argument("name")
    remove_parser.add_argument("--json", action="store_true", help="Emit JSON.")

    # ── bench ──
    bench = subcommands.add_parser("bench")
    bench_subcommands = bench.add_subparsers(dest="bench_command", required=True)

    # bench shm
    shm = bench_subcommands.add_parser("shm")
    shm.add_argument("--message-size", type=int, default=4096)
    shm.add_argument("--count", type=int, default=100_000)
    shm.add_argument("--capacity", type=int, default=1024)
    shm.add_argument(
        "--backend",
        choices=("dsline", "multiprocessing-queue", "both"),
        default="both",
    )
    shm.add_argument("--json", action="store_true", help="Emit JSON lines.")

    # bench pipeline
    pipeline = bench_subcommands.add_parser("pipeline")
    pipeline.add_argument("--count", type=int, default=100_000)
    pipeline.add_argument(
        "--backends",
        choices=("pure-python", "dsline-expr", "dsline-py-udf", "all"),
        default="all",
        help="Which backends to benchmark (default: all).",
    )
    pipeline.add_argument(
        "--batch-sizes",
        type=int,
        nargs="*",
        default=None,
        help="Batch sizes for pipeline benchmarks (e.g. 1 16 64 256).",
    )
    pipeline.add_argument("--json", action="store_true", help="Emit JSON lines.")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "info":
        print(format_info(get_info(), args.json))
        return 0

    if args.command == "shm":
        return _handle_shm(args, parser)

    if args.command == "bench":
        return _handle_bench(args, parser)

    parser.error("unsupported command")
    return 2


def _handle_shm(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    try:
        if args.shm_command == "dir":
            print(dir_path(args.json))
        elif args.shm_command == "list":
            print(list_regions(args.json))
        elif args.shm_command == "inspect":
            print(inspect(args.name, args.json))
        elif args.shm_command == "cleanup":
            print(cleanup(args.older_than, args.json))
        elif args.shm_command == "remove":
            print(remove(args.name, args.json))
        else:
            parser.error("unsupported shm command")
            return 2
    except ValueError as exc:
        parser.error(str(exc))
        return 2
    return 0


def _handle_bench(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    if args.bench_command == "shm":
        try:
            results = run_shm_bench(
                message_size=args.message_size,
                count=args.count,
                capacity=args.capacity,
                backend=args.backend,
            )
        except ValueError as exc:
            parser.error(str(exc))
        print(format_results(results, args.json))
        return 0

    if args.bench_command == "pipeline":
        try:
            results = run_pipeline_bench(
                count=args.count,
                backends=args.backends,
                batch_sizes=args.batch_sizes,
            )
        except ValueError as exc:
            parser.error(str(exc))
        print(format_results(results, args.json))
        return 0

    parser.error("unsupported bench command")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
