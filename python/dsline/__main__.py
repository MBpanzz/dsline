import argparse

from ._bench import format_results, run_shm_bench
from ._info import format_info, get_info


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="dsline")
    subcommands = parser.add_subparsers(dest="command", required=True)

    info = subcommands.add_parser("info")
    info.add_argument("--json", action="store_true", help="Emit JSON.")

    bench = subcommands.add_parser("bench")
    bench_subcommands = bench.add_subparsers(dest="bench_command", required=True)

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

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "info":
        print(format_info(get_info(), args.json))
        return 0

    if args.command == "bench" and args.bench_command == "shm":
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

    parser.error("unsupported command")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
