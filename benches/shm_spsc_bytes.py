import sys

from dsline.__main__ import main


if __name__ == "__main__":
    raise SystemExit(main(["bench", "shm", *sys.argv[1:]]))
