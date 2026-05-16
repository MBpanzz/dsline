import dsline


def main() -> None:
    with dsline.ShmChannel(
        "demo",
        capacity=4,
        slot_size=64,
        backpressure=dsline.Backpressure.Raise,
    ) as ch:
        ch.send(b"hello")
        assert ch.recv() == b"hello"
        print(f"dsline {dsline.__version__}: bytes channel ok")


if __name__ == "__main__":
    main()
