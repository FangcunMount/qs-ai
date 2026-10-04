"""Retired entry point: result delivery is owned exclusively by the MQ runtime."""


def main() -> None:
    raise SystemExit("Legacy gRPC delivery is retired; use bootstrap.server with MQ enabled")


if __name__ == "__main__":
    main()
