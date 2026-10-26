"""Retired M1 result transfer entry; never loads configuration or opens resources."""

import sys


def main() -> None:
    print(
        "Legacy message transfer is retired; retain original evidence "
        "and use current transaction diagnostics",
        file=sys.stderr,
    )
    raise SystemExit(2)


if __name__ == "__main__":
    main()
