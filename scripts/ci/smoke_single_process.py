"""Retired NSQ image profile; final transaction images are verified by infra C08."""

import sys

print("Retired NSQ image smoke; use the fixed RocketMQ transaction image gate", file=sys.stderr)
raise SystemExit(2)
