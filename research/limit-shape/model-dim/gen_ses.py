#!/usr/bin/env python3
"""Sinh session id đúng dạng opencode: ses_ + 12 hex + 14 ký tự base62 (26 ký tự sau ses_).

Dạng này đã đo 200 (12/12 lần) với upstream; các dạng khác (sai độ dài, ký tự lạ) bị 429.
"""
import secrets
import sys
import time

ALPHA = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"


def gen() -> str:
    head = f"{int(time.time() * 1000) & 0xFFFFFFFFFFFF:012x}"
    tail = "".join(secrets.choice(ALPHA) for _ in range(14))
    return "ses_" + head + tail


if __name__ == "__main__":
    print(gen())
