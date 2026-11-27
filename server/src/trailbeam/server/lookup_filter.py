"""The Bloom filter of the sealed places' lookup keys (PostgresStore, postgres.py "Lookups"; QV-127).

A Bloom filter answers "may this key be in the set?": never "no" for a key that is in it, and "yes" for one that
isn't with probability about ``0.6185 ** BITS_PER_KEY`` while it holds no more than its capacity (20 bits a key:
about 0.007 %). Here the set is the message IDs and receipt hashes of every sealed place, so a lookup reads a
sealed place's partitions only for the keys the filter says yes to. It takes ``BITS_PER_KEY / 8`` bytes a key of
memory in each replica, against the 50 to 90 bytes a key of a B-tree index that would have to stay cached.

Positions are ``h1 + i * h2 (mod m)`` for ``i < k``, from a 128-bit BLAKE2b of the key (Kirsch and Mitzenmacher,
"Less hashing, same performance", 2006): the same in every process and every Python, so the bits stored in
Postgres mean the same to every replica.
"""
from __future__ import annotations

import hashlib
import math

BITS_PER_KEY = 20
MIN_CAPACITY = 1 << 14  # keys: a filter is never smaller than 40 KB


def mid_key(mid: str) -> bytes:
    return b"m" + mid.encode("utf-8", "surrogatepass")


def hash_key(receipt_hmac: str, key_id: str) -> bytes:
    return b"h" + receipt_hmac.encode("utf-8", "surrogatepass") + b"\x00" + key_id.encode("utf-8", "surrogatepass")


class LookupFilter:
    """``m`` bits, ``k`` of them set per key. ``version``: the filter's version in Postgres (0: not stored);
    ``capacity``: the keys it was sized for; ``keys``: the keys added (a key added twice counts twice)."""

    def __init__(self, m: int, k: int, *, capacity: int, keys: int = 0, version: int = 0, bits=None):
        if m < 8 or k < 1:
            raise ValueError("a filter needs at least 8 bits and 1 hash")
        self.m, self.k, self.capacity, self.keys, self.version = int(m), int(k), int(capacity), int(keys), version
        self.bits = bytearray((self.m + 7) // 8) if bits is None else bytearray(bits)
        if len(self.bits) != (self.m + 7) // 8:
            raise ValueError(f"{len(self.bits)} bytes for {self.m} bits")

    @classmethod
    def sized(cls, capacity: int) -> "LookupFilter":
        """An empty filter for ``capacity`` keys at BITS_PER_KEY bits each, with the best k for that."""
        capacity = max(int(capacity), MIN_CAPACITY)
        return cls(capacity * BITS_PER_KEY, max(1, round(BITS_PER_KEY * math.log(2))), capacity=capacity)

    @staticmethod
    def _hashes(key: bytes) -> tuple[int, int]:
        d = hashlib.blake2b(key, digest_size=16).digest()
        return int.from_bytes(d[:8], "little"), int.from_bytes(d[8:], "little") | 1

    def add(self, key: bytes) -> None:
        h1, h2, m, bits = *self._hashes(key), self.m, self.bits
        for i in range(self.k):
            p = (h1 + i * h2) % m
            bits[p >> 3] |= 1 << (p & 7)
        self.keys += 1

    def __contains__(self, key: bytes) -> bool:
        h1, h2, m, bits = *self._hashes(key), self.m, self.bits
        for i in range(self.k):
            p = (h1 + i * h2) % m
            if not bits[p >> 3] >> (p & 7) & 1:
                return False
        return True

    def full(self, more: int = 0) -> bool:
        """Whether ``more`` keys would take it past its capacity (its false-positive rate then climbs)."""
        return self.keys + more > self.capacity
