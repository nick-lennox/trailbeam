"""The lookup filter (lookup_filter.py, QV-127): no false negatives, the false-positive rate it is sized for,
and the same bits in every process (they are stored in Postgres and read by every replica)."""
import pytest

from trailbeam.server.lookup_filter import BITS_PER_KEY, LookupFilter, hash_key, mid_key


def test_every_key_added_is_found_and_others_rarely():
    f = LookupFilter.sized(20_000)
    keys = [mid_key(f"m-{i}") for i in range(10_000)] + [hash_key(f"h-{i}", "k1") for i in range(10_000)]
    for k in keys:
        f.add(k)
    assert f.keys == 20_000 and not f.full() and f.full(1)
    assert all(k in f for k in keys)
    others = sum(mid_key(f"other-{i}") in f for i in range(200_000))
    assert others <= 200_000 * 0.6185 ** BITS_PER_KEY * 4  # about 14 expected at 20 bits a key


def test_mid_and_hash_keys_do_not_collide():
    assert mid_key("ab") != hash_key("ab", "") and hash_key("a", "bc") != hash_key("ab", "c")


def test_the_bits_are_the_same_in_every_process():
    """Fixed vector: a change here would make replicas disagree with the filters already stored."""
    f = LookupFilter(1024, 3, capacity=10)
    f.add(mid_key("msg-1"))
    assert [i for i in range(1024) if f.bits[i >> 3] >> (i & 7) & 1] == _VECTOR


_VECTOR = [636, 659, 682]


def test_a_stored_filter_reads_back():
    f = LookupFilter.sized(100)
    f.add(hash_key("h", "k"))
    g = LookupFilter(f.m, f.k, capacity=f.capacity, keys=f.keys, version=3, bits=bytes(f.bits))
    assert hash_key("h", "k") in g and g.version == 3
    with pytest.raises(ValueError):
        LookupFilter(f.m, f.k, capacity=1, bits=b"\x00")
