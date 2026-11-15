"""Checks docs/span-contract-vectors.json with the standard library (contract section 7)."""
import hashlib
import hmac
import json
from pathlib import Path

VECTORS = json.loads(
    (Path(__file__).resolve().parents[2] / "docs" / "span-contract-vectors.json").read_text("utf-8")
)


def _hmac_hex(key: str, data: str) -> str:
    return hmac.new(key.encode("utf-8"), data.encode("utf-8"), hashlib.sha256).hexdigest()


def test_vectors_match_hmac_sha256():
    assert VECTORS["vectors"]
    for v in VECTORS["vectors"]:
        assert _hmac_hex(v["key"], v["receipt_handle"])[:32] == v["hmac"], v["name"]
        assert _hmac_hex(v["key"], "trailbeam key id")[:8] == v["key_id"], v["name"]


def test_default_key_id_is_the_one_in_the_contract():
    contract = (Path(__file__).resolve().parents[2] / "docs" / "span-contract.md").read_text("utf-8")
    key_id = _hmac_hex(VECTORS["default_key"], "trailbeam key id")[:8]
    assert f"`{VECTORS['default_key']}`" in contract
    assert f"`{key_id}`" in contract
