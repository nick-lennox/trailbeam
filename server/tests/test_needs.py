"""The end-to-end tests' optional packages: skipped locally if missing, a failure in CI (QV-32 §6)."""
import pytest

import needs

MISSING = "trailbeam_no_such_module_for_this_test"


def test_missing_package_skips_locally(monkeypatch):
    monkeypatch.delenv("CI", raising=False)
    monkeypatch.delenv("TRAILBEAM_REQUIRE_E2E", raising=False)
    with pytest.raises(pytest.skip.Exception, match="fail instead of skipping in CI"):
        needs.need(MISSING)


@pytest.mark.parametrize("var", ["CI", "TRAILBEAM_REQUIRE_E2E"])
def test_missing_package_fails_in_ci(monkeypatch, var):
    monkeypatch.delenv("CI", raising=False)
    monkeypatch.setenv(var, "true")
    with pytest.raises(ImportError, match="is required here"):
        needs.need(MISSING)


def test_present_package_is_imported(monkeypatch):
    monkeypatch.setenv("CI", "true")
    assert needs.need("json").dumps([]) == "[]"
