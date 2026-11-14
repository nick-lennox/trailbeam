"""Optional test dependencies that must not be skipped silently where they are meant to run (QV-32 §6).

The end-to-end tests need the `qv2` package from ../python (and moto), which the store does not depend on.
Locally they are skipped, with the reason and the command to install them, if those are missing. In CI
(the ``CI`` variable GitHub Actions sets) or with ``QV2_REQUIRE_E2E=1``, a missing package is an import
error instead, which fails the run: CI installs them (`pip install -e './server[test]' -e './python[test]'`),
so a missing one there means the setup broke, not that the tests don't apply.
"""
import importlib
import os

import pytest

INSTALL = "pip install -e './python[test]' (from the repository root)"


def required() -> bool:
    return bool(os.environ.get("CI") or os.environ.get("QV2_REQUIRE_E2E"))


def need(module: str):
    """Import ``module``; if it is missing, skip the calling test module locally, fail in CI."""
    if required():
        try:
            return importlib.import_module(module)
        except ImportError as e:
            raise ImportError(f"{module} is required here (CI or QV2_REQUIRE_E2E is set) but missing: {e}. "
                              f"Install it with {INSTALL}.") from e
    return pytest.importorskip(module, reason=f"{module} is not installed; {INSTALL} to run these tests "
                                              "(they fail instead of skipping in CI)")
