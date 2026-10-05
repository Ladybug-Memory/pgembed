"""Pytest configuration for pgembed tests.

Most tests need a running PostgreSQL server backed by the bundled
binaries (built via `make`). Where those binaries have not been built --
e.g. a lightweight PR check or a fresh contributor checkout -- the
server-dependent tests are skipped instead of failing.

Tests that do NOT need a server opt out explicitly with::

    @pytest.mark.no_server_required
"""

import importlib.util
from pathlib import Path

import pytest


def _postgres_binaries_available() -> bool:
    spec = importlib.util.find_spec("pgembed")
    if spec is None or not spec.submodule_search_locations:
        return False
    bin_dir = Path(spec.submodule_search_locations[0]) / "pginstall" / "bin"
    return (bin_dir / "postgres").exists() or (bin_dir / "postgres.exe").exists()


POSTGRES_BINARIES_AVAILABLE = _postgres_binaries_available()

requires_server = pytest.mark.skipif(
    not POSTGRES_BINARIES_AVAILABLE,
    reason="PostgreSQL binaries not built; run `make` to build them",
)


def pytest_collection_modifyitems(items):
    if POSTGRES_BINARIES_AVAILABLE:
        return
    for item in items:
        if "no_server_required" not in item.keywords:
            item.add_marker(requires_server)
