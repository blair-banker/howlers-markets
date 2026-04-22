"""Root conftest.py — adds markets-core tests dir to sys.path so that
`from markets_core.tests.contract.test_classifier_contract import ClassifierContract`
resolves correctly in pipeline contract tests.
"""
import sys
from pathlib import Path

# Make packages/markets-core/tests importable as markets_core.tests.*
_core_tests = Path(__file__).parent / "packages" / "markets-core" / "tests"
_core_tests_parent = _core_tests.parent  # packages/markets-core

# We need 'markets_core/tests' to be importable. Since 'markets_core' is already
# a package rooted at packages/markets-core/src/markets_core, the cleanest approach
# is to insert the packages/markets-core directory into sys.path so that
# 'tests' is accessible as a top-level module, then re-export it under
# markets_core.tests via a namespace package shim — but the simplest path
# that matches the import statement is to add a shim.
#
# Simpler: insert packages/markets-core into sys.path so that 'tests' is
# importable, and then monkeypatch markets_core.tests to point there.
import importlib
import types

if str(_core_tests_parent) not in sys.path:
    sys.path.insert(0, str(_core_tests_parent))

# Ensure markets_core.tests resolves to packages/markets-core/tests
import markets_core as _mc
if not hasattr(_mc, "tests"):
    # Load the tests package from its actual location
    _tests_pkg = importlib.util.spec_from_file_location(
        "markets_core.tests",
        str(_core_tests / "__init__.py"),
        submodule_search_locations=[str(_core_tests)],
    )
    if _tests_pkg is not None:
        _mod = importlib.util.module_from_spec(_tests_pkg)
        sys.modules["markets_core.tests"] = _mod
        _tests_pkg.loader.exec_module(_mod)  # type: ignore[union-attr]
        _mc.tests = _mod  # type: ignore[attr-defined]
