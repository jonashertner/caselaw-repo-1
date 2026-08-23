"""Shared fixtures for tests/.

Kept deliberately minimal — most fixtures live in the test files that own
them (repo convention).
"""
import pytest


@pytest.fixture(autouse=True)
def _clear_search_result_cache():
    """Isolate mcp_server._SEARCH_RESULT_CACHE between tests.

    In-memory fixture DBs all report user_version=0, so the db_generation
    hook that invalidates the cache in production never fires across tests:
    two tests calling search_fts5 with identical args but different fixture
    corpora would silently share results. Same collision class the
    generation-keyed _FTS_TOTAL_CACHE handles per-file in
    test_search_total_exact.py; the result cache is cleared globally here
    because any search test can hit it.
    """
    try:
        import mcp_server
        cache = mcp_server._SEARCH_RESULT_CACHE
    except Exception:
        yield
        return
    cache.clear()
    yield
    cache.clear()
