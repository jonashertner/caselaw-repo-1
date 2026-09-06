"""_e_number_sort_key must never raise and must keep document order.

Regression for the 2026-09-06 incident: the decision_structure sidecar
began carrying lettered Erwägung numbers ('2a', '2aa', '3c/aa') and the
old key produced int-or-str tuples, so sorting a decision that mixed
'2' and '2a' raised ``TypeError: '<' not supported between instances of
'str' and 'int'`` in get_erwaegung, get_decision_structure,
get_case_brief and every /entscheid page of such a decision.

Shapes observed live on 2026-09-06 (decision_structure.db):
  bge_BGE_125_III_70              2, 3, 2a, 2b, 2c, 3a, 3b, 3c
  ag_anwaltskommission_AGVE_2000_18  2, 2aa, 2bb, 2cc, 2dd
  others                           3.3.3aa, 5.2.2a, 05.00a, 3c/aa
"""

import random

import pytest

import seo_pages
from mcp_server import _e_number_sort_key as server_key

page_key = seo_pages._e_number_sort_key

LIVE_SHAPES = [
    "2", "3", "2a", "2b", "2c", "3a", "3b", "3c",
    "2aa", "2bb", "2cc", "2dd",
    "3.3.3aa", "3.3.3bb", "05.00a", "05.00b", "5.2.2a", "5.2.2b",
    "3c/aa", "3c/bb", "1", "1.1", "1.10", "1.9", "10", "II", "1bis", "",
]


@pytest.mark.parametrize("key", [server_key, page_key], ids=["mcp_server", "seo_pages"])
def test_mixed_numeric_and_lettered_never_raises(key):
    items = list(LIVE_SHAPES)
    for seed in range(20):
        random.Random(seed).shuffle(items)
        sorted(items, key=key)  # must not raise


@pytest.mark.parametrize("key", [server_key, page_key], ids=["mcp_server", "seo_pages"])
def test_numeric_levels_sort_numerically(key):
    assert sorted(["1.10", "1.9", "1.2", "2", "10", "1"], key=key) == [
        "1", "1.2", "1.9", "1.10", "2", "10",
    ]


@pytest.mark.parametrize("key", [server_key, page_key], ids=["mcp_server", "seo_pages"])
def test_lettered_erwaegungen_follow_document_order(key):
    # BGE old style: E. 2 -> 2a, 2b, 2c; double letters under a letter are
    # written '3c/aa' by the extractor and belong right after '3c'.
    assert sorted(["3d", "3c/bb", "3c", "3c/aa", "3", "2b", "2a", "2"], key=key) == [
        "2", "2a", "2b", "3", "3c", "3c/aa", "3c/bb", "3d",
    ]
    # A double-letter run directly under a number: aa < ab < bb.
    assert sorted(["2bb", "2ab", "2aa", "2"], key=key) == ["2", "2aa", "2ab", "2bb"]
    # Dotted numeric prefix with a lettered leaf.
    assert sorted(["3.3.3bb", "3.3.3aa", "3.3.3", "3.3.4"], key=key) == [
        "3.3.3", "3.3.3aa", "3.3.3bb", "3.3.4",
    ]


def test_server_and_page_keys_agree():
    items = list(LIVE_SHAPES)
    random.Random(7).shuffle(items)
    assert sorted(items, key=server_key) == sorted(items, key=page_key)
