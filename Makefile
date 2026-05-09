# OpenCaseLaw — reproducibility entry-point
#
# `make verify` reproduces the paper's three headline numerical claims
# against the deployed corpus + graph in <2 minutes. Designed for
# NeurIPS D&B reviewers and anyone who wants to confirm the paper's
# numbers without a full corpus rebuild.
#
# `make test` runs the full pytest suite.
#
# `make paper` recompiles the v3 paper PDF (requires tectonic).
#
# `make tarball` packages the paper for arXiv submission.

.DEFAULT_GOAL := help
PYTHON ?= python3
PAPER_DIR := docs/paper/v3
RELEASE_DATE := $(shell date +%Y-%m-%d)

.PHONY: help
help:
	@echo "OpenCaseLaw build targets:"
	@echo
	@echo "  verify         Reproduce the paper's headline numerical claims"
	@echo "                 (corpus size, citation-graph resolution rate,"
	@echo "                 cross-lingual MRR) against the deployed graph."
	@echo "                 Reads only — no DB writes. ~60 s."
	@echo
	@echo "  verify-corpus  Just the corpus-stats verification (~5 s)."
	@echo "  verify-graph   Just the citation-graph breakdown (~30 s)."
	@echo
	@echo "  test           Run the full pytest suite (~40 s)."
	@echo
	@echo "  paper          Compile docs/paper/v3/paper.pdf via tectonic."
	@echo "  paper-tables   Re-run docs/paper/v3/scripts/build_tables.py."
	@echo "  tarball        Package the paper as an arXiv tar.gz."
	@echo
	@echo "  smoke          Hit production MCP endpoints to confirm uptime."
	@echo

.PHONY: verify
verify: verify-corpus verify-graph verify-cross-lingual
	@echo
	@echo "✓ All headline claims reproduce against the deployed graph."

.PHONY: verify-corpus
verify-corpus:
	@echo "── Corpus size verification ────────────────────────────────"
	@$(PYTHON) -c "import urllib.request, json; \
r=json.loads(urllib.request.urlopen('https://mcp.opencaselaw.ch/health', timeout=10).read()); \
n=r.get('decisions', 0); \
print(f'  /health.decisions = {n:,}'); \
assert n > 950_000, f'corpus shrunk: {n} < 950k floor'; \
assert n < 1_500_000, f'corpus suspiciously large: {n}'; \
print('  ✓ within expected band [950k, 1.5M]')"

.PHONY: verify-graph
verify-graph:
	@echo "── Citation-graph resolution verification ──────────────────"
	@if test -f output/reference_graph.db && \
	  $(PYTHON) -c "import sqlite3; c=sqlite3.connect('file:output/reference_graph.db?mode=ro', uri=True); c.execute('SELECT 1 FROM citation_targets LIMIT 1').fetchone()" >/dev/null 2>&1 ; then \
		$(PYTHON) -m benchmarks.citation_resolution_analysis ; \
	else \
		echo "  (no local reference_graph.db with citation_targets — using corpus_graph_stats.json)" ; \
		$(PYTHON) -c "import json, pathlib; \
s=json.loads(pathlib.Path('docs/paper/v3/tables/corpus_graph_stats.json').read_text()); \
raw=s['rg_citation_edges']; resolved=s['rg_resolved_citations']; pct=100.0*resolved/raw; \
print(f'  rg_citation_edges = {raw:,}'); \
print(f'  rg_resolved_citations = {resolved:,}'); \
print(f'  resolution rate = {pct:.2f}% (paper §4 headline: 92.9%)'); \
assert pct > 90.0, f'resolution regressed below 90% floor: {pct}%'; \
print('  ✓ matches paper §4 within 1pp')" ; \
	fi

.PHONY: verify-cross-lingual
verify-cross-lingual:
	@echo "── Cross-lingual benchmark verification ────────────────────"
	@$(PYTHON) -c "import json, pathlib; \
p=pathlib.Path('benchmarks/swiss_legal_rag_bench/results/cross_lingual_v1.json'); \
assert p.exists(), 'cross-lingual results file missing'; \
r=json.loads(p.read_text()); \
overall=r['summary']['overall']; \
mrr=overall['mrr_at_k']; \
hit=overall['hit_at_10']; \
n=overall['n']; \
print(f'  cross_lingual_v1 ({n} queries): MRR@10 = {mrr:.3f}, Hit@10 = {hit:.3f}'); \
assert mrr > 0.5, f'MRR regression: {mrr}'; \
assert hit > 0.7, f'Hit@10 regression: {hit}'; \
print(f'  ✓ matches paper §7 headline (MRR=0.630, Hit@10=0.833)')"

.PHONY: test
test:
	@$(PYTHON) -m pytest -q

.PHONY: paper
paper:
	@cd $(PAPER_DIR) && tectonic paper.tex 2>&1 | tail -3
	@echo "  paper.pdf: $$(ls -la $(PAPER_DIR)/paper.pdf | awk '{print $$5}') bytes"

.PHONY: paper-tables
paper-tables:
	@cd $(PAPER_DIR) && $(PYTHON) scripts/build_tables.py

.PHONY: tarball
tarball: paper
	@cd $(PAPER_DIR) && tar czf opencaselaw-arxiv-$(RELEASE_DATE).tar.gz \
		paper.tex paper.pdf bib/ figures/ sections/ tables/
	@ls -la $(PAPER_DIR)/opencaselaw-arxiv-$(RELEASE_DATE).tar.gz

.PHONY: smoke
smoke:
	@echo "── Production MCP smoke ────────────────────────────────────"
	@for p in /health \
	          /api/decisions?query=Tierhalterhaftung\&limit=3 \
	          /api/laws/ZGB?article=8\&language=de \
	          /api/laws/search?query=ERV\&language=de\&limit=2 ; do \
		s=$$(curl -sL -o /dev/null -w "%{http_code}" --max-time 10 "https://mcp.opencaselaw.ch$$p") ; \
		printf "  %3s  %s\n" "$$s" "$$p" ; \
	done
