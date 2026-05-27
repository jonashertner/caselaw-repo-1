"""Swiss open-access legal scholarship scrapers.

Harvests OA legal publications (articles, dissertations, books, working
papers) from Swiss institutional repositories + journals. The unified
metadata schema lives in `search_stack.build_legal_scholarship`.

Sources:
  - sui-generis.ch (OJS, OAI-PMH, CC-BY-SA)
  - LeGes (Bundeskanzlei, federal legislation journal)
  - Justice-Justiz-Giustizia (judges' association)
  - University IRs filtered to law (ZORA, BORIS, SERVAL, UNIGE, edoc.unibas,
    Alexandria SG, FOLIA, LIBRA, …) via OAI-PMH set filters
  - e-periodica.ch historical legal periodicals (ZBJV, ZSR, RDS, …)
"""
