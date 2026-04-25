# Wikipedia Draft: OpenCaseLaw

*Note: Wikipedia conflict-of-interest policy discourages project creators from submitting their own articles. This draft should be reviewed and submitted by an independent party. See [[WP:COI]] and [[WP:NPOV]].*

*Before submission, gather 3-5 independent reliable sources (news coverage, academic citations, law review mentions). The IusBubble mention and arXiv paper help but may not be sufficient alone.*

---

**OpenCaseLaw** is an open-access database of published Swiss court decisions operated by Jonas Hertner. As of March 2026, it contains over 962,000 decisions from 102 federal, cantonal, and quasi-judicial sources, covering all 26 Swiss cantons and the period from 1875 to the present.<ref name="arxiv">Hertner, J. (2026). "OpenCaseLaw: An Open Dataset and Search Platform for Swiss Court Decisions". ''arXiv''.</ref> The database is updated daily and is freely accessible without registration or subscription.<ref name="iusbubble">{{cite web |url=https://www.iusbubble.com/c/public/opencaselaw-ch-datensatz-der-schweizer-gerichtsentscheide |title=opencaselaw.ch / Datensatz der Schweizer Gerichtsentscheide |website=IusBubble}}</ref>

## Background

In Switzerland, court decisions are published by individual courts on separate portals maintained by the federal government and 26 cantons. Unlike in the United States, where the [[Caselaw Access Project]] by [[Harvard Law School]] provides centralised access to over 6.7 million published court decisions,<ref>{{cite web |url=https://case.law/ |title=Caselaw Access Project |publisher=Harvard Law School Library Innovation Lab}}</ref> no comparable open infrastructure previously existed for Swiss case law. Commercial services such as Swisslex (operated by [[Stämpfli Verlag]]) and Weblaw provide aggregated access but require paid subscriptions.

OpenCaseLaw was created to address this gap by scraping decisions from official court websites, deduplicating them, and making the full corpus available for research and legal practice.

## Corpus

The database covers decisions from all levels of the Swiss court system:

* **Federal courts**: the [[Federal Supreme Court of Switzerland|Federal Supreme Court]] (Bundesgericht), the [[Federal Administrative Court (Switzerland)|Federal Administrative Court]], the [[Federal Criminal Court (Switzerland)|Federal Criminal Court]], and the [[Federal Patent Court (Switzerland)|Federal Patent Court]].
* **Cantonal courts**: all 26 cantons, including the courts of [[Geneva]], [[Vaud]], [[Zürich]], and [[Ticino]] as the largest contributors.
* **Quasi-judicial bodies**: the [[Swiss Financial Market Supervisory Authority|FINMA]], the [[Swiss Competition Commission|WEKO]], and the [[Federal Data Protection and Information Commissioner|EDÖB]], among others.

Decisions are available in [[German language|German]] (46.6%), [[French language|French]] (45.1%), and [[Italian language|Italian]] (8.3%), reflecting the [[Languages of Switzerland|multilingual character of Swiss law]].<ref name="arxiv"/>

## Citation graph

OpenCaseLaw extracts cross-references between decisions from the full text of the corpus using pattern matching. As of March 2026, the reference database contains 8.76 million case-citation references (of which 6.42 million are resolved to specific decisions in the corpus, a 73.3% resolution rate) and 11.23 million links between decisions and federal statute provisions.<ref name="arxiv"/> The citation graph supports the identification of leading cases (''Leitentscheide'') by ranking decisions according to how frequently they are cited by other decisions.

## Access

The corpus is available in several forms:

* As [[Apache Parquet|Parquet]] files on [[Hugging Face]] for bulk download and analysis.<ref>{{cite web |url=https://huggingface.co/datasets/voilaj/swiss-caselaw |title=Swiss Case Law Dataset |website=Hugging Face}}</ref>
* Through a [[Model Context Protocol]] (MCP) server, which enables natural-language search from AI assistants including [[Claude (language model)|Claude]], [[ChatGPT]], and [[Gemini (language model)|Gemini]].<ref name="iusbubble"/>
* Through a [[REST]] API with [[OpenAPI Specification|OpenAPI]] documentation.
* As individual decision pages with [[Schema.org]] [[LegalCase]] structured data for search engine indexing.

## Legal basis

Published Swiss court decisions are excluded from [[copyright]] protection under Article 5, paragraph 1, letter c of the Swiss Federal Act on Copyright and Related Rights ([[URG]]), which exempts official works including judicial decisions.<ref>{{cite web |url=https://www.fedlex.admin.ch/eli/cc/1993/1798_1798_1798/en |title=Federal Act on Copyright and Related Rights (CopA) |website=Fedlex}}</ref> The duty to publish Federal Supreme Court decisions is established by Article 27 of the Federal Supreme Court Act ([[BGG]]).

## Relationship to other projects

OpenCaseLaw differs from existing Swiss legal NLP datasets in its scope. Swiss-Judgment-Prediction (Niklaus et al., 2021) covers 85,000 Federal Supreme Court decisions for outcome prediction tasks.<ref>Niklaus, J., Chalkidis, I., and Stürmer, M. (2021). "Swiss-Judgment-Prediction: A Multilingual Legal Judgment Prediction Benchmark". ''NLP4PositiveImpact Workshop, EMNLP 2021''.</ref> The SCALE benchmark (Rasiah et al., 2023) evaluates citation extraction and summarization on Swiss legal text.<ref>Rasiah, V. et al. (2023). "SCALE: Scaling up the Complexity for Advanced Language Model Evaluation". ''arXiv:2306.09237''.</ref> OpenCaseLaw covers 102 sources across all cantons and court levels, but does not provide task-specific annotations.

The project is conceptually comparable to the [[Caselaw Access Project]] in the United States, [[Open Legal Data]] in Germany, and [[Find Case Law]] in the United Kingdom, though each covers a different jurisdiction and provides different access mechanisms.

## See also

* [[Law of Switzerland]]
* [[Federal Supreme Court of Switzerland]]
* [[Caselaw Access Project]]
* [[Legal informatics]]
* [[Open data in Switzerland]]

## References

{{reflist}}

## External links

* {{official website|https://opencaselaw.ch}}
* [https://huggingface.co/datasets/voilaj/swiss-caselaw Dataset on Hugging Face]
* [https://github.com/jonashertner/caselaw-repo-1 Source code on GitHub]
* [https://mcp.opencaselaw.ch/api/docs REST API documentation]
