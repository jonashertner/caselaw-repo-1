---
to: Registry of the European Court of Human Rights, Strasbourg
contact: publications@echr.coe.int  (primary); contact form at https://www.echr.coe.int/contact
cc: echrregistry@echr.coe.int
from: Jonas Hertner · OpenCaseLaw · team@jonashertner.com
date: 2026-04-24
subject: Request for written permission — bulk reuse of HUDOC authoritative-language judgments in the OpenCaseLaw research corpus
status: draft (awaiting send)
---

Registry of the European Court of Human Rights
Council of Europe
F-67075 Strasbourg Cedex
France

Strasbourg, 24 April 2026

Subject: Request for written permission under the HUDOC copyright and
disclaimer framework — bulk reuse of authoritative-language judgments

Dear Members of the Registry,

I am writing on behalf of OpenCaseLaw (https://opencaselaw.ch), an
independent non-profit research project that operates a unified, freely
accessible search corpus of Swiss and Swiss-adjacent case-law. The
corpus currently includes 968,000+ judgments from all federal and
cantonal Swiss courts, 5,000+ federal statutes, 10,500+ cantonal
legislative texts, and 1,000+ scholarly commentaries from open-access
sources.

Because the European Convention on Human Rights is directly applicable
in Switzerland and the jurisprudence of your Court is routinely cited
by the Swiss Federal Supreme Court (Bundesgericht) as well as by every
cantonal jurisdiction, ECtHR judgments are an indispensable source for
the Swiss legal researchers, practitioners, and academics our service
serves. The corpus already mirrors approximately 810 judgments in which
Switzerland is the respondent state (collected with attribution under
the existing public-reuse notice).

We would now like to extend this coverage to the full corpus of ECtHR
authoritative-language judgments (Grand Chamber, Chamber, and Committee
judgments — approximately 55,000 documents), and we respectfully
request written permission to do so under the framework described in
your "Copyright and disclaimer" page
(https://www.echr.coe.int/copyright-and-disclaimer).

We have carefully read the reuse conditions published by the Court and
wish to explicitly confirm and commit to the following:

1. **Scope.** Only judgments authored by the Court would be mirrored
   (authoritative-language versions in English and French). We will
   **not** redistribute third-party translations, which we understand
   are protected independently by their translators or by national
   authorities.

2. **Attribution.** Every record — in our search interface, our
   Model Context Protocol (MCP) responses, and our dashboard pages —
   will carry the attribution "© ECHR-CEDH" together with a direct
   link back to the canonical HUDOC item. Our OpenAPI and dataset
   metadata will carry the same attribution.

3. **No commingling with CC0 content.** Our existing Hugging Face
   mirror of Swiss case-law is released under CC0 (public-domain
   Swiss court output). ECtHR content would be hosted in a **separate**
   Hugging Face dataset repository with its own licence notice
   reproducing the Court's copyright and reuse conditions verbatim —
   **not** CC0.

4. **Use model.** The base MCP search, API, and dashboard will remain
   **free of charge** to users worldwide. The project carries a small
   paid productivity add-in for Microsoft Word (CHF 5 / month), which
   is why we wish to obtain *written* permission rather than relying on
   the information/education exception alone; we want ECtHR content to
   be accessible in every environment our users reach for, including
   environments touched by the paid add-in. We would of course be
   equally willing to exclude ECtHR content from any paid surface if
   that is the Court's preference.

5. **Takedown respect.** We commit to honouring any correction or
   takedown request from the Court within 24 hours, and to maintaining
   a visible contact address on every page where ECtHR content is
   surfaced.

6. **No translations produced.** We will not generate or publish
   machine translations of ECtHR judgments into other languages. If the
   Court later publishes additional authoritative-language versions, we
   will simply mirror them with the same attribution.

7. **Anti-mirroring of inadmissibility decisions.** We will not include
   inadmissibility decisions (the ~900,000 Committee DECs) in this
   ingest. They are excluded by design.

Our motivation is strictly public-interest: many Swiss-law research
questions cannot be answered without parallel access to ECtHR
jurisprudence, and commercial legal-research tools (Swisslex, Weblaw)
have historically restricted this access behind paywalls that exclude
students, pro-bono practitioners, NGOs, and journalists. A free,
attribution-respecting mirror improves the legal-information commons
in a way that is directly aligned with the Council of Europe's own
priorities on rule-of-law accessibility.

We would be grateful for written confirmation that:

(a) bulk mirroring of authoritative-language ECtHR judgments for
search and research purposes is permitted under the terms described
above; and

(b) our paid add-in — which is not itself a redistribution of ECtHR
content, but which may access it during research workflows — does
not, for the avoidance of doubt, conflict with those terms.

We are happy to sign a short letter of undertaking, adopt any specific
attribution wording the Registry prefers, or restrict the scope
further if that would accelerate a positive response.

If the Registry would prefer that we remove ECtHR content from any
commercial surface (the add-in) while retaining it on the free MCP /
API / dashboard, that is acceptable to us and we will gladly confirm
in writing.

Please find below a short project summary for reference, and do not
hesitate to contact me directly at **team@jonashertner.com** for any
clarification or further documentation. I am based in Zurich and would
be pleased to travel to Strasbourg for an in-person meeting if that
would be helpful.

Thank you for the work the Court does, and for considering this
request.

Yours sincerely,

Jonas Hertner
Founder and Maintainer, OpenCaseLaw
team@jonashertner.com · https://jonashertner.com
https://opencaselaw.ch · https://mcp.opencaselaw.ch

---

### Project summary (for reference)

- **Organisation.** OpenCaseLaw — independent, non-profit, Swiss
  individual, no external funding, no commercial sponsor.
- **Corpus.** 968,000+ court decisions from every Swiss federal and
  cantonal court; 5,000+ federal statutes; 10,500+ cantonal laws;
  1,000+ scholarly commentaries (CC BY / CC BY-SA). Updated daily.
- **Interfaces.**
  - `https://opencaselaw.ch` — public search dashboard (DE/FR/IT/EN/RM).
  - `https://mcp.opencaselaw.ch` — Model Context Protocol server,
    24 tools, free and rate-limited. Consumed by Claude, ChatGPT, and
    third-party legal-tech plugins.
  - `https://word.opencaselaw.ch` — Microsoft Word add-in
    (citation-verification; CHF 5 / month; optional).
  - `voilaj/swiss-caselaw` on Hugging Face — Swiss case-law dataset,
    CC0 (official Swiss court output is not copyrightable under
    art. 5 URG).
- **Funding.** None. Operational costs are paid personally by the
  maintainer (approx. EUR 140/month for VPS + DNS).
- **Governance.** A published removal / correction policy is in place
  (`docs/governance-and-removal-policy.md`).

### Attached draft attribution wording

> Source: © ECHR-CEDH — European Court of Human Rights.
> Reproduced under the Court's copyright and disclaimer:
> https://www.echr.coe.int/copyright-and-disclaimer
> Canonical HUDOC link: https://hudoc.echr.coe.int/eng?i={itemid}

This string would appear on every rendered ECtHR record (dashboard,
MCP responses, add-in output, Hugging Face dataset card, OpenAPI
metadata).
