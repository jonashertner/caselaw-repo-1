# AppSource Listing — OpenCaseLaw

## App name
OpenCaseLaw - Swiss Case Law

## Short description (100 chars max)
Search 963,000+ Swiss court decisions and insert correct citations directly in Word.

## Long description (4000 chars max)

OpenCaseLaw gives lawyers, legal scholars, and students instant access to the complete Swiss case law database — directly inside Microsoft Word.

**963,000+ court decisions. All courts. One search.**

Search the full text of decisions from the Federal Supreme Court (BGer/TF), Federal Administrative Court (BVGer/TAF), Federal Criminal Court (BStGer/TPF), Federal Patent Court (BPatGer/TFB), FINMA, WEKO/COMCO, and all 26 cantonal courts. The database is updated daily and covers decisions from 1875 to today.

**Insert correctly formatted citations in one click**

Find a decision, click "Insert" — the correctly formatted citation appears at your cursor position. Citations follow Swiss legal citation standards and adapt to your chosen language:
- German: (BGE 133 III 121, E. 3)
- French: (ATF 133 III 121, consid. 3)
- Italian: (DTF 133 III 121, consid. 3)

No more manual formatting. No more typos in docket numbers.

**Look up federal statutes inline**

Search for "Art. 41 OR" or "Art. 8 BV" — the full article text appears directly in the panel alongside relevant court decisions. Covers 80+ Swiss federal laws including OR, ZGB, StGB, BV, StPO, ZPO, SchKG, and more.

**Find related decisions**

Select a citation in your document and click "Find related" to discover decisions on the same topic. Built on a citation graph with 8.7 million cross-references between decisions.

**AI-powered reference verification (Pro)**

Select a passage in your document that cites a court decision, then click "Verify reference". AI reads the cited decision and checks whether it actually supports your claim. Results show a clear verdict (supported / partially supported / not supported) with the relevant consideration quoted.

Pro subscription: CHF 5/month, 100 verifications per day.

**Multilingual**

Full interface in German, French, Italian, and English. Court names, button labels, error messages — everything adapts to your language choice. Citations are automatically formatted in the selected language.

**Free and open**

Search and citation insertion are completely free, with no account required. The court decision data is CC0 1.0 (public domain). The add-in code is MIT licensed. Built by opencaselaw.ch.

## Categories
- Productivity
- Legal
- Reference

## Keywords
Swiss law, case law, court decisions, citations, legal research, BGE, ATF, DTF, Bundesgericht, Tribunal federal, Rechtsprechung, jurisprudence, Schweizer Recht, droit suisse

## Supported languages
German, French, Italian, English

## Support URL
https://opencaselaw.ch

## Privacy policy URL
https://word.opencaselaw.ch/privacy.html

## Terms of use URL
https://word.opencaselaw.ch/terms.html

## Test notes for Microsoft reviewers

To test the add-in:

1. Open the add-in panel — it should show the welcome screen with "963,000+ Entscheide" and feature cards
2. Type "Mietrecht Kündigung" in the search bar and press Enter — results should appear within 2 seconds
3. Click "Volltext" on any result — the detail view should show the regeste, considerations, and full text
4. Click "Insert" — in standalone browser mode this logs to console; in Word it inserts at cursor
5. Search for "Art. 41 OR" — a blue law article card should appear above the decision results showing the article text
6. Change language to FR — all labels, placeholders, and court names should switch to French
7. Click the gear icon — settings page shows the citation format preview and Pro upgrade section
8. The "Verify reference" feature requires a Pro subscription (CHF 5/month) or can be tested with license key: contact info@opencaselaw.ch for a test key

The API server is at mcp.opencaselaw.ch. No authentication is required for search and citation features.
