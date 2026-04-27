# OpenCaseLaw Word Plugin

Search 969,000+ Swiss court decisions and insert citations directly in Microsoft Word.

## Installation (Sideload)

### Word for Windows / Mac

1. Download [manifest.xml](https://word.opencaselaw.ch/manifest.xml)
2. Open Word → **File** → **Get Add-ins** → **Upload My Add-in**
3. Select the downloaded `manifest.xml`
4. The OpenCaseLaw sidebar appears on the right

### Word Online

1. Open a document at office.com
2. Click **Insert** → **Add-ins** → **Upload My Add-in**
3. Enter URL: `https://word.opencaselaw.ch/manifest.xml`

## Features

**Free:**
- Search court decisions across all Swiss courts
- Insert formatted citations at cursor: `(BGE 125 III 231, E. 3)`
- Language-aware formatting (DE/FR/IT/EN)
- Look up federal statute articles
- View decision details with individual Erwägungen

**Pro (license-based, server-side):**
- Select a paragraph → click "Referenz prüfen"
- AI verifies whether the cited decision supports the claim
- Color-coded verdict (green/yellow/red) with the most relevant Erwägung
- Inserts a Word comment with the verification result

The Pro verification path is **fully server-side**: the add-in posts the
selected text to `POST /api/billing/verify` together with the user's
license key, and the OpenCaseLaw backend performs the LLM call. Users
never need to provide their own LLM API key in the browser.

The earlier "Tier B" flow that called `https://api.anthropic.com/v1/messages`
directly from the add-in with a user-supplied Anthropic key was removed
on 2026-04-25 (see `tests/web/test_word_addin_no_browser_anthropic.py`
for the regression guard).

## Citation Formats

| Language | Example |
|----------|---------|
| DE | (BGer 4A_747/2012 vom 5. April 2013, E. 2) |
| FR | (TF 4A_747/2012 du 5 avril 2013, consid. 2) |
| IT | (TF 4A_747/2012 del 5 aprile 2013, consid. 2) |
| EN | (BGer 4A_747/2012 of 5 April 2013, para. 2) |

## License

Code: MIT · Data: CC0 1.0 · Source: [OpenCaseLaw.ch](https://opencaselaw.ch)
