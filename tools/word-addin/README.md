# OpenCaseLaw Word Plugin

Search 963,000+ Swiss court decisions and insert citations directly in Microsoft Word.

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

**Free (Tier A):**
- Search court decisions across all Swiss courts
- Insert formatted citations at cursor: `(BGE 125 III 231, E. 3)`
- Language-aware formatting (DE/FR/IT/EN)
- Look up federal statute articles
- View decision details with individual Erwägungen

**With API Key (Tier B):**
- Select a paragraph → click "Referenz prüfen"
- AI verifies if the cited decision supports the claim
- Color-coded verdict (green/yellow/red) with relevant Erwägung
- Inserts Word comment with the verification result

## Citation Formats

| Language | Example |
|----------|---------|
| DE | (BGer 4A_747/2012 vom 5. April 2013, E. 2) |
| FR | (TF 4A_747/2012 du 5 avril 2013, consid. 2) |
| IT | (TF 4A_747/2012 del 5 aprile 2013, consid. 2) |
| EN | (BGer 4A_747/2012 of 5 April 2013, para. 2) |

## License

Code: MIT · Data: CC0 1.0 · Source: [OpenCaseLaw.ch](https://opencaselaw.ch)
