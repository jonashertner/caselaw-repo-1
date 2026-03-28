/**
 * Tier B: Reference verification using user's Anthropic API key.
 * Extracts citations from text, fetches decisions, checks with Claude.
 */

var CITATION_PATTERNS = [
  /(BGE|ATF|DTF)\s+(\d+)\s+([IVX]+)\s+(\d+)/g,
  /(\d[A-Z]_\d+\/\d{4})/g,
];

function extractCitations(text) {
  var refs = [];
  for (var i = 0; i < CITATION_PATTERNS.length; i++) {
    var regex = new RegExp(CITATION_PATTERNS[i].source, CITATION_PATTERNS[i].flags);
    var match;
    while ((match = regex.exec(text)) !== null) {
      refs.push(match[0]);
    }
  }
  // Deduplicate
  return refs.filter(function (v, i, a) { return a.indexOf(v) === i; });
}

var VERIFY_SYSTEM_PROMPT =
  'You are a Swiss legal reference verification assistant. ' +
  'Given a text passage that cites a court decision, and the decision\'s case brief, ' +
  'determine whether the citation accurately supports the claim made in the text.\n\n' +
  'Respond ONLY in valid JSON:\n' +
  '{\n' +
  '  "verdict": "supports" or "partial" or "contradicts",\n' +
  '  "explanation": "Brief explanation in the document\'s language (2-3 sentences max)",\n' +
  '  "relevant_erwaegung": "The most relevant E./consid. number, e.g. \'3\' or \'2.1\'",\n' +
  '  "quote": "Key quote from the decision supporting your verdict (max 200 chars)"\n' +
  '}';

async function verifyReference(selectedText, apiKey, lang) {
  // Extract citations
  var refs = extractCitations(selectedText);
  if (refs.length === 0) {
    throw { type: 'no_citation', message: 'Keine Entscheidreferenz im markierten Text gefunden.' };
  }

  // Fetch first cited decision
  var mainRef = refs[0];
  var caseBrief;
  try {
    caseBrief = await getCaseBrief(mainRef);
  } catch (e) {
    throw { type: 'decision_not_found', message: 'Entscheid "' + mainRef + '" nicht gefunden.' };
  }

  // Build brief text for LLM (max 4000 chars)
  var parts = [];
  if (caseBrief.regeste) parts.push(caseBrief.regeste);
  if (caseBrief.sachverhalt) parts.push(caseBrief.sachverhalt);
  if (caseBrief.erwaegungen && caseBrief.erwaegungen.length) {
    parts.push(caseBrief.erwaegungen.map(function (e) {
      return 'E. ' + e.number + ': ' + e.text;
    }).join('\n'));
  }
  if (caseBrief.dispositiv) parts.push(caseBrief.dispositiv);
  var briefText = parts.join('\n\n').substring(0, 4000);

  // Call Claude Haiku
  var resp = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-api-key': apiKey,
      'anthropic-version': '2023-06-01',
      'anthropic-dangerous-direct-browser-access': 'true',
    },
    body: JSON.stringify({
      model: 'claude-haiku-4-5-20251001',
      max_tokens: 500,
      system: VERIFY_SYSTEM_PROMPT,
      messages: [{
        role: 'user',
        content: 'Text passage (check if the citation supports the claims):\n"' +
          selectedText + '"\n\nCited decision (' + mainRef + ') - case brief:\n' + briefText,
      }],
    }),
  });

  if (!resp.ok) {
    var err = {};
    try { err = await resp.json(); } catch (e) { /* ignore */ }
    if (resp.status === 401) throw { type: 'invalid_key', message: 'API-Key ungueltig.' };
    throw { type: 'llm_error', message: (err.error && err.error.message) || 'Fehler bei der Analyse.' };
  }

  var data = await resp.json();
  var content = (data.content && data.content[0] && data.content[0].text) || '';

  var result;
  try {
    result = JSON.parse(content);
  } catch (e) {
    // Try to extract JSON from response
    var jsonMatch = content.match(/\{[\s\S]*\}/);
    if (jsonMatch) {
      result = JSON.parse(jsonMatch[0]);
    } else {
      throw { type: 'parse_error', message: 'Analyse konnte nicht verarbeitet werden.' };
    }
  }

  // Attach decision for "Volltext" navigation
  result._decision = caseBrief;
  result._ref = mainRef;

  return result;
}
