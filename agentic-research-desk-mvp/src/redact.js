"use strict";

const STRUCTURAL_PATTERNS = [
  { name: "email", regex: /\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b/gi, token: "[EMAIL]" },
  { name: "iban", regex: /\bCH\d{2}(?:\s?[0-9A-Z]){17}\b/gi, token: "[IBAN]" },
  { name: "ahv", regex: /\b756[.\s-]?\d{4}[.\s-]?\d{4}[.\s-]?\d{2}\b/g, token: "[AHV]" },
  { name: "phone", regex: /(?:\+41|0041|0)\s?\d{2}\s?\d{3}\s?\d{2}\s?\d{2}\b/g, token: "[PHONE]" },
  { name: "postal_address", regex: /\b[A-ZÄÖÜ][A-Za-zÄÖÜäöüéèàç'\-]+(?:strasse|gasse|weg|platz|allee)\s+\d+[a-z]?\b/g, token: "[ADDRESS]" },
];

const STOPWORDS = new Set([
  "der", "die", "das", "den", "dem", "des", "und", "oder", "mit", "ohne", "für", "zur", "zum",
  "eine", "einer", "eines", "ein", "im", "in", "am", "an", "auf", "bei", "von", "vor", "nach",
  "the", "and", "for", "with", "without", "from", "into", "about", "over", "under", "against",
  "le", "la", "les", "des", "du", "de", "et", "ou", "avec", "sans", "dans", "sur",
  "find", "leading", "swiss", "case", "cases", "legal", "research", "memo", "prepare", "identify",
  "controlling", "statute", "statutes", "court", "decision", "decisions", "law", "laws",
  "art", "article", "termination", "dismissal", "notice", "during", "illness", "sickness",
  "disease", "incapacity",
]);

const ISSUE_EXPANSIONS = [
  { regex: /\btermination|dismissal|notice\b/i, terms: ["Kündigung"] },
  { regex: /\billness|sickness|disease|incapacity\b/i, terms: ["Krankheit", "Sperrfrist"] },
  { regex: /\bemployer|employee|employment|work\b/i, terms: ["Arbeitsvertrag", "Arbeitnehmer", "Arbeitgeber"] },
  { regex: /\brent|lease|tenant|landlord\b/i, terms: ["Miete", "Mietvertrag", "Kündigung"] },
  { regex: /\bprivacy|data protection|personal data\b/i, terms: ["Datenschutz", "Personendaten"] },
  { regex: /\basylum|refugee|migration\b/i, terms: ["Asyl", "Wegweisung", "Ausländerrecht"] },
];

function redactStructuralPii(text = "") {
  let redacted = String(text);
  const patterns = [];
  for (const pattern of STRUCTURAL_PATTERNS) {
    if (pattern.regex.test(redacted)) {
      patterns.push(pattern.name);
      redacted = redacted.replace(pattern.regex, pattern.token);
    }
    pattern.regex.lastIndex = 0;
  }
  return { redacted, patterns };
}

function compactWhitespace(text = "") {
  return String(text).replace(/\s+/g, " ").trim();
}

function truncate(text = "", maxLength = 600) {
  const compact = compactWhitespace(text);
  if (compact.length <= maxLength) {
    return compact;
  }
  return `${compact.slice(0, maxLength - 1).trim()}...`;
}

function extractRemoteQuery({ objective = "", facts = "", statuteReferences = "" } = {}) {
  const combined = [objective, statuteReferences].filter(Boolean).join(" ");
  const redacted = redactStructuralPii(combined).redacted;
  const expansions = [];
  for (const expansion of ISSUE_EXPANSIONS) {
    if (expansion.regex.test(combined)) {
      expansions.push(...expansion.terms);
    }
  }
  const tokens = redacted
    .replace(/\[[A-Z_]+]/g, " ")
    .replace(/[^\p{L}\p{N}_./ ]/gu, " ")
    .split(/\s+/)
    .map((token) => token.trim().replace(/^[./]+|[./]+$/g, ""))
    .filter((token) => token.length >= 3 && !STOPWORDS.has(token.toLowerCase()));

  const unique = [];
  for (const token of tokens) {
    if (!unique.includes(token)) {
      unique.push(token);
    }
  }

  for (const term of expansions) {
    if (!unique.includes(term)) {
      unique.push(term);
    }
  }

  const query = unique.slice(0, 14).join(" ");
  const fallback = truncate(redactStructuralPii(objective || facts).redacted, 160);
  return query || fallback || "Schweizer Recht";
}

module.exports = {
  compactWhitespace,
  extractRemoteQuery,
  redactStructuralPii,
  truncate,
};
