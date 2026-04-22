/**
 * REST API client for mcp.opencaselaw.ch.
 * Handles fetch, error states, retry, rate limiting.
 *
 * Privacy: optionally sends X-Install-Cohort, an 8-hex-char monthly hash
 * SHA-256(install_id + YYYY-MM)[:8]. See buildClientHeaders() below.
 * See docs/datenschutz/ for the full privacy contract.
 */

const API_BASE = 'https://mcp.opencaselaw.ch/api';

// ─── Install cohort (privacy-respecting usage signal) ────────────────
// One random UUID per install is stored locally. Every request derives
// a fresh hash per month, so cross-month tracking is cryptographically
// infeasible. The user can disable it in settings at any time via the
// 'ocl_usage_signal_optout' localStorage key.

function oclGetInstallId() {
  try {
    var id = localStorage.getItem('ocl_install_id');
    if (!id) {
      if (typeof crypto !== 'undefined' && crypto.randomUUID) {
        id = crypto.randomUUID();
      } else {
        // Fallback: 16 random bytes as hex
        var buf = new Uint8Array(16);
        crypto.getRandomValues(buf);
        id = Array.from(buf, function (b) { return b.toString(16).padStart(2, '0'); }).join('');
      }
      localStorage.setItem('ocl_install_id', id);
    }
    return id;
  } catch (e) {
    return null;  // localStorage unavailable — skip the header entirely
  }
}

async function oclComputeCohort() {
  // Opt-out check
  try {
    if (localStorage.getItem('ocl_usage_signal_optout') === '1') return null;
  } catch (e) { return null; }

  var id = oclGetInstallId();
  if (!id) return null;
  var now = new Date();
  var month = now.getUTCFullYear() + '-' + String(now.getUTCMonth() + 1).padStart(2, '0');
  var input = id + '|' + month;
  try {
    var buf = new TextEncoder().encode(input);
    var hashBuf = await crypto.subtle.digest('SHA-256', buf);
    var hex = Array.from(new Uint8Array(hashBuf), function (b) {
      return b.toString(16).padStart(2, '0');
    }).join('');
    return hex.slice(0, 8);
  } catch (e) {
    return null;
  }
}

async function oclBuildHeaders(extra) {
  var headers = { 'X-Client': 'word-addin' };
  var cohort = await oclComputeCohort();
  if (cohort) headers['X-Install-Cohort'] = cohort;
  if (extra) {
    Object.keys(extra).forEach(function (k) { headers[k] = extra[k]; });
  }
  return headers;
}

async function apiFetch(path, params) {
  params = params || {};
  var url = new URL(API_BASE + path);
  Object.keys(params).forEach(function (k) {
    var v = params[k];
    if (v !== undefined && v !== null && v !== '') url.searchParams.set(k, v);
  });

  var headers = await oclBuildHeaders();
  var resp = await fetch(url.toString(), { headers: headers });

  if (resp.status === 429) {
    var retryAfter = parseInt(resp.headers.get('Retry-After') || '30', 10);
    throw { type: 'rate_limit', retryAfter: retryAfter };
  }
  if (!resp.ok) {
    throw { type: 'http_error', status: resp.status, message: resp.statusText };
  }
  return resp.json();
}

async function searchDecisions(query, filters) {
  filters = filters || {};
  return apiFetch('/decisions', {
    query: query,
    court: filters.court,
    canton: filters.canton,
    language: filters.language,
    date_from: filters.dateFrom,
    date_to: filters.dateTo,
    limit: filters.limit || 20,
    offset: filters.offset || 0,
    sort: filters.sort || 'relevance',
  });
}

async function getDecision(decisionId) {
  return apiFetch('/decisions/' + encodeURIComponent(decisionId), { full_text: true });
}

async function getCaseBrief(caseRef) {
  return apiFetch('/case-brief/' + encodeURIComponent(caseRef));
}

async function listCourts() {
  return apiFetch('/courts');
}

async function searchLaws(query, opts) {
  opts = opts || {};
  return apiFetch('/laws/search', {
    query: query,
    language: opts.language || 'de',
    limit: opts.limit || 10,
  });
}

async function getLaw(abbreviation, article, language) {
  return apiFetch('/laws/' + encodeURIComponent(abbreviation), {
    article: article,
    language: language,
  });
}

async function getLeadingCases(query, lawCode, article) {
  return apiFetch('/leading-cases', {
    query: query,
    law_code: lawCode,
    article: article,
  });
}

async function getDoctrine(query) {
  return apiFetch('/doctrine', { query: query });
}

// Canonical citation lookup — returns citation_string_{de,fr,it} + canonical_url + close_matches.
async function citeReference(reference, lang) {
  return apiFetch('/cite', { reference: reference, lang: lang || 'de' });
}

// Document audit — single POST that finds every citation, validates existence
// and pinpoints, and returns annotated_text + structured issues with positions.
async function attestDocument(draftText, lang) {
  return apiPost('/attest', { draft_text: draftText, lang: lang || 'de' });
}

// ── Billing / Pro ───────────────────────────────────────────

async function createCheckout(successUrl, cancelUrl) {
  return apiPost('/billing/checkout?success_url=' + encodeURIComponent(successUrl) +
    '&cancel_url=' + encodeURIComponent(cancelUrl));
}

async function validateLicense(key) {
  return apiFetch('/billing/validate', { key: key });
}

async function verifyReferencePro(licenseKey, selectedText, caseRef, lang) {
  return apiPost('/billing/verify', {
    license_key: licenseKey,
    selected_text: selectedText,
    case_ref: caseRef,
    lang: lang || 'de',
  });
}

async function apiPost(path, body) {
  var url = API_BASE + path;
  var headers = await oclBuildHeaders();
  var opts = { method: 'POST', headers: headers };
  if (body) {
    opts.headers['Content-Type'] = 'application/json';
    opts.body = JSON.stringify(body);
  }
  var resp = await fetch(url, opts);
  if (resp.status === 429) {
    throw { type: 'rate_limit', retryAfter: parseInt(resp.headers.get('Retry-After') || '30', 10) };
  }
  if (resp.status === 401) {
    throw { type: 'invalid_license', message: 'License key invalid or expired.' };
  }
  if (!resp.ok) {
    var errData = {};
    try { errData = await resp.json(); } catch (e) {}
    throw { type: 'http_error', status: resp.status, message: errData.error || resp.statusText };
  }
  return resp.json();
}
