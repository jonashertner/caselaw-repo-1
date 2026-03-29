/**
 * REST API client for mcp.opencaselaw.ch.
 * Handles fetch, error states, retry, rate limiting.
 */

const API_BASE = 'https://mcp.opencaselaw.ch/api';

async function apiFetch(path, params) {
  return _apiFetchOnce(path, params).catch(function (e) {
    if (e.type === 'rate_limit' || e.type === 'http_error') throw e;
    // Network error — retry once after 1s
    return new Promise(function (resolve) { setTimeout(resolve, 1000); })
      .then(function () { return _apiFetchOnce(path, params); });
  });
}

async function _apiFetchOnce(path, params) {
  params = params || {};
  var url = new URL(API_BASE + path);
  Object.keys(params).forEach(function (k) {
    var v = params[k];
    if (v !== undefined && v !== null && v !== '') url.searchParams.set(k, v);
  });

  var resp = await fetch(url.toString());

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

// ── Billing / Pro ───────────────────────────────────────────

async function findCitations(decisionId, direction, limit) {
  return apiFetch('/citations/' + encodeURIComponent(decisionId), {
    direction: direction || 'both',
    limit: limit || 10,
  });
}

async function createCheckout(successUrl, cancelUrl, locale) {
  var params = 'success_url=' + encodeURIComponent(successUrl) +
    '&cancel_url=' + encodeURIComponent(cancelUrl);
  if (locale) params += '&locale=' + encodeURIComponent(locale);
  return apiPost('/billing/checkout?' + params);
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

async function findSupportingDecisions(licenseKey, statement, lang) {
  return apiPost('/billing/find-support', {
    license_key: licenseKey,
    statement: statement,
    lang: lang || 'de',
  });
}

async function apiPost(path, body) {
  return _apiPostOnce(path, body).catch(function (e) {
    if (e.type === 'rate_limit' || e.type === 'invalid_license' || e.type === 'http_error') throw e;
    // Network error — retry once after 2s
    return new Promise(function (resolve) { setTimeout(resolve, 2000); })
      .then(function () { return _apiPostOnce(path, body); });
  });
}

async function _apiPostOnce(path, body) {
  var url = API_BASE + path;
  var opts = { method: 'POST' };
  if (body) {
    opts.headers = { 'Content-Type': 'application/json' };
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
