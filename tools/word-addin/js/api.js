/**
 * REST API client for mcp.opencaselaw.ch.
 * Handles fetch, error states, retry, rate limiting.
 */

const API_BASE = 'https://mcp.opencaselaw.ch/api';

async function apiFetch(path, params) {
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
