"use strict";

class OpenCaseLawClient {
  constructor(options = {}) {
    this.baseUrl = (options.baseUrl || process.env.OCL_API_BASE || "https://mcp.opencaselaw.ch/api").replace(/\/+$/, "");
    this.timeoutMs = options.timeoutMs || Number(process.env.OCL_TIMEOUT_MS || 18000);
    this.fetchImpl = options.fetchImpl || fetch;
  }

  async request(pathname, params = {}, options = {}) {
    const url = new URL(`${this.baseUrl}${pathname}`);
    for (const [key, value] of Object.entries(params)) {
      if (value !== undefined && value !== null && value !== "") {
        url.searchParams.set(key, String(value));
      }
    }

    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.timeoutMs);
    const init = {
      method: options.method || "GET",
      headers: {
        "accept": "application/json",
        ...(options.headers || {}),
      },
      signal: controller.signal,
    };

    if (options.body) {
      init.headers["content-type"] = "application/json";
      init.body = JSON.stringify(options.body);
    }

    try {
      const response = await this.fetchImpl(url, init);
      const text = await response.text();
      let payload = null;
      if (text) {
        try {
          payload = JSON.parse(text);
        } catch {
          payload = { raw: text };
        }
      }
      if (!response.ok) {
        const detail = payload && (payload.detail || payload.message || payload.error);
        throw new Error(`${response.status} ${response.statusText}${detail ? `: ${detail}` : ""}`);
      }
      return payload || {};
    } finally {
      clearTimeout(timer);
    }
  }

  searchDecisions(params) {
    return this.request("/decisions", {
      q: params.query,
      canton: params.canton,
      language: params.language,
      date_from: params.dateFrom,
      date_to: params.dateTo,
      limit: params.limit || 12,
      fields: params.fields || "compact",
      sort: "relevance",
    });
  }

  findLeadingCases(params) {
    return this.request("/leading-cases", {
      query: params.query,
      court: params.court,
      date_from: params.dateFrom,
      date_to: params.dateTo,
      limit: params.limit || 8,
    });
  }

  searchLaws(params) {
    return this.request("/laws/search", {
      q: params.query,
      sr_number: params.srNumber,
      canton: params.canton,
      jurisdiction: params.jurisdiction || "all",
      language: params.language,
      limit: params.limit || 8,
    });
  }

  searchLegislation(params) {
    return this.request("/legislation/search", {
      q: params.query,
      canton: params.canton,
      language: params.language,
      limit: params.limit || 8,
      active_only: true,
      fetch_top_n_texts: 1,
    });
  }

  searchBotschaft(params) {
    return this.request("/search-botschaft", {
      q: params.query,
      language: params.language,
      limit: params.limit || 8,
    });
  }

  searchScholarship(params) {
    return this.request("/scholarship/search", {
      q: params.query,
      language: params.language,
      limit: params.limit || 8,
    });
  }

  getArticleHistory({ srNumber, article, language, limit }) {
    return this.request(`/article-history/${encodeURIComponent(srNumber)}/${encodeURIComponent(article)}`, {
      language: language || "de",
      leading_cases_limit: limit || 5,
    });
  }

  attestDraft({ draftText }) {
    return this.request("/attest", {}, {
      method: "POST",
      body: {
        redacted_text: draftText,
        audit_grounding: false,
        audit_quotes: false,
        client_redactor_version: "agentic-research-desk-mvp/0.1.0",
      },
    });
  }
}

module.exports = {
  OpenCaseLawClient,
};
