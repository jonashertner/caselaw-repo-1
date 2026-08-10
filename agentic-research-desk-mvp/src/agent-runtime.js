"use strict";

const { OpenCaseLawClient } = require("./opencaselaw-client");
const {
  extractRemoteQuery,
  redactStructuralPii,
  truncate,
} = require("./redact");

const jobs = new Map();
let nextJobNumber = 1;
let nextTraceNumber = 1;

const AGENT_MODES = {
  source_packet: {
    label: "Source Packet Agent",
    objective: "Find controlling Swiss legal sources and package them for lawyer or downstream-agent review.",
    output: "Verified source packet plus memo export.",
  },
  authority_map: {
    label: "Authority Map Agent",
    objective: "Prioritize leading cases, later cases, and authority signals.",
    output: "Ranked authority map with citation-graph provenance.",
  },
  legislation_trace: {
    label: "Legislation Trace Agent",
    objective: "Trace current law, article history, Materialien, and related case law.",
    output: "Legislative history packet with article timeline.",
  },
  counter_authority: {
    label: "Counter-Authority Agent",
    objective: "Surface exceptions, contrary authorities, and weak points in a legal position.",
    output: "Counter-authority packet with caveats and adversarial probes.",
  },
  draft_verification: {
    label: "Draft Verification Agent",
    objective: "Check whether cited or implied Swiss legal propositions are source-supported.",
    output: "Verification packet with gaps, unsupported-claim warnings, and safer source candidates.",
  },
};

const STAGE_ORDER = [
  ["intake", "Intake"],
  ["plan", "Research plan"],
  ["approval", "Human approval"],
  ["retrieval", "Parallel retrieval"],
  ["evidence", "Evidence ledger"],
  ["counter", "Counter-authority probe"],
  ["draft", "Memo drafting"],
  ["audit", "Critic and citation audit"],
  ["final", "Final packet"],
];

const FEDERAL_SR = {
  BV: "101",
  ZGB: "210",
  OR: "220",
  ZPO: "272",
  SchKG: "281.1",
  StGB: "311.0",
  StPO: "312.0",
  VwVG: "172.021",
  DSG: "235.1",
  EMRK: "0.101",
};

function now() {
  return new Date().toISOString();
}

function makeJobId() {
  const suffix = String(nextJobNumber++).padStart(4, "0");
  return `job_${suffix}`;
}

function clone(value) {
  return JSON.parse(JSON.stringify(value));
}

function initialStages() {
  return STAGE_ORDER.map(([id, label]) => ({
    id,
    label,
    status: id === "intake" || id === "plan" ? "completed" : "pending",
    startedAt: id === "intake" || id === "plan" ? now() : null,
    completedAt: id === "intake" || id === "plan" ? now() : null,
    notes: [],
  }));
}

function updateJob(job) {
  job.updatedAt = now();
  jobs.set(job.id, job);
}

function stage(job, id) {
  return job.stages.find((item) => item.id === id);
}

function setStage(job, id, status, note = null) {
  const item = stage(job, id);
  if (!item) {
    return;
  }
  item.status = status;
  if (status === "running" && !item.startedAt) {
    item.startedAt = now();
  }
  if (status === "completed" || status === "warning" || status === "failed") {
    item.completedAt = now();
  }
  if (note) {
    item.notes.push({ at: now(), text: note });
  }
  updateJob(job);
}

function normalizeJurisdiction(input) {
  if (!input || input === "all") {
    return { label: "All Switzerland", canton: null, lawJurisdiction: "all" };
  }
  if (input === "federal" || input === "CH") {
    return { label: "Federal", canton: "CH", lawJurisdiction: "federal" };
  }
  return { label: input, canton: input, lawJurisdiction: "cantonal" };
}

function extractStatuteRefs(text = "") {
  const refs = [];
  const regex = /\bArt\.?\s*(\d+[a-z]?(?:\s*[a-z])?)\s+(BV|ZGB|OR|ZPO|SchKG|StGB|StPO|VwVG|DSG|EMRK)\b/gi;
  let match;
  while ((match = regex.exec(text)) !== null) {
    const article = match[1].replace(/\s+/g, "");
    const lawCode = match[2];
    refs.push({
      article,
      lawCode,
      srNumber: FEDERAL_SR[lawCode],
      label: `Art. ${article} ${lawCode}`,
    });
  }
  return refs;
}

function buildResearchPlan(input = {}) {
  const jurisdiction = normalizeJurisdiction(input.jurisdiction);
  const agentType = AGENT_MODES[input.agentType] ? input.agentType : "source_packet";
  const agentMode = AGENT_MODES[agentType];
  const redacted = redactStructuralPii([input.objective, input.facts, input.statuteReferences].filter(Boolean).join("\n"));
  const statuteRefs = extractStatuteRefs(`${input.objective || ""}\n${input.statuteReferences || ""}\n${input.facts || ""}`);
  const remoteQuery = buildRemoteQuery(input, statuteRefs);
  const language = input.language || "de";
  const riskLevel = input.riskLevel || "standard";

  const tasks = [
    {
      id: "scope",
      role: "Planner",
      action: `Confirm jurisdiction, language, date range, output type, and high-risk ambiguity for ${agentMode.label}.`,
      status: "ready",
    },
    {
      id: "case-sweep",
      role: "Precedent researcher",
      action: "Search Swiss court decisions for the research issue and collect citable authorities.",
      tool: "GET /api/decisions",
      status: "ready",
    },
    {
      id: "leading-cases",
      role: "Authority researcher",
      action: "Rank leading cases by citation graph and surface likely controlling authorities.",
      tool: "GET /api/leading-cases",
      status: "ready",
    },
    {
      id: "statute-sweep",
      role: "Statute researcher",
      action: "Search federal and cantonal statute articles relevant to the issue.",
      tool: "GET /api/laws/search",
      status: "ready",
    },
    {
      id: "materials-doctrine",
      role: "Materials researcher",
      action: "Search Botschaften, legislation index, and open legal scholarship for context.",
      tool: "GET /api/search-botschaft, /api/scholarship/search, /api/legislation/search",
      status: "ready",
    },
    {
      id: "counter-authority",
      role: "Adversarial critic",
      action: agentType === "counter_authority"
        ? "Run adversarial searches first and classify authority that weakens the stated position."
        : "Probe for exceptions, negative treatment, and authorities that weaken the first-pass theory.",
      status: "ready",
    },
    {
      id: "draft",
      role: "Memo drafter",
      action: "Draft only from the evidence ledger, with every proposition traceable to a source id.",
      status: "blocked_until_retrieval",
    },
    {
      id: "audit",
      role: "Citation auditor",
      action: "Check source traceability, coverage gaps, unsupported claims, and export readiness.",
      status: "blocked_until_draft",
    },
  ];

  for (const ref of statuteRefs) {
    if (ref.srNumber) {
      tasks.splice(4, 0, {
        id: `article-history-${ref.lawCode}-${ref.article}`,
        role: "Legislative history researcher",
        action: `Trace article history and leading cases for ${ref.label}.`,
        tool: `GET /api/article-history/${ref.srNumber}/${ref.article}`,
        status: "ready",
      });
    }
  }

  return {
    createdAt: now(),
    agentType,
    agentLabel: agentMode.label,
    productObjective: agentMode.objective,
    expectedOutput: agentMode.output,
    remoteQuery,
    jurisdiction,
    language,
    riskLevel,
    outputType: input.outputType || "research_memo",
    statuteRefs,
    piiPatternsDetected: redacted.patterns,
    privacyPosture: redacted.patterns.length
      ? "Structural PII was detected and redacted from remote search terms."
      : "Remote searches use extracted issue terms, not a full fact upload.",
    approvalRequired: true,
    tasks,
  };
}

function buildRemoteQuery(input, statuteRefs) {
  const base = extractRemoteQuery(input);
  const statuteTerms = statuteRefs.flatMap((ref) => [ref.article, ref.lawCode === "OR" ? "" : ref.lawCode]).filter(Boolean);
  const tokens = [...statuteTerms, ...base.split(/\s+/)]
    .map((token) => token.trim().replace(/^[./]+|[./]+$/g, ""))
    .filter(Boolean);
  const unique = [];
  const seen = new Set();
  for (const token of tokens) {
    const key = token.toLowerCase();
    if (!seen.has(key)) {
      seen.add(key);
      unique.push(token);
    }
  }
  return unique.slice(0, 14).join(" ");
}

function createJob(input = {}) {
  if (!input.objective || !input.objective.trim()) {
    throw new Error("Objective is required.");
  }

  const redactedFacts = redactStructuralPii(input.facts || "");
  const job = {
    id: makeJobId(),
    status: "needs_approval",
    createdAt: now(),
    updatedAt: now(),
    tenantId: "local-mvp",
    matterId: input.matterId || `matter-${new Date().toISOString().slice(0, 10)}`,
    objective: input.objective.trim(),
    agentType: AGENT_MODES[input.agentType] ? input.agentType : "source_packet",
    agentLabel: AGENT_MODES[AGENT_MODES[input.agentType] ? input.agentType : "source_packet"].label,
    localFactsPreview: truncate(redactedFacts.redacted, 900),
    constraints: {
      jurisdiction: input.jurisdiction || "federal",
      language: input.language || "de",
      dateFrom: input.dateFrom || "",
      dateTo: input.dateTo || "",
      outputType: input.outputType || "research_memo",
      riskLevel: input.riskLevel || "standard",
      agentType: AGENT_MODES[input.agentType] ? input.agentType : "source_packet",
    },
    plan: buildResearchPlan(input),
    stages: initialStages(),
    toolTrace: [],
    evidence: [],
    sourceMap: null,
    gaps: [],
    memo: null,
    packet: null,
    audit: null,
    errors: [],
    planApproval: null,
  };

  setStage(job, "approval", "pending", "Research plan is waiting for human approval.");
  jobs.set(job.id, job);
  return publicJob(job);
}

function publicJob(job) {
  return clone(job);
}

function listJobs() {
  return Array.from(jobs.values())
    .sort((a, b) => b.createdAt.localeCompare(a.createdAt))
    .map((job) => ({
      id: job.id,
      status: job.status,
      matterId: job.matterId,
      agentType: job.agentType,
      agentLabel: job.agentLabel,
      objective: job.objective,
      createdAt: job.createdAt,
      updatedAt: job.updatedAt,
      evidenceCount: job.evidence.length,
      packetReady: Boolean(job.packet),
      auditOk: job.audit ? job.audit.ok : null,
    }));
}

function getJob(id) {
  const job = jobs.get(id);
  return job ? publicJob(job) : null;
}

function approveJob(id, approval = {}) {
  const job = jobs.get(id);
  if (!job) {
    return null;
  }
  if (job.status !== "needs_approval") {
    return publicJob(job);
  }
  job.planApproval = {
    approvedAt: now(),
    approvedBy: approval.approvedBy || "local-user",
    notes: approval.notes || "",
  };
  setStage(job, "approval", "completed", "Human approved the plan.");
  job.status = "running";
  updateJob(job);

  const client = approval.client || new OpenCaseLawClient();
  runResearchJob(job, { client }).catch((error) => {
    job.status = "failed";
    job.errors.push({ at: now(), stage: "runtime", message: error.message });
    setStage(job, "final", "failed", error.message);
    updateJob(job);
  });

  return publicJob(job);
}

function countResults(payload) {
  if (!payload) {
    return 0;
  }
  if (Array.isArray(payload)) {
    return payload.length;
  }
  for (const key of ["results", "cases", "leading_cases", "items", "timeline"]) {
    if (Array.isArray(payload[key])) {
      return payload[key].length;
    }
  }
  return Object.keys(payload).length ? 1 : 0;
}

async function callTool(job, name, params, fn) {
  const trace = {
    id: `trace_${String(nextTraceNumber++).padStart(5, "0")}`,
    name,
    params,
    status: "running",
    startedAt: now(),
    completedAt: null,
    durationMs: null,
    resultCount: null,
    error: null,
  };
  const started = Date.now();
  job.toolTrace.push(trace);
  updateJob(job);

  try {
    const payload = await fn();
    trace.status = "success";
    trace.resultCount = countResults(payload);
    return { name, params, payload };
  } catch (error) {
    trace.status = "error";
    trace.error = error.message;
    job.errors.push({ at: now(), stage: "tool", tool: name, message: error.message });
    return { name, params, error };
  } finally {
    trace.completedAt = now();
    trace.durationMs = Date.now() - started;
    updateJob(job);
  }
}

function buildCounterQuery(remoteQuery, language) {
  const probes = {
    de: "Ausnahme abweichend verneint nicht",
    fr: "exception contraire rejette pas",
    it: "eccezione contrario respinge non",
    en: "exception contrary rejected not",
  };
  return `${remoteQuery} ${probes[language] || probes.de}`;
}

async function runResearchJob(job, { client = new OpenCaseLawClient() } = {}) {
  const query = job.plan.remoteQuery;
  const jurisdiction = job.plan.jurisdiction;
  const language = job.plan.language === "en" ? undefined : job.plan.language;
  const dateFrom = job.constraints.dateFrom || undefined;
  const dateTo = job.constraints.dateTo || undefined;
  const primaryStatute = job.plan.statuteRefs.find((item) => item.srNumber);
  const lawQuery = primaryStatute
    ? Array.from(new Set([primaryStatute.article, ...query.split(/\s+/).filter((term) => term !== primaryStatute.lawCode)])).slice(0, 8).join(" ")
    : query;

  setStage(job, "retrieval", "running", "Running case, statute, materials, and scholarship searches in parallel.");
  const retrievalCalls = [];
  if (!primaryStatute) {
    retrievalCalls.push(callTool(job, "search_decisions", { query, canton: jurisdiction.canton, language, dateFrom, dateTo, limit: 8 }, () =>
      client.searchDecisions({ query, canton: jurisdiction.canton, language, dateFrom, dateTo, limit: 8 })));
  } else {
    job.toolTrace.push({
      id: `trace_${String(nextTraceNumber++).padStart(5, "0")}`,
      name: "search_decisions",
      params: { query, skippedBecause: "statute_reference_detected" },
      status: "skipped",
      startedAt: now(),
      completedAt: now(),
      durationMs: 0,
      resultCount: 0,
      error: null,
    });
  }

  retrievalCalls.push(
    callTool(job, "find_leading_cases", { query, dateFrom, dateTo, limit: 8 }, () =>
      client.findLeadingCases({ query, dateFrom, dateTo, limit: 8 })),
    callTool(job, "search_laws", { query: lawQuery, srNumber: primaryStatute && primaryStatute.srNumber, canton: jurisdiction.canton, jurisdiction: jurisdiction.lawJurisdiction, language, limit: 8 }, () =>
      client.searchLaws({ query: lawQuery, srNumber: primaryStatute && primaryStatute.srNumber, canton: jurisdiction.canton, jurisdiction: jurisdiction.lawJurisdiction, language, limit: 8 })),
    callTool(job, "search_legislation", { query, canton: jurisdiction.canton, language, limit: 8 }, () =>
      client.searchLegislation({ query, canton: jurisdiction.canton, language, limit: 8 })),
    callTool(job, "search_botschaft", { query, language, limit: 8 }, () =>
      client.searchBotschaft({ query, language, limit: 8 })),
    callTool(job, "search_scholarship", { query, language, limit: 8 }, () =>
      client.searchScholarship({ query, language, limit: 8 })),
  );

  for (const ref of job.plan.statuteRefs.filter((item) => item.srNumber).slice(0, 3)) {
    retrievalCalls.push(callTool(
      job,
      "get_article_history",
      { srNumber: ref.srNumber, article: ref.article, language, label: ref.label },
      () => client.getArticleHistory({ srNumber: ref.srNumber, article: ref.article, language, limit: 5 }),
    ));
  }

  const retrieval = await Promise.all(retrievalCalls);
  const retrievalErrors = retrieval.filter((item) => item.error).length;
  setStage(
    job,
    "retrieval",
    retrievalErrors ? "warning" : "completed",
    retrievalErrors ? `${retrievalErrors} retrieval tool(s) failed; continuing with available evidence.` : "Retrieval completed.",
  );

  setStage(job, "evidence", "running", "Normalizing source results into a traceable evidence ledger.");
  job.evidence = rankEvidence(dedupeEvidence(retrieval.flatMap(normalizeEvidenceBundle))).map((item, index) => ({
    ...item,
    evidenceId: `E${index + 1}`,
  }));
  job.sourceMap = buildSourceMap(job.evidence);
  job.gaps = buildGapAnalysis(job);
  setStage(job, "evidence", job.evidence.length ? "completed" : "warning", `${job.evidence.length} evidence item(s) in ledger.`);

  setStage(job, "counter", "running", "Running a focused negative-treatment and exception probe.");
  const counterQuery = buildCounterQuery(query, job.plan.language);
  const counter = await callTool(job, "counter_authority_probe", { query: counterQuery, limit: 8 }, () =>
    client.findLeadingCases({ query: counterQuery, dateFrom, dateTo, limit: 8 }));
  const counterEvidence = normalizeEvidenceBundle(counter).map((item) => ({ ...item, type: "counter_case" }));
  const existingCount = job.evidence.length;
  job.evidence = rankEvidence(dedupeEvidence([...job.evidence, ...counterEvidence])).map((item, index) => ({
    ...item,
    evidenceId: item.evidenceId || `E${index + 1}`,
  }));
  job.sourceMap = buildSourceMap(job.evidence);
  setStage(job, "counter", counter.error ? "warning" : "completed", `${Math.max(0, job.evidence.length - existingCount)} counter-authority item(s) added.`);

  setStage(job, "draft", "running", "Drafting from the evidence ledger only.");
  job.memo = buildDraftMemo(job);
  setStage(job, "draft", "completed", "Draft memo packet generated.");

  setStage(job, "audit", "running", "Running critic checks for traceability, coverage, and unsupported output.");
  job.audit = auditMemo(job);
  if (process.env.OCL_REMOTE_ATTEST === "1" && job.memo && job.memo.markdown) {
    const attest = await callTool(job, "attest_response", { draftText: "[redacted memo markdown]" }, () =>
      client.attestDraft({ draftText: job.memo.markdown }));
    job.audit.remoteAttest = attest.error ? { ok: false, error: attest.error.message || attest.error } : attest.payload;
  }
  setStage(job, "audit", job.audit.ok ? "completed" : "warning", job.audit.summary);
  job.packet = buildSourcePacket(job);

  setStage(job, "final", "completed", "Final packet is ready for lawyer review and export.");
  job.status = "completed";
  updateJob(job);
  return publicJob(job);
}

function valuesFromPayload(payload) {
  if (!payload || typeof payload !== "object") {
    return [];
  }
  for (const key of ["results", "cases", "leading_cases", "items", "timeline"]) {
    if (Array.isArray(payload[key])) {
      return payload[key];
    }
  }
  if (Array.isArray(payload.sources)) {
    return payload.sources;
  }
  return [];
}

function stripHtml(text = "") {
  return String(text).replace(/<[^>]*>/g, " ").replace(/\s+/g, " ").trim();
}

function firstNonEmpty(...values) {
  return values.find((value) => value !== undefined && value !== null && String(value).trim() !== "");
}

function normalizeEvidenceBundle(bundle) {
  if (!bundle || bundle.error) {
    return [];
  }
  const toolName = bundle.name;
  const rows = valuesFromPayload(bundle.payload);
  return rows.map((row) => normalizeEvidence(row, toolName)).filter(Boolean);
}

function normalizeEvidence(row, toolName) {
  const typeByTool = {
    search_decisions: "case",
    find_leading_cases: "leading_case",
    search_laws: "law",
    search_legislation: "legislation",
    search_botschaft: "material",
    search_scholarship: "scholarship",
    get_article_history: "article_history",
    counter_authority_probe: "counter_case",
  };
  let type = typeByTool[toolName] || "source";
  if (toolName === "get_article_history") {
    if (row.kind === "court_decision" || row.decision_id) {
      type = row.bge_ref ? "leading_case" : "case";
    } else if (row.kind === "botschaft") {
      type = "material";
    } else if (row.kind === "commentary") {
      type = "scholarship";
    }
  }

  const decisionId = firstNonEmpty(row.decision_id, row.id, row.case_id);
  const citation = firstNonEmpty(
    row.citation_string_de,
    row.citation,
    row.citation_string,
    row.bge_reference,
    row.bge_ref,
    row.docket_number,
    row.bbl_citation,
    row.article_label,
    row.law_abbreviation,
    row.title,
  );
  const title = firstNonEmpty(
    row.title,
    row.name,
    row.short_title,
    row.law_title,
    row.publication_title,
    citation,
    decisionId,
  );
  const sourceUrl = firstNonEmpty(
    row.canonical_url,
    row.url,
    row.source_url,
    row.eli_uri,
    row.fedlex_url,
    decisionId ? `https://opencaselaw.ch/entscheid/${decisionId}` : "",
  );
  const snippet = stripHtml(firstNonEmpty(
    row.snippet,
    row.highlighted_snippet,
    row.regeste,
    row.summary,
    row.text,
    row.excerpt,
    row.content,
    row.abstract,
  ) || "");

  if (!title && !citation && !snippet) {
    return null;
  }

  return {
    type,
    tool: toolName,
    title: truncate(title || citation || "Untitled source", 180),
    citation: truncate(citation || title || "", 220),
    decisionId: decisionId || "",
    court: firstNonEmpty(row.court_name, row.court, row.authority, row.canton, ""),
    date: firstNonEmpty(row.decision_date, row.date, row.publication_date, row.year, ""),
    sourceUrl: sourceUrl || "",
    snippet: truncate(snippet, 700),
    authorityScore: Number(firstNonEmpty(row.citation_count, row.score, row.relevance_score, 0)) || 0,
    isLeading: Boolean(row.is_leading_case || toolName === "find_leading_cases"),
    rawKeys: Object.keys(row).slice(0, 20),
  };
}

function evidenceKey(item) {
  if (item.decisionId) {
    return `decision:${item.decisionId.toLowerCase()}`;
  }
  if (item.sourceUrl) {
    return `url:${item.sourceUrl.toLowerCase()}`;
  }
  return [
    item.citation,
    item.title,
    item.type,
  ].filter(Boolean).join("|").toLowerCase();
}

function dedupeEvidence(items) {
  const seen = new Set();
  const result = [];
  for (const item of items) {
    const key = evidenceKey(item);
    if (key && seen.has(key)) {
      continue;
    }
    seen.add(key);
    result.push(item);
  }
  return result;
}

function rankEvidence(items) {
  const typeWeight = {
    leading_case: 100,
    case: 80,
    counter_case: 75,
    law: 70,
    article_history: 65,
    material: 55,
    scholarship: 45,
    legislation: 40,
  };
  return [...items].sort((a, b) => {
    const typeDelta = (typeWeight[b.type] || 0) - (typeWeight[a.type] || 0);
    if (typeDelta) {
      return typeDelta;
    }
    return (b.authorityScore || 0) - (a.authorityScore || 0);
  });
}

function buildSourceMap(evidence) {
  const map = {
    leadingCases: evidence.filter((item) => item.type === "leading_case").slice(0, 8),
    cases: evidence.filter((item) => item.type === "case").slice(0, 12),
    counterAuthority: evidence.filter((item) => item.type === "counter_case").slice(0, 8),
    statutes: evidence.filter((item) => item.type === "law" || item.type === "legislation").slice(0, 12),
    materials: evidence.filter((item) => item.type === "material" || item.type === "article_history").slice(0, 10),
    scholarship: evidence.filter((item) => item.type === "scholarship").slice(0, 10),
  };
  map.total = evidence.length;
  map.counts = {
    leadingCases: map.leadingCases.length,
    cases: map.cases.length,
    counterAuthority: map.counterAuthority.length,
    statutes: map.statutes.length,
    materials: map.materials.length,
    scholarship: map.scholarship.length,
  };
  return map;
}

function buildGapAnalysis(job) {
  const gaps = [];
  const byType = new Map();
  for (const item of job.evidence) {
    byType.set(item.type, (byType.get(item.type) || 0) + 1);
  }
  if (!(byType.get("case") || byType.get("leading_case"))) {
    gaps.push({ severity: "high", message: "No case-law evidence was found. Narrow or rephrase the research objective." });
  }
  if (!(byType.get("law") || byType.get("legislation"))) {
    gaps.push({ severity: "medium", message: "No statute evidence was found. Add known article references if the issue is statute-driven." });
  }
  if (!byType.get("counter_case")) {
    gaps.push({ severity: "medium", message: "Counter-authority probe did not return additional cases." });
  }
  if (!byType.get("material") && !byType.get("article_history")) {
    gaps.push({ severity: "low", message: "No preparatory-material evidence was found in this run." });
  }
  if (job.errors.length) {
    gaps.push({ severity: "medium", message: `${job.errors.length} tool error(s) occurred. Review trace before relying on the packet.` });
  }
  return gaps;
}

function evidenceList(items, max = 8) {
  if (!items.length) {
    return "- None surfaced in this run.";
  }
  return items.slice(0, max).map((item) => {
    const label = item.citation || item.title;
    const url = item.sourceUrl ? ` (${item.sourceUrl})` : "";
    const snippet = item.snippet ? `\n  - Source note: ${item.snippet}` : "";
    return `- [${item.evidenceId}] ${label}${url}${snippet}`;
  }).join("\n");
}

function buildDraftMemo(job) {
  const sourceMap = job.sourceMap || buildSourceMap(job.evidence);
  const gaps = job.gaps || [];
  const title = `Research memo packet: ${job.objective}`;
  const markdown = [
    `# ${title}`,
    "",
    `Matter: ${job.matterId}`,
    `Generated: ${now()}`,
    `Status: research work product for lawyer review, not legal advice.`,
    "",
    "## Objective",
    job.objective,
    "",
    "## Approved Agent Plan",
    ...job.plan.tasks.map((task) => `- ${task.role}: ${task.action}`),
    "",
    "## Source Map",
    `Total evidence items: ${sourceMap.total}`,
    "",
    "### Leading Cases",
    evidenceList(sourceMap.leadingCases),
    "",
    "### Case Law",
    evidenceList(sourceMap.cases),
    "",
    "### Statutes And Legislation",
    evidenceList(sourceMap.statutes),
    "",
    "### Preparatory Materials",
    evidenceList(sourceMap.materials),
    "",
    "### Scholarship",
    evidenceList(sourceMap.scholarship),
    "",
    "### Counter-Authority Probe",
    evidenceList(sourceMap.counterAuthority),
    "",
    "## Preliminary Research Orientation",
    "The agent found the sources above through the approved plan and did not use model memory for source discovery. Treat this section as an orientation layer: legal conclusions should be written or approved by a qualified professional after inspecting the evidence ledger.",
    "",
    "The strongest starting point is the leading-case and case-law set, followed by the statute and materials set where available. Counter-authority items should be reviewed before relying on any one-sided proposition.",
    "",
    "## Gaps And Caveats",
    gaps.length ? gaps.map((gap) => `- ${gap.severity.toUpperCase()}: ${gap.message}`).join("\n") : "- No automatic gaps were detected.",
    "",
    "## Audit Contract",
    "- Every cited source in this packet is referenced by an evidence id.",
    "- Quotations should be added only after opening the source URL or fetching the relevant Erwägung.",
    "- Final legal propositions should be checked with claim-support tooling before client delivery.",
    "",
    "## Evidence Ledger",
    ...job.evidence.map((item) => [
      `### ${item.evidenceId} ${item.type}`,
      `Citation: ${item.citation || item.title || "n/a"}`,
      `Tool: ${item.tool}`,
      item.sourceUrl ? `URL: ${item.sourceUrl}` : "URL: n/a",
      item.snippet ? `Snippet: ${item.snippet}` : "Snippet: n/a",
      "",
    ].join("\n")),
  ].join("\n");

  return {
    title,
    generatedAt: now(),
    markdown,
  };
}

function summarizeEvidence(item) {
  return {
    evidenceId: item.evidenceId,
    type: item.type,
    citation: item.citation || item.title || "",
    title: item.title || item.citation || "",
    decisionId: item.decisionId || "",
    court: item.court || "",
    date: item.date || "",
    sourceUrl: item.sourceUrl || "",
    snippet: item.snippet || "",
    authorityScore: item.authorityScore || 0,
    isLeading: Boolean(item.isLeading),
    tool: item.tool,
  };
}

function buildSourcePacket(job) {
  const sourceMap = job.sourceMap || buildSourceMap(job.evidence);
  const trace = job.toolTrace.map((item) => ({
    id: item.id,
    tool: item.name,
    status: item.status,
    durationMs: item.durationMs,
    resultCount: item.resultCount,
    params: item.params,
    error: item.error,
  }));

  return {
    schemaVersion: "ocl.agentic-access.source-packet.v1",
    packetId: `packet_${job.id}`,
    jobId: job.id,
    createdAt: job.createdAt,
    completedAt: job.updatedAt,
    tenantId: job.tenantId,
    matterId: job.matterId,
    agent: {
      type: job.agentType,
      label: job.agentLabel,
      modeObjective: job.plan.productObjective,
      expectedOutput: job.plan.expectedOutput,
    },
    objective: job.objective,
    constraints: job.constraints,
    query: {
      remoteQuery: job.plan.remoteQuery,
      jurisdiction: job.plan.jurisdiction,
      language: job.plan.language,
      statuteRefs: job.plan.statuteRefs,
      privacyPosture: job.plan.privacyPosture,
    },
    plan: job.plan.tasks.map((task) => ({
      id: task.id,
      role: task.role,
      action: task.action,
      tool: task.tool || "",
      status: task.status,
    })),
    sourceMap: {
      counts: sourceMap.counts,
      leadingCases: sourceMap.leadingCases.map(summarizeEvidence),
      cases: sourceMap.cases.map(summarizeEvidence),
      counterAuthority: sourceMap.counterAuthority.map(summarizeEvidence),
      statutes: sourceMap.statutes.map(summarizeEvidence),
      materials: sourceMap.materials.map(summarizeEvidence),
      scholarship: sourceMap.scholarship.map(summarizeEvidence),
    },
    evidenceLedger: job.evidence.map(summarizeEvidence),
    gaps: job.gaps,
    audit: job.audit,
    runTrace: trace,
    assurances: {
      humanPlanApproval: Boolean(job.planApproval),
      evidenceLedgerRequired: true,
      citationsConstructedByModel: false,
      finalLegalAdvice: false,
      remoteSearchUsesExtractedTermsOnly: true,
    },
    exports: {
      markdown: `/api/jobs/${job.id}/export.md`,
      json: `/api/jobs/${job.id}/packet.json`,
    },
  };
}

function auditMemo(job) {
  const issues = [];
  const markdown = job.memo ? job.memo.markdown : "";
  const evidenceIds = new Set(job.evidence.map((item) => item.evidenceId));
  const referenced = new Set(Array.from(markdown.matchAll(/\[(E\d+)]/g)).map((match) => match[1]));

  if (!job.evidence.length) {
    issues.push({ severity: "critical", message: "Memo has no evidence ledger." });
  }

  for (const id of referenced) {
    if (!evidenceIds.has(id)) {
      issues.push({ severity: "critical", message: `Memo references unknown evidence id ${id}.` });
    }
  }

  const usedUntraceable = job.evidence.filter((item) => !item.sourceUrl && !item.decisionId && !item.citation);
  if (usedUntraceable.length) {
    issues.push({ severity: "medium", message: `${usedUntraceable.length} evidence item(s) lack URL, decision id, or citation.` });
  }

  if (job.gaps.some((gap) => gap.severity === "high")) {
    issues.push({ severity: "high", message: "High-severity research gap remains open." });
  }

  if (/we (advise|recommend)|you should file|must sue/i.test(markdown)) {
    issues.push({ severity: "critical", message: "Draft contains advice-like language that should be removed." });
  }

  const criticalCount = issues.filter((issue) => issue.severity === "critical").length;
  const highCount = issues.filter((issue) => issue.severity === "high").length;
  return {
    ok: criticalCount === 0,
    summary: criticalCount
      ? `${criticalCount} critical audit issue(s) require correction.`
      : highCount
        ? `${highCount} high-severity caveat(s) remain; lawyer review required.`
        : "Traceability audit passed for MVP packet.",
    issues,
    metrics: {
      evidenceItems: job.evidence.length,
      toolCalls: job.toolTrace.length,
      referencedEvidenceIds: referenced.size,
      gaps: job.gaps.length,
    },
  };
}

function exportJobMarkdown(id) {
  const job = jobs.get(id);
  if (!job || !job.memo) {
    return null;
  }
  return job.memo.markdown;
}

function exportJobPacket(id) {
  const job = jobs.get(id);
  if (!job || !job.packet) {
    return null;
  }
  return publicJob(job).packet;
}

module.exports = {
  AGENT_MODES,
  auditMemo,
  approveJob,
  buildDraftMemo,
  buildGapAnalysis,
  buildRemoteQuery,
  buildResearchPlan,
  buildSourcePacket,
  createJob,
  dedupeEvidence,
  exportJobMarkdown,
  exportJobPacket,
  extractStatuteRefs,
  getJob,
  listJobs,
  normalizeEvidence,
  normalizeEvidenceBundle,
  publicJob,
  rankEvidence,
  runResearchJob,
};
