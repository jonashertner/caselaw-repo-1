"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");

const {
  AGENT_MODES,
  auditMemo,
  buildDraftMemo,
  buildResearchPlan,
  buildRemoteQuery,
  dedupeEvidence,
  extractStatuteRefs,
  normalizeEvidenceBundle,
  runResearchJob,
} = require("../src/agent-runtime");
const { extractRemoteQuery, redactStructuralPii } = require("../src/redact");

test("redacts structural PII before remote query construction", () => {
  const redacted = redactStructuralPii("Email max@example.ch, IBAN CH93 0076 2011 6238 5295 7, phone +41 79 123 45 67.");
  assert.deepEqual(redacted.patterns.sort(), ["email", "iban", "phone"]);
  assert(!redacted.redacted.includes("max@example.ch"));
  assert(!redacted.redacted.includes("+41 79 123 45 67"));

  const query = extractRemoteQuery({
    objective: "Research Art. 336c OR for max@example.ch and termination during illness.",
    facts: "Private facts should not be required for remote retrieval.",
  });
  assert(query.includes("336c"));
  assert(query.includes("Kündigung"));
  assert(!query.includes("@"));
});

test("builds an approval-gated multi-role plan", () => {
  const plan = buildResearchPlan({
    objective: "Find leading cases on Art. 336c OR.",
    jurisdiction: "federal",
    language: "de",
  });
  assert.equal(plan.approvalRequired, true);
  assert.equal(plan.agentType, "source_packet");
  assert.equal(plan.agentLabel, AGENT_MODES.source_packet.label);
  assert.equal(plan.jurisdiction.canton, "CH");
  assert.equal(plan.remoteQuery.includes("Find"), false);
  assert(plan.remoteQuery.includes("336c"));
  assert(plan.tasks.some((task) => task.role === "Adversarial critic"));
  assert(plan.tasks.some((task) => task.role === "Citation auditor"));
});

test("counter-authority mode changes the adversarial plan", () => {
  const plan = buildResearchPlan({
    agentType: "counter_authority",
    objective: "Find weaknesses in an Art. 336c OR position.",
    jurisdiction: "federal",
    language: "de",
  });
  const criticTask = plan.tasks.find((task) => task.id === "counter-authority");
  assert.equal(plan.agentType, "counter_authority");
  assert.match(criticTask.action, /adversarial searches first/);
});

test("prioritizes detected statute references in remote queries", () => {
  const query = buildRemoteQuery(
    { objective: "Find leading Swiss cases on Art. 336c OR and termination during illness." },
    [{ article: "336c", lawCode: "OR", srNumber: "220", label: "Art. 336c OR" }],
  );
  assert.equal(query, "336c Kündigung Krankheit Sperrfrist");
  assert(query.includes("Krankheit"));
});

test("extracts common Swiss statute references", () => {
  const refs = extractStatuteRefs("Analyze Art. 336c OR and Art. 8 BV.");
  assert.deepEqual(refs.map((ref) => ref.label), ["Art. 336c OR", "Art. 8 BV"]);
  assert.equal(refs[0].srNumber, "220");
});

test("normalizes and deduplicates evidence", () => {
  const bundle = {
    name: "search_decisions",
    payload: {
      results: [
        {
          decision_id: "bger_4A_1_2024",
          citation_string_de: "BGer 4A_1/2024",
          canonical_url: "https://example.test/1",
          snippet: "<mark>Kündigung</mark> während Krankheit",
        },
        {
          decision_id: "bger_4A_1_2024",
          citation_string_de: "BGer 4A_1/2024",
          canonical_url: "https://example.test/1",
        },
      ],
    },
  };
  const items = dedupeEvidence(normalizeEvidenceBundle(bundle));
  assert.equal(items.length, 1);
  assert.equal(items[0].type, "case");
  assert.equal(items[0].snippet, "Kündigung während Krankheit");
});

test("classifies article-history court decisions as case-law evidence", () => {
  const items = normalizeEvidenceBundle({
    name: "get_article_history",
    payload: {
      timeline: [
        {
          kind: "court_decision",
          decision_id: "bge_BGE_119_II_449",
          bge_ref: "BGE 119 II 449",
          regeste: "Arbeitsvertrag; Krankheit des Arbeitnehmers nach der Kündigung.",
        },
      ],
    },
  });
  assert.equal(items[0].type, "leading_case");
  assert.equal(items[0].citation, "BGE 119 II 449");
  assert.equal(items[0].sourceUrl, "https://opencaselaw.ch/entscheid/bge_BGE_119_II_449");
});

test("runs the agent job with a fake OpenCaseLaw client", async () => {
  const job = {
    id: "job_test",
    status: "running",
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    tenantId: "test",
    matterId: "matter-test",
    objective: "Research termination during illness.",
    agentType: "source_packet",
    agentLabel: AGENT_MODES.source_packet.label,
    localFactsPreview: "",
    constraints: {
      jurisdiction: "federal",
      language: "de",
      dateFrom: "",
      dateTo: "",
      outputType: "research_memo",
      riskLevel: "standard",
      agentType: "source_packet",
    },
    plan: buildResearchPlan({
      objective: "Research termination during illness under Art. 336c OR.",
      jurisdiction: "federal",
      language: "de",
    }),
    stages: [
      "intake", "plan", "approval", "retrieval", "evidence", "counter", "draft", "audit", "final",
    ].map((id) => ({ id, label: id, status: "pending", notes: [] })),
    toolTrace: [],
    evidence: [],
    sourceMap: null,
    gaps: [],
    memo: null,
    packet: null,
    audit: null,
    errors: [],
  };

  const fakeClient = {
    searchDecisions: async () => ({
      results: [
        {
          decision_id: "bger_4A_1_2024",
          citation_string_de: "BGer 4A_1/2024",
          canonical_url: "https://opencaselaw.ch/entscheid/bger_4A_1_2024",
          snippet: "Sperrfrist und Kündigung während Krankheit.",
        },
      ],
    }),
    findLeadingCases: async () => ({
      results: [
        {
          decision_id: "bge_132_III_1",
          citation_string_de: "BGE 132 III 1",
          canonical_url: "https://opencaselaw.ch/entscheid/bge_132_III_1",
          snippet: "Leitentscheid zur Kündigung.",
          citation_count: 12,
        },
      ],
    }),
    searchLaws: async () => ({
      results: [
        {
          title: "Art. 336c OR",
          citation: "Art. 336c OR",
          url: "https://www.fedlex.admin.ch/",
          text: "Kündigung zur Unzeit.",
        },
      ],
    }),
    searchLegislation: async () => ({ results: [] }),
    searchBotschaft: async () => ({ results: [] }),
    searchScholarship: async () => ({ results: [] }),
    getArticleHistory: async () => ({
      timeline: [
        {
          kind: "court_decision",
          decision_id: "bge_BGE_119_II_449",
          bge_ref: "BGE 119 II 449",
          regeste: "Krankheit des Arbeitnehmers nach der Kündigung.",
        },
      ],
    }),
  };

  const completed = await runResearchJob(job, { client: fakeClient });
  assert.equal(completed.status, "completed");
  assert(completed.evidence.length >= 3);
  assert(completed.memo.markdown.includes("Evidence Ledger"));
  assert.equal(completed.packet.schemaVersion, "ocl.agentic-access.source-packet.v1");
  assert.equal(completed.packet.assurances.citationsConstructedByModel, false);
  assert.equal(completed.packet.sourceMap.counts.leadingCases >= 1, true);
  assert.equal(completed.audit.ok, true);
});

test("memo audit rejects missing evidence ledger", () => {
  const job = {
    evidence: [],
    memo: buildDraftMemo({
      matterId: "empty",
      objective: "No evidence.",
      plan: { tasks: [] },
      sourceMap: { total: 0, leadingCases: [], cases: [], statutes: [], materials: [], scholarship: [], counterAuthority: [] },
      gaps: [],
      evidence: [],
    }),
    gaps: [],
    toolTrace: [],
  };
  const audit = auditMemo(job);
  assert.equal(audit.ok, false);
});
