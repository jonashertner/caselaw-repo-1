# OpenCaseLaw Agentic Access MVP

Standalone MVP for agentic access to Swiss case law and legislation, built on the public OpenCaseLaw API.

This is intentionally separate from the production OpenCaseLaw service. It owns job state locally and treats OpenCaseLaw as the read-only legal retrieval substrate.

## What It Does

- Creates structured source-packet jobs, not chat sessions.
- Supports multiple agent modes: Source Packet, Authority Map, Legislation Trace, Counter-Authority, and Draft Verification.
- Builds an approval-gated research plan.
- Runs parallel retrieval across case law, leading cases, statutes, legislation, Botschaften, scholarship, and article history when statute references are detected.
- Stores a local evidence ledger with source ids.
- Runs a counter-authority probe.
- Drafts a research memo packet only from the evidence ledger.
- Runs an MVP critic pass for traceability, coverage gaps, and advice-like language.
- Exports the memo packet as Markdown.
- Exposes a versioned `ocl.agentic-access.source-packet.v1` JSON artifact for downstream agents.

The core product object is the source packet: plan, trace, evidence ledger, grouped authorities, audit result, gaps, and export links.

## Run

```bash
npm start
```

Default URL:

```text
http://127.0.0.1:3200
```

Optional environment variables:

```bash
PORT=3201
OCL_API_BASE=https://mcp.opencaselaw.ch/api
OCL_TIMEOUT_MS=18000
OCL_REMOTE_ATTEST=1
```

`OCL_REMOTE_ATTEST=1` asks the public citation attestation endpoint to audit generated memo text. It is off by default because the MVP already runs a local traceability audit and remote attestation may have quota/cost constraints.

## Test

```bash
npm test
```

The tests use a fake OpenCaseLaw client and do not require network access.

## API

```bash
curl -s http://127.0.0.1:3200/api/agent-tools
curl -s -X POST http://127.0.0.1:3200/api/jobs \
  -H 'content-type: application/json' \
  --data '{"agentType":"source_packet","objective":"Find leading Swiss cases on Art. 336c OR and termination during illness.","jurisdiction":"federal","language":"de"}'
curl -s -X POST http://127.0.0.1:3200/api/jobs/job_0001 \
  -H 'content-type: application/json' \
  --data '{"approvedBy":"local-user"}'
curl -s http://127.0.0.1:3200/api/jobs/job_0001/packet.json
```

## Deployment Shape

For a commercial product, keep this as a separate agent runtime and workbench:

- Public OpenCaseLaw API/MCP remains open infrastructure.
- Paid Agentic Access owns tenants, matters, jobs, source packets, evidence, exports, billing, model routing, and audit logs.
- Long-running research should remain asynchronous jobs with visible stages and human approval gates.

The evidence ledger is the core commercial artifact. The memo is only one export of that ledger.
