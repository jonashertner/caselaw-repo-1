"use strict";

let selectedJobId = null;
let pollHandle = null;

const els = {
  form: document.querySelector("#job-form"),
  jobList: document.querySelector("#job-list"),
  refreshJobs: document.querySelector("#refresh-jobs"),
  approvePlan: document.querySelector("#approve-plan"),
  exportLink: document.querySelector("#export-link"),
  exportJsonLink: document.querySelector("#export-json-link"),
  jobTitle: document.querySelector("#job-title"),
  statusStrip: document.querySelector("#status-strip"),
  planView: document.querySelector("#plan-view"),
  traceView: document.querySelector("#trace-view"),
  evidenceView: document.querySelector("#evidence-view"),
  memoView: document.querySelector("#memo-view"),
  packetView: document.querySelector("#packet-view"),
  jobStatus: document.querySelector("#job-status"),
  evidenceCount: document.querySelector("#evidence-count"),
  auditBadge: document.querySelector("#audit-badge"),
  packetBadge: document.querySelector("#packet-badge"),
  privacyBadge: document.querySelector("#privacy-badge"),
};

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: {
      "content-type": "application/json",
      ...(options.headers || {}),
    },
    ...options,
  });
  const text = await response.text();
  const payload = text ? JSON.parse(text) : {};
  if (!response.ok) {
    throw new Error(payload.message || payload.error || response.statusText);
  }
  return payload;
}

function escapeHtml(value = "") {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function statusLabel(status) {
  return String(status || "unknown").replaceAll("_", " ");
}

function setPolling(active) {
  if (pollHandle) {
    clearInterval(pollHandle);
    pollHandle = null;
  }
  if (active && selectedJobId) {
    pollHandle = setInterval(() => loadJob(selectedJobId), 1500);
  }
}

async function loadJobs() {
  const { jobs } = await api("/api/jobs");
  els.jobList.innerHTML = jobs.length
    ? jobs.map((job) => `
      <button class="job-item ${job.id === selectedJobId ? "active" : ""}" data-job-id="${job.id}">
        <strong>${escapeHtml(job.matterId)} · ${escapeHtml(job.status)}</strong>
        <span class="job-meta">${escapeHtml(job.agentLabel || job.agentType || "Agent")}</span>
        <span class="job-meta">${escapeHtml(job.objective.slice(0, 92))}${job.objective.length > 92 ? "..." : ""}</span>
      </button>
    `).join("")
    : `<div class="empty-state">No jobs yet.</div>`;
}

async function loadJob(jobId) {
  const { job } = await api(`/api/jobs/${jobId}`);
  selectedJobId = job.id;
  renderJob(job);
  await loadJobs();
  setPolling(job.status === "running");
}

function renderStages(job) {
  els.statusStrip.innerHTML = job.stages.map((stage) => `
    <div class="stage ${escapeHtml(stage.status)}">
      <span class="stage-label">${escapeHtml(stage.label)}</span>
      <span class="stage-status">${escapeHtml(statusLabel(stage.status))}</span>
    </div>
  `).join("");
}

function renderPlan(job) {
  if (!job.plan) {
    els.planView.innerHTML = `<div class="empty-state">No plan available.</div>`;
    return;
  }
  const gaps = job.gaps && job.gaps.length
    ? `<div class="task-list">${job.gaps.map((gap) => `
        <div class="gap-row">
          <span class="task-role">${escapeHtml(gap.severity.toUpperCase())}</span>
          <div class="task-action">${escapeHtml(gap.message)}</div>
        </div>
      `).join("")}</div>`
    : "";

  els.planView.innerHTML = `
    <div class="task-list">
      <div class="task-row">
        <span class="task-role">${escapeHtml(job.plan.agentLabel || "Agent")} · Remote query</span>
        <div class="task-action">${escapeHtml(job.plan.remoteQuery)}</div>
        <div class="trace-detail">${escapeHtml(job.plan.expectedOutput || "")}</div>
      </div>
      ${job.plan.tasks.map((task) => `
        <div class="task-row">
          <span class="task-role">${escapeHtml(task.role)}</span>
          <div class="task-action">${escapeHtml(task.action)}</div>
          ${task.tool ? `<div class="trace-detail">${escapeHtml(task.tool)}</div>` : ""}
        </div>
      `).join("")}
      ${gaps}
    </div>
  `;
  els.privacyBadge.textContent = job.plan.piiPatternsDetected.length
    ? `Redacted: ${job.plan.piiPatternsDetected.join(", ")}`
    : "Minimal remote terms";
}

function sourceCount(packet, key) {
  return packet && packet.sourceMap && packet.sourceMap.counts ? packet.sourceMap.counts[key] || 0 : 0;
}

function renderPacket(job) {
  if (!job.packet) {
    els.packetBadge.textContent = "Not ready";
    els.packetView.innerHTML = `<div class="empty-state">The packet JSON becomes available after retrieval, drafting, and audit.</div>`;
    return;
  }

  const packet = job.packet;
  els.packetBadge.textContent = packet.audit && packet.audit.ok ? "Ready" : "Review";
  const gaps = packet.gaps && packet.gaps.length
    ? packet.gaps.map((gap) => `<li>${escapeHtml(gap.severity.toUpperCase())}: ${escapeHtml(gap.message)}</li>`).join("")
    : "<li>No automatic gaps detected.</li>";

  els.packetView.innerHTML = `
    <div class="packet-grid">
      <div class="metric-tile">
        <span>${sourceCount(packet, "leadingCases")}</span>
        <label>Leading cases</label>
      </div>
      <div class="metric-tile">
        <span>${sourceCount(packet, "statutes")}</span>
        <label>Statutes</label>
      </div>
      <div class="metric-tile">
        <span>${sourceCount(packet, "materials")}</span>
        <label>Materials</label>
      </div>
      <div class="metric-tile">
        <span>${packet.evidenceLedger.length}</span>
        <label>Evidence IDs</label>
      </div>
    </div>
    <div class="packet-section">
      <span class="task-role">${escapeHtml(packet.agent.label)}</span>
      <div class="task-action">${escapeHtml(packet.agent.modeObjective)}</div>
    </div>
    <div class="packet-section">
      <span class="task-role">Assurances</span>
      <div class="assurance-list">
        <span>Human plan approval</span>
        <span>Evidence-led drafting</span>
        <span>No model-built citations</span>
        <span>Source packet JSON</span>
      </div>
    </div>
    <div class="packet-section">
      <span class="task-role">Gaps</span>
      <ul class="gap-list">${gaps}</ul>
    </div>
  `;
}

function renderTrace(job) {
  els.traceView.innerHTML = job.toolTrace.length
    ? job.toolTrace.map((trace) => `
      <div class="trace-row">
        <span class="trace-name">${escapeHtml(trace.name)} · ${escapeHtml(trace.status)}</span>
        <div class="trace-detail">Results: ${trace.resultCount ?? "-"} · ${trace.durationMs ?? "-"} ms</div>
        ${trace.error ? `<div class="trace-detail">Error: ${escapeHtml(trace.error)}</div>` : ""}
      </div>
    `).join("")
    : `<div class="empty-state">Tool calls appear here after plan approval.</div>`;
}

function renderEvidence(job) {
  els.evidenceCount.textContent = String(job.evidence.length);
  els.evidenceView.innerHTML = job.evidence.length
    ? job.evidence.slice(0, 80).map((item) => `
      <div class="evidence-row">
        <div class="evidence-type">${escapeHtml(item.evidenceId)} · ${escapeHtml(item.type)}</div>
        <span class="evidence-title">${escapeHtml(item.citation || item.title)}</span>
        <div class="evidence-detail">${escapeHtml(item.snippet || "No snippet returned.")}</div>
        ${item.sourceUrl ? `<a class="evidence-detail" href="${escapeHtml(item.sourceUrl)}" target="_blank" rel="noreferrer">Open source</a>` : ""}
      </div>
    `).join("")
    : `<div class="empty-state">Evidence will be normalized into a ledger after retrieval.</div>`;
}

function renderMemo(job) {
  els.memoView.textContent = job.memo ? job.memo.markdown : "Final packet will appear here after the critic pass.";
  if (job.audit) {
    els.auditBadge.textContent = job.audit.ok ? "Audit passed" : "Audit warnings";
  } else {
    els.auditBadge.textContent = "Not audited";
  }
}

function renderJob(job) {
  els.jobTitle.textContent = `${job.id}: ${job.objective}`;
  els.jobStatus.textContent = statusLabel(job.status);
  els.approvePlan.disabled = job.status !== "needs_approval";
  els.exportLink.href = job.memo ? `/api/jobs/${job.id}/export.md` : "#";
  els.exportLink.classList.toggle("disabled", !job.memo);
  els.exportLink.setAttribute("aria-disabled", job.memo ? "false" : "true");
  els.exportJsonLink.href = job.packet ? `/api/jobs/${job.id}/packet.json` : "#";
  els.exportJsonLink.classList.toggle("disabled", !job.packet);
  els.exportJsonLink.setAttribute("aria-disabled", job.packet ? "false" : "true");
  renderStages(job);
  renderPlan(job);
  renderTrace(job);
  renderEvidence(job);
  renderPacket(job);
  renderMemo(job);
}

els.form.addEventListener("submit", async (event) => {
  event.preventDefault();
  const formData = new FormData(els.form);
  const body = Object.fromEntries(formData.entries());
  const { job } = await api("/api/jobs", {
    method: "POST",
    body: JSON.stringify(body),
  });
  selectedJobId = job.id;
  renderJob(job);
  await loadJobs();
});

els.approvePlan.addEventListener("click", async () => {
  if (!selectedJobId) {
    return;
  }
  const { job } = await api(`/api/jobs/${selectedJobId}`, {
    method: "POST",
    body: JSON.stringify({ approvedBy: "local-user" }),
  });
  renderJob(job);
  setPolling(true);
});

els.refreshJobs.addEventListener("click", async () => {
  await loadJobs();
  if (selectedJobId) {
    await loadJob(selectedJobId);
  }
});

els.jobList.addEventListener("click", async (event) => {
  const button = event.target.closest("[data-job-id]");
  if (!button) {
    return;
  }
  await loadJob(button.dataset.jobId);
});

loadJobs().catch((error) => {
  els.jobList.innerHTML = `<div class="empty-state">${escapeHtml(error.message)}</div>`;
});
