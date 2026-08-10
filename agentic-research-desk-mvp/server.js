#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const http = require("node:http");
const path = require("node:path");
const { URL } = require("node:url");

const {
  AGENT_MODES,
  approveJob,
  createJob,
  exportJobMarkdown,
  exportJobPacket,
  getJob,
  listJobs,
} = require("./src/agent-runtime");

const PORT = Number(process.env.PORT || 3200);
const HOST = process.env.HOST || "127.0.0.1";
const PUBLIC_DIR = path.join(__dirname, "public");

const MIME_TYPES = {
  ".html": "text/html; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".js": "application/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".svg": "image/svg+xml",
  ".txt": "text/plain; charset=utf-8",
  ".md": "text/markdown; charset=utf-8",
};

function sendJson(res, status, payload) {
  res.writeHead(status, {
    "content-type": "application/json; charset=utf-8",
    "cache-control": "no-store",
  });
  res.end(JSON.stringify(payload));
}

function sendText(res, status, body, contentType = "text/plain; charset=utf-8") {
  res.writeHead(status, {
    "content-type": contentType,
    "cache-control": "no-store",
  });
  res.end(body);
}

function parseJsonBody(req) {
  return new Promise((resolve, reject) => {
    let body = "";
    req.on("data", (chunk) => {
      body += chunk;
      if (body.length > 1_000_000) {
        reject(new Error("Request body too large"));
        req.destroy();
      }
    });
    req.on("end", () => {
      if (!body.trim()) {
        resolve({});
        return;
      }
      try {
        resolve(JSON.parse(body));
      } catch (error) {
        reject(new Error(`Invalid JSON: ${error.message}`));
      }
    });
    req.on("error", reject);
  });
}

function readStaticFile(pathname) {
  const decoded = decodeURIComponent(pathname);
  const safePath = path.normalize(decoded).replace(/^(\.\.[/\\])+/, "");
  const requested = safePath === "/" ? "/index.html" : safePath;
  const filePath = path.join(PUBLIC_DIR, requested);
  if (!filePath.startsWith(PUBLIC_DIR)) {
    return null;
  }
  if (!fs.existsSync(filePath) || !fs.statSync(filePath).isFile()) {
    return null;
  }
  return filePath;
}

async function handleApi(req, res, url) {
  const pathname = url.pathname;

  if (req.method === "GET" && pathname === "/api/health") {
    sendJson(res, 200, {
      ok: true,
      service: "OpenCaseLaw Agentic Research Desk MVP",
      mode: "local",
      jobs: listJobs().length,
      agentModes: Object.keys(AGENT_MODES),
      openCaseLawBase: process.env.OCL_API_BASE || "https://mcp.opencaselaw.ch/api",
    });
    return;
  }

  if (req.method === "GET" && pathname === "/api/agent-tools") {
    sendJson(res, 200, {
      product: "OpenCaseLaw Agentic Access",
      schemaVersion: "ocl.agentic-access.catalog.v1",
      modes: AGENT_MODES,
      tools: [
        {
          name: "create_source_packet_job",
          method: "POST",
          path: "/api/jobs",
          description: "Create an approval-gated Swiss legal source-packet job.",
        },
        {
          name: "approve_source_packet_plan",
          method: "POST",
          path: "/api/jobs/{job_id}",
          description: "Approve the plan and start agentic retrieval.",
        },
        {
          name: "get_source_packet",
          method: "GET",
          path: "/api/jobs/{job_id}/packet.json",
          description: "Fetch the completed versioned source packet for downstream agents.",
        },
      ],
    });
    return;
  }

  if (req.method === "GET" && pathname === "/api/jobs") {
    sendJson(res, 200, { jobs: listJobs() });
    return;
  }

  if (req.method === "POST" && pathname === "/api/jobs") {
    const body = await parseJsonBody(req);
    const job = createJob(body);
    sendJson(res, 201, { job });
    return;
  }

  const jobMatch = pathname.match(/^\/api\/jobs\/([^/]+)$/);
  if (jobMatch && req.method === "GET") {
    const job = getJob(jobMatch[1]);
    if (!job) {
      sendJson(res, 404, { error: "job_not_found" });
      return;
    }
    sendJson(res, 200, { job });
    return;
  }

  if (jobMatch && req.method === "POST") {
    const body = await parseJsonBody(req);
    const job = approveJob(jobMatch[1], body);
    if (!job) {
      sendJson(res, 404, { error: "job_not_found" });
      return;
    }
    sendJson(res, 202, { job });
    return;
  }

  const exportMatch = pathname.match(/^\/api\/jobs\/([^/]+)\/export\.md$/);
  if (exportMatch && req.method === "GET") {
    const markdown = exportJobMarkdown(exportMatch[1]);
    if (!markdown) {
      sendJson(res, 404, { error: "job_not_found_or_not_ready" });
      return;
    }
    sendText(res, 200, markdown, "text/markdown; charset=utf-8");
    return;
  }

  const packetMatch = pathname.match(/^\/api\/jobs\/([^/]+)\/packet\.json$/);
  if (packetMatch && req.method === "GET") {
    const packet = exportJobPacket(packetMatch[1]);
    if (!packet) {
      sendJson(res, 404, { error: "job_not_found_or_packet_not_ready" });
      return;
    }
    sendJson(res, 200, { packet });
    return;
  }

  sendJson(res, 404, { error: "not_found" });
}

async function handleRequest(req, res) {
  const url = new URL(req.url, `http://${req.headers.host || `${HOST}:${PORT}`}`);
  try {
    if (url.pathname.startsWith("/api/")) {
      await handleApi(req, res, url);
      return;
    }

    const filePath = readStaticFile(url.pathname) || path.join(PUBLIC_DIR, "index.html");
    const ext = path.extname(filePath);
    const contentType = MIME_TYPES[ext] || "application/octet-stream";
    res.writeHead(200, { "content-type": contentType });
    fs.createReadStream(filePath).pipe(res);
  } catch (error) {
    sendJson(res, 500, {
      error: "internal_error",
      message: error.message,
    });
  }
}

const server = http.createServer(handleRequest);

server.listen(PORT, HOST, () => {
  console.log(`OpenCaseLaw Agentic Research Desk MVP: http://${HOST}:${PORT}`);
});
