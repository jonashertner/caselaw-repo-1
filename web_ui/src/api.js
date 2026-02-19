const API_BASE = '/api';

export async function checkHealth() {
  const resp = await fetch(`${API_BASE}/health`);
  return resp.json();
}

export async function getDecision(decisionId) {
  const resp = await fetch(`${API_BASE}/decision/${encodeURIComponent(decisionId)}`);
  return resp.json();
}

export async function getKeyStatus() {
  const resp = await fetch(`${API_BASE}/settings/keys`);
  return resp.json();
}

export async function setApiKey(provider, apiKey) {
  const resp = await fetch(`${API_BASE}/settings/keys`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ provider, api_key: apiKey }),
  });
  return resp.json();
}

export async function removeApiKey(provider) {
  const resp = await fetch(`${API_BASE}/settings/keys/${encodeURIComponent(provider)}`, {
    method: 'DELETE',
  });
  return resp.json();
}

export async function getOllamaStatus() {
  const resp = await fetch(`${API_BASE}/settings/ollama`);
  return resp.json();
}

export async function setOllamaUrl(baseUrl) {
  const resp = await fetch(`${API_BASE}/settings/ollama`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ base_url: baseUrl }),
  });
  return resp.json();
}

export async function searchDecisions({ query, limit = 20 }) {
  const params = new URLSearchParams({ query, limit: String(limit) });
  const resp = await fetch(`${API_BASE}/search?${params}`);
  if (!resp.ok) throw new Error(`Search failed: ${resp.status}`);
  return resp.json();
}

export async function fetchStatuteText(url, article) {
  const params = new URLSearchParams({ url, article });
  const resp = await fetch(`${API_BASE}/statute?${params}`);
  if (!resp.ok) throw new Error(`Statute fetch failed: ${resp.status}`);
  return resp.json();
}

/**
 * Stream chat responses via SSE.
 * @param {object} params - { provider, message, session_id, filters }
 * @param {function} onChunk - Called with each parsed ChatChunk
 * @param {AbortSignal} signal - For cancellation
 */
export async function streamChat({ provider, message, session_id, filters }, onChunk, signal) {
  const resp = await fetch(`${API_BASE}/chat`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ provider, message, session_id, filters }),
    signal,
  });

  if (!resp.ok) {
    const err = await resp.text();
    throw new Error(`Chat failed: ${resp.status} ${err}`);
  }

  const reader = resp.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });

    const lines = buffer.split('\n');
    buffer = lines.pop() || '';

    for (const line of lines) {
      if (line.startsWith('data: ')) {
        try {
          const chunk = JSON.parse(line.slice(6));
          onChunk(chunk);
        } catch {
          // skip malformed
        }
      }
    }
  }
}
