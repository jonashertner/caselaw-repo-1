/**
 * Tests for api.js transport behaviour — retry and timeout.
 *
 * The module header promised "retry, rate limiting" and neither existed:
 * nothing retried anything, and fetch() has no default timeout, so a
 * stalled request left the task pane waiting forever with no way back.
 * Server-side tools genuinely run for tens of seconds, so users hit this.
 *
 * A GET is safe to repeat. A POST is not — it bills a Pro call and may
 * already have been applied server-side — so only GETs retry. That
 * asymmetry is the main thing these tests hold in place.
 *
 * Run with: node tests/api.test.js
 */
'use strict';

const assert = require('assert');
const fs = require('fs');
const path = require('path');
const vm = require('vm');

const SRC = fs.readFileSync(path.join(__dirname, '..', 'js', 'api.js'), 'utf8');

// `extra` injects the abort primitives. Omitting them models an older
// Office webview that has neither — which is a real target, and the case
// where a signal-only timeout silently does nothing.
function loadApi(fetchImpl, extra) {
  const ctx = {
    fetch: fetchImpl,
    URL, TextEncoder, setTimeout, clearTimeout, console,
    JSON, Object, Array, String, Number, parseInt, Date, Promise, Error,
    crypto: { getRandomValues: (b) => b, subtle: null, randomUUID: () => 'x' },
    localStorage: { getItem: () => null, setItem: () => {} },
    window: undefined,
  };
  Object.assign(ctx, extra || {});
  ctx.globalThis = ctx;
  vm.createContext(ctx);
  vm.runInContext(SRC, ctx);
  return ctx;
}

const res = (status, body) => ({
  ok: status >= 200 && status < 300,
  status,
  statusText: 'x',
  headers: { get: () => null },
  json: async () => body || {},
});

let failures = 0;
function check(name, fn) {
  return Promise.resolve()
    .then(fn)
    .then(() => console.log('  ok   ' + name))
    .catch((e) => {
      failures += 1;
      console.log('  FAIL ' + name + ' — ' + (e && e.message));
    });
}

async function main() {
  await check('a GET retries a transient 503, then succeeds', async () => {
    let calls = 0;
    const api = loadApi(async () => {
      calls += 1;
      return calls < 3 ? res(503) : res(200, { ok: true });
    });
    const out = await api.apiFetch('/decisions', { query: 'x' });
    assert.deepStrictEqual(out, { ok: true });
    assert.strictEqual(calls, 3, 'expected two retries, got ' + (calls - 1));
  });

  await check('a GET gives up after three attempts', async () => {
    let calls = 0;
    const api = loadApi(async () => { calls += 1; return res(503); });
    await assert.rejects(() => api.apiFetch('/decisions', {}),
      (e) => e.type === 'http_error' && e.status === 503);
    assert.strictEqual(calls, 3, 'attempts must be bounded, got ' + calls);
  });

  await check('a 404 is an answer, not a blip, so it is not retried', async () => {
    let calls = 0;
    const api = loadApi(async () => { calls += 1; return res(404); });
    await assert.rejects(() => api.apiFetch('/decisions', {}));
    assert.strictEqual(calls, 1, '4xx retried ' + calls + ' times');
  });

  await check('429 surfaces as rate_limit and is not retried', async () => {
    let calls = 0;
    const api = loadApi(async () => {
      calls += 1;
      return { ok: false, status: 429, statusText: 'x',
        headers: { get: (h) => (h === 'Retry-After' ? '12' : null) },
        json: async () => ({}) };
    });
    await assert.rejects(() => api.apiFetch('/decisions', {}),
      (e) => e.type === 'rate_limit' && e.retryAfter === 12);
    assert.strictEqual(calls, 1);
  });

  await check('a POST is never retried — it may already have been applied',
    async () => {
      let calls = 0;
      const api = loadApi(async () => { calls += 1; return res(503); });
      await assert.rejects(() => api.apiPost('/attest', { a: 1 }));
      assert.strictEqual(calls, 1, 'a Pro call was repeated ' + calls + ' times');
    });

  await check('a network failure is reported, not swallowed', async () => {
    const api = loadApi(async () => { throw new Error('boom'); });
    await assert.rejects(() => api.apiFetch('/decisions', {}),
      (e) => e.type === 'network_error');
  });

  // The webview HAS AbortController — the normal path.
  await check('a hung request aborts instead of waiting forever', async () => {
    const api = loadApi((_u, opts) => new Promise((_res, rej) => {
      opts.signal.addEventListener('abort', () => {
        const e = new Error('aborted');
        e.name = 'AbortError';
        rej(e);
      });
    }), { AbortController, AbortSignal });
    api.REQUEST_TIMEOUT_MS = 40;
    const started = Date.now();
    await assert.rejects(() => api._fetchWithTimeout('https://x/y', {}),
      (e) => e.type === 'timeout');
    assert.ok(Date.now() - started < 4000, 'did not abort promptly');
  });

  // The webview has NO AbortController. The request cannot be cancelled,
  // so the timeout can only come from racing a timer — and without that
  // race the caller waits forever with no error, which is how this very
  // test file first exited silently.
  await check('the timeout still fires where AbortController is absent',
    async () => {
      const api = loadApi(() => new Promise(() => {}));   // never settles
      api.REQUEST_TIMEOUT_MS = 40;
      const started = Date.now();
      await assert.rejects(() => api._fetchWithTimeout('https://x/y', {}),
        (e) => e.type === 'timeout');
      assert.ok(Date.now() - started < 4000, 'unbounded wait');
    });

  if (failures) {
    console.error('api.test.js: ' + failures + ' failure(s)');
    process.exit(1);
  }
  console.log('api.test.js: all assertions passed');
}

main().catch((e) => { console.error('api.test.js crashed:', e); process.exit(1); });
