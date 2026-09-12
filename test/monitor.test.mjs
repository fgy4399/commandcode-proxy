import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { setTimeout as delay } from 'node:timers/promises';

const root = new URL('../', import.meta.url);
const usage = { inputTokens: 120, outputTokens: 24, cachedInputTokens: 80, inputTokenDetails: { cacheWriteTokens: 12 }, outputTokenDetails: { reasoningTokens: 10 } };
const listen = async server => { server.listen(0, '127.0.0.1'); await once(server, 'listening'); return server.address().port; };
async function eventually(fn) {
  let error;
  for (let i = 0; i < 100; i++) {
    try { return await fn(); } catch (e) { error = e; await delay(20); }
  }
  throw error;
}

async function fixture(t, extraEnv = {}) {
  const forwarded = [];
  const upstream = http.createServer(async (req, res) => {
    let text = '';
    for await (const chunk of req) text += chunk;
    if (req.url !== '/alpha/generate') { res.end('{}'); return; }
    const params = JSON.parse(text).params;
    forwarded.push(params);
    const model = params.model;
    if (model === 'http-error') { res.writeHead(503); res.end('secret-upstream-response'); return; }
    res.setHeader('Content-Type', 'application/x-ndjson');
    const event = value => res.write(JSON.stringify(value) + '\n');
    if (model === 'hold') { event({ type: 'start' }); return; }
    if (model === 'finish-tail' || model === 'step-error') {
      res.end([
        {type: 'reasoning-delta', text: 'thinking'},
        ...(model === 'step-error' ? [{type: 'finish-step', finishReason: 'error'}] : []),
        {type: 'finish', finishReason: 'stop', totalUsage: usage},
        ...(model === 'finish-tail' ? [{type: 'error', message: 'ignored after finish'}, {type: 'text-delta', text: 'must-not-forward'}] : []),
      ].map(value => JSON.stringify(value)).join('\n') + '\n');
      return;
    }
    if (['long-reasoning-no-usage', 'reasoning-truncated', 'step-truncated', 'finish-open', 'empty-finished', 'partial-usage', 'reasoning-zero'].includes(model)) {
      if (model !== 'empty-finished') event({ type: 'reasoning-delta', text: 'private-thinking-content' });
      if (model === 'long-reasoning-no-usage') {
        for (let i = 0; i < 8; i++) { await delay(40); event({ type: 'reasoning-delta', text: 'continuing thinking' }); }
      }
      if (model === 'step-truncated') event({ type: 'finish-step', usage });
      if (model === 'reasoning-truncated' || model === 'step-truncated') { res.end(); return; }
      event({ type: 'finish', finishReason: model === 'long-reasoning-no-usage' ? 'length' : 'stop',
        ...(model === 'partial-usage' ? { totalUsage: {inputTokens: 120} } : {}),
        ...(model === 'reasoning-zero' ? { totalUsage: {...usage, outputTokens: 0} } : {}) });
      if (model !== 'finish-open') res.end();
      return;
    }
    event({ type: 'text-delta', text: 'private-response-content' });
    if (model === 'stream-error') { event({ type: 'error', message: '<429> secret-upstream-response' }); res.end(); return; }
    if (model === 'timeout') return;
    if (model === 'unreported') { event({ type: 'finish', finishReason: 'stop' }); res.end(); return; }
    if (model === 'zero') { event({ type: 'finish', totalUsage: { ...usage, outputTokens: 0 } }); res.end(); return; }
    event({ type: 'finish-step', usage: { inputTokens: 30, outputTokens: 6, cachedInputTokens: 20 } });
    // Final total replaces the step, and has no trailing newline.
    const finalUsage = model === 'cache-details'
      ? { inputTokens: 120, outputTokens: 24, inputTokenDetails: { cacheReadTokens: 80, cacheWriteTokens: 12 } }
      : usage;
    res.end(JSON.stringify({ type: 'finish', finishReason: 'stop', totalUsage: finalUsage }));
  });
  const upstreamPort = await listen(upstream);
  const reserve = http.createServer();
  const port = await listen(reserve);
  await new Promise(resolve => reserve.close(resolve));
  // Avoid all external requests, including the startup npm version lookup.
  const entry = new URL('proxy.mjs', root).href;
  const bootstrap = `const originalFetch = globalThis.fetch; globalThis.fetch = (url, options) => String(url).startsWith('http://127.0.0.1:') ? originalFetch(url, options) : Promise.resolve(new Response('{"version":"0.32.3"}', {status:200})); import(${JSON.stringify(entry)});`;
  const child = spawn(process.execPath, ['--input-type=module', '-e', bootstrap], {
    cwd: root,
    env: { ...process.env, PORT: String(port), HOST: '127.0.0.1', CC_API_BASE: `http://127.0.0.1:${upstreamPort}`, CC_USE_PROVIDER_MODELS: 'false', CC_MONITOR_TOKEN: '', CC_MONITOR_DIR: 'off', CC_MONITOR_MAX_RECORDS: '1000', CC_TRUSTED_PROXIES: '', CC_MAX_INFLIGHT: '0', CC_STREAM_IDLE_MS: '200', CC_NONSTREAM_IDLE_MS: '200', LOG_FILE: '', ...extraEnv },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let output = '';
  child.stdout.on('data', data => { output = (output + data).slice(-4000); });
  child.stderr.on('data', data => { output = (output + data).slice(-4000); });
  t.after(async () => {
    if (child.exitCode === null && child.signalCode === null) { child.kill('SIGTERM'); await once(child, 'exit'); }
    upstream.closeAllConnections();
    await new Promise(resolve => upstream.close(resolve));
  });
  const base = `http://127.0.0.1:${port}`;
  await eventually(async () => {
    assert.equal(child.exitCode, null, output);
    assert.equal((await fetch(base + '/health')).status, 200);
  });
  const snapshot = async () => {
    const res = await fetch(base + '/monitor/api/snapshot', { headers: extraEnv.CC_MONITOR_TOKEN ? { Authorization: 'Bearer ' + extraEnv.CC_MONITOR_TOKEN } : {} });
    assert.equal(res.status, 200);
    return res.json();
  };
  const request = async (path, model, stream, options = {}) => {
    const response = await fetch(base + path, { method: 'POST', headers: { 'Content-Type': 'application/json', Authorization: 'Bearer user_test_private_key' }, body: JSON.stringify({ model, stream, max_tokens: 100, messages: [{ role: 'user', content: 'private-prompt-content' }] }), ...options });
    const body = await response.text();
    return { response, body };
  };
  return { base, snapshot, request, forwarded, child };
}

test('monitor captures real proxy requests across both protocols', { timeout: 20000 }, async t => {
  const f = await fixture(t);
  await t.test('monitor and health polling do not create records', async () => {
    const page = await fetch(f.base + '/monitor');
    assert.equal(page.status, 200);
    assert.match(await page.text(), /请求监控/);
    assert.equal(page.headers.get('access-control-allow-origin'), null);
    await fetch(f.base + '/v1/messages', { method: 'OPTIONS' });
    assert.equal((await f.snapshot()).requests.length, 0);
  });
  for (const path of ['/v1/chat/completions', '/v1/messages']) {
    for (const stream of [false, true]) {
      await t.test(`${path} stream=${stream}: raw usage and unterminated final line`, async () => {
        const { response, body } = await f.request(path, 'reported', stream);
        assert.equal(response.status, 200, body);
        const record = (await f.snapshot()).requests[0];
        assert.equal(record.path, path);
        assert.equal(record.stream, stream);
        assert.equal(record.status, 'success');
        assert.equal(record.inputTokens, 120);
        assert.equal(record.outputTokens, 24);
        assert.equal(record.reasoningTokens, 10);
        assert.equal(record.cachedInputTokens, 80);
        assert.equal(record.cacheWriteTokens, 12);
        assert.equal(record.usageReported, true);
        assert.ok(record.endedAt);
        assert.ok(record.durationMs >= 0);
      });
    }
  }
  await t.test('cache detail fallback uses upstream fields', async () => {
    await f.request('/v1/chat/completions', 'cache-details', true);
    assert.equal((await f.snapshot()).requests[0].cachedInputTokens, 80);
  });
  await t.test('missing usage stays unknown, even if Anthropic estimates output', async () => {
    await f.request('/v1/messages', 'unreported', true);
    const r = (await f.snapshot()).requests[0];
    assert.equal(r.usageReported, false);
    assert.equal(r.inputTokens, null);
    assert.equal(r.outputTokens, null);
    assert.equal(r.cachedInputTokens, null);
    assert.equal(r.reasoningTokens, null);
  });
  await t.test('zero-output billing normalization does not erase raw reported input', async () => {
    await f.request('/v1/chat/completions', 'zero', true);
    const r = (await f.snapshot()).requests[0];
    assert.equal(r.status, 'error');
    assert.equal(r.inputTokens, 120);
    assert.equal(r.outputTokens, 0);
    assert.equal(r.cachedInputTokens, 80);
  });
  for (const path of ['/v1/chat/completions', '/v1/messages']) {
    await t.test(`${path}: SSE error is failed despite HTTP 200`, async () => {
      await f.request(path, 'stream-error', true);
      const r = (await f.snapshot()).requests[0];
      assert.equal(r.status, 'error');
      assert.equal(r.httpStatus, 200);
      assert.ok(r.error);
    });
    await t.test(`${path}: upstream timeout is recorded as error`, async () => {
      await f.request(path, 'timeout', true).catch(() => {});
      await eventually(async () => { assert.equal((await f.snapshot()).requests[0].status, 'error'); });
    });
  }
  await t.test('HTTP upstream errors retain status but not upstream text', async () => {
    await f.request('/v1/messages', 'http-error', false);
    const r = (await f.snapshot()).requests[0];
    assert.equal(r.status, 'error');
    assert.equal(r.httpStatus, 503);
    assert.ok(r.error);
  });
  await t.test('validation and auth failures are monitored', async () => {
    await fetch(f.base + '/v1/chat/completions', { method: 'POST', body: '{' });
    assert.equal((await f.snapshot()).requests[0].httpStatus, 400);
    await f.request('/v1/messages', 'unauthorized-model', false, { headers: { 'Content-Type': 'application/json' } });
    const r = (await f.snapshot()).requests[0];
    assert.equal(r.httpStatus, 401);
    assert.equal(r.model, 'unauthorized-model');
  });
  await t.test('active request transitions to aborted without duplicate entries', async () => {
    const controller = new AbortController();
    const pending = f.request('/v1/chat/completions', 'hold', true, { signal: controller.signal }).catch(() => {});
    const active = await eventually(async () => {
      const r = (await f.snapshot()).requests.find(r => r.model === 'hold');
      assert.equal(r?.status, 'pending');
      return r;
    });
    controller.abort();
    await pending;
    await eventually(async () => {
      const records = (await f.snapshot()).requests.filter(r => r.id === active.id);
      assert.equal(records.length, 1);
      assert.equal(records[0].status, 'aborted');
    });
  });
  await t.test('query strings and conversation content never appear in snapshots', async () => {
    await fetch(f.base + '/missing?secret=query-private');
    const snapshot = await f.snapshot();
    assert.equal(snapshot.requests[0].path, '/missing');
    assert.doesNotMatch(JSON.stringify(snapshot), /query-private|private-prompt-content|private-response-content|user_test_private_key|secret-upstream-response/);
  });
});

test('monitor token protects snapshots and records remain bounded', { timeout: 20000 }, async t => {
  const f = await fixture(t, { CC_MONITOR_TOKEN: 'test-monitor-token' });
  assert.equal((await fetch(f.base + '/monitor')).status, 200);
  assert.equal((await fetch(f.base + '/monitor/api/snapshot')).status, 401);
  assert.equal((await fetch(f.base + '/monitor/api/snapshot', { headers: { Authorization: 'Bearer wrong-token' } })).status, 401);
  const response = await fetch(f.base + '/monitor/api/snapshot', { headers: { Authorization: 'Bearer test-monitor-token' } });
  assert.equal(response.status, 200);
  assert.match(response.headers.get('cache-control'), /no-store/);
  assert.equal(response.headers.get('access-control-allow-origin'), null);
  assert.equal((await f.snapshot()).requests.length, 0);
  for (let batch = 0; batch < 21; batch++) {
    await Promise.all(Array.from({ length: 50 }, (_, i) => fetch(f.base + '/missing-' + (batch * 50 + i)).then(r => r.text())));
  }
  const snapshot = await f.snapshot();
  assert.equal(snapshot.retentionLimit, 1000);
  assert.equal(snapshot.requests.length, 1000);
  assert.equal(new Set(snapshot.requests.map(r => r.id)).size, 1000);
});

test('reasoning effort sent upstream matches explicit client values', { timeout: 20000 }, async t => {
  const f = await fixture(t);
  async function send(path, stream, extra, expected, requested = expected) {
    const body = { model: 'reported', stream, messages: [{ role: 'user', content: 'hi' }], ...extra };
    const { response } = await f.request(path, 'reported', stream, { body: JSON.stringify(body) });
    assert.equal(response.status, 200);
    const upstream = f.forwarded.at(-1);
    const record = (await f.snapshot()).requests[0];
    assert.equal(upstream.reasoning_effort, expected);
    assert.equal(Object.hasOwn(upstream, 'reasoning_effort'), expected !== undefined);
    assert.equal(record.reasoningEffort, expected ?? null);
    assert.equal(record.requestedReasoningEffort, requested ?? null);
    return record;
  }
  for (const path of ['/v1/chat/completions', '/v1/messages']) {
    for (const stream of [false, true]) {
      await t.test(`${path} stream=${stream}: explicit efforts are not clamped`, async () => {
        for (const effort of ['low', 'medium', 'high', 'xhigh', 'max', 'hi']) {
          await send(path, stream, { reasoning_effort: effort }, effort);
        }
        await send(path, stream, {}, undefined);
      });
    }
  }
  for (const stream of [false, true]) {
    await t.test(`Anthropic stream=${stream}: effort fields and budget compatibility`, async () => {
      const path = '/v1/messages';
      await send(path, stream, { thinking: { type: 'adaptive' }, output_config: { effort: 'max' } }, 'max');
      await send(path, stream, { thinking: { type: 'adaptive', effort: 'xhigh' } }, 'xhigh');
      await send(path, stream, { thinking: { type: 'adaptive' } }, undefined);
      await send(path, stream, { thinking: { type: 'disabled' } }, undefined);
      await send(path, stream, { reasoning_effort: 'low', output_config: { effort: 'max' }, thinking: { type: 'enabled', effort: 'high', budget_tokens: 16000 } }, 'low');
      await send(path, stream, { output_config: { effort: 'max' }, thinking: { type: 'enabled', effort: 'low', budget_tokens: 16000 } }, 'max');
      for (const [budget, effort] of [[1000, 'low'], [5000, 'medium'], [10000, 'high']]) {
        const record = await send(path, stream, { thinking: { type: 'enabled', budget_tokens: budget } }, effort, null);
        assert.equal(record.thinkingType, 'enabled');
        assert.equal(record.thinkingBudgetTokens, budget);
      }
    });
  }
  await t.test('rejected request has requested effort but no forwarded effort', async () => {
    const count = f.forwarded.length;
    await f.request('/v1/messages', 'reported', false, { headers: {}, body: JSON.stringify({ model: 'reported', reasoning_effort: 'high', messages: [] }) });
    const record = (await f.snapshot()).requests[0];
    assert.equal(record.httpStatus, 401);
    assert.equal(record.requestedReasoningEffort, 'high');
    assert.equal(record.reasoningEffort, null);
    assert.equal(f.forwarded.length, count);
  });
});

test('HTTP key filtering, paginated summaries and filtered export', { timeout: 20000 }, async t => {
  const f = await fixture(t);
  const keyA = 'user_4jGGJKmH8FIRSTxMyDZch';
  const keyB = 'user_4jGGJKmH8SECONDxMyDZch';
  await f.request('/v1/chat/completions', 'reported', false, { headers: { Authorization: 'Bearer ' + keyA } });
  await f.request('/v1/messages', 'reported', true, { headers: { 'x-api-key': keyA } });
  await f.request('/v1/messages', 'other-model', false, { headers: { Authorization: 'Bearer ' + keyB } });
  await fetch(f.base + '/v1/chat/completions', { method: 'POST', headers: { Authorization: 'Bearer ' + keyA }, body: '{' });
  await fetch(f.base + '/v1/models', { headers: { 'x-api-key': keyB } });
  await fetch(f.base + '/unknown-no-key');
  const query = async params => {
    const response = await fetch(f.base + '/monitor/api/query?' + new URLSearchParams(params));
    assert.equal(response.status, 200);
    return response.json();
  };
  const all = await query({pageSize: '1'});
  assert.equal(all.total, 6);
  assert.equal(all.requests.length, 1);
  assert.equal(all.summary.total, 4);
  assert.equal(all.summary.trafficTotal, 6);
  assert.equal(all.summary.excludedTotal, 2);
  assert.equal(all.facets.keys.length, 2);
  assert.ok(all.facets.keys.every(key => key.label === '4jGG****DZch'));
  const snapshot = await f.snapshot();
  const recordA = snapshot.requests.find(r => r.path === '/v1/chat/completions' && r.status === 'success');
  assert.match(recordA.keyId, /^[a-f0-9]{64}$/);
  assert.equal(snapshot.requests.find(r => r.httpStatus === 400).keyId, recordA.keyId);
  const selected = await query({keyId: recordA.keyId, model: 'reported', status: 'success', pageSize: '1'});
  assert.equal(selected.total, 2);
  assert.equal(selected.pages, 2);
  assert.equal(selected.summary.inputTokens, 240);
  assert.equal(selected.summary.outputTokens, 48);
  assert.equal(selected.summary.reasoningTokens, 20);
  assert.equal(selected.summary.cacheRate, 2 / 3);
  assert.equal(selected.trend.reduce((sum, bucket) => sum + bucket.inputTokens, 0), 240);
  const next = await query({keyId: recordA.keyId, model: 'reported', status: 'success', pageSize: '1', page: '2'});
  assert.notEqual(next.requests[0].id, selected.requests[0].id);
  assert.deepEqual(next.summary, selected.summary);
  const exactTime = await query({from: recordA.startedAt, to: recordA.startedAt, keyId: recordA.keyId});
  assert.ok(exactTime.requests.some(r => r.id === recordA.id));
  const withoutKey = await query({keyId: 'none'});
  assert.equal(withoutKey.total, 1);
  assert.equal(withoutKey.requests[0].path, '/unknown-no-key');
  const exported = await (await fetch(f.base + '/monitor/api/export?' + new URLSearchParams({keyId: recordA.keyId, model: 'reported', status: 'success'}))).json();
  assert.equal(exported.requests.length, 2);
  assert.ok(exported.requests.every(r => r.keyId === recordA.keyId));
  const detail = await (await fetch(f.base + '/monitor/api/request?' + new URLSearchParams({id: recordA.id}))).json();
  assert.equal(detail.request.keyLabel, '4jGG****DZch');
  for (const data of [all, snapshot, selected, exported, detail]) {
    assert.ok(!JSON.stringify(data).includes(keyA));
    assert.ok(!JSON.stringify(data).includes(keyB));
    assert.doesNotMatch(JSON.stringify(data), /JKmH8FIRST|JKmH8SECOND/);
  }
  assert.equal((await fetch(f.base + '/monitor/api/query?from=invalid')).status, 400);
  assert.equal((await fetch(f.base + '/monitor/api/query?from=2026-02-02T00%3A00%3A00Z&to=2026-01-01T00%3A00%3A00Z')).status, 400);
});

test('new history endpoints require the monitor token', { timeout: 10000 }, async t => {
  const f = await fixture(t, {CC_MONITOR_TOKEN: 'history-access-token'});
  for (const path of ['/monitor/api/query', '/monitor/api/export', '/monitor/api/request?id=missing']) {
    assert.equal((await fetch(f.base + path)).status, 401);
    const authorized = await fetch(f.base + path, {headers: {Authorization: 'Bearer history-access-token'}});
    assert.equal(authorized.headers.get('access-control-allow-origin'), null);
    assert.match(authorized.headers.get('cache-control'), /no-store/);
    assert.ok([200, 400, 404].includes(authorized.status));
  }
});

test('shutdown has a bounded drain deadline for active streams', { timeout: 10000 }, async t => {
  const f = await fixture(t, {CC_STREAM_IDLE_MS: '20000'});
  const pending = f.request('/v1/chat/completions', 'hold', true).catch(() => {});
  await eventually(async () => { assert.equal((await f.snapshot()).requests[0]?.status, 'pending'); });
  const started = Date.now();
  const exit = once(f.child, 'exit');
  f.child.kill('SIGTERM');
  const [code, signal] = await exit;
  assert.equal(code, 0);
  assert.equal(signal, null);
  assert.ok(Date.now() - started < 8000);
  await pending;
});

test('OpenAI thinking completion distinguishes unknown usage, zero and truncated streams', { timeout: 20000 }, async t => {
  const f = await fixture(t);
  function sseEvents(body) {
    return body.split('\n').filter(line => line.startsWith('data: ') && line !== 'data: [DONE]').map(line => JSON.parse(line.slice(6)));
  }
  for (const stream of [true, false]) {
    for (const model of ['long-reasoning-no-usage', 'unreported', 'partial-usage', 'finish-open']) {
      await t.test(`${model} stream=${stream}: completed content needs no fabricated usage`, async () => {
        const {response, body} = await f.request('/v1/chat/completions', model, stream);
        assert.equal(response.status, 200, body);
        const record = (await f.snapshot()).requests[0];
        assert.equal(record.status, 'success', body);
        assert.equal(record.upstreamFinishReceived, true);
        assert.equal(record.hasUpstreamOutput, true);
        assert.equal(record.outputTokens, null);
        assert.equal(record.lastUpstreamEvent, 'finish');
        if (model === 'partial-usage') assert.equal(record.inputTokens, 120);
        else assert.equal(record.usageReported, false);
        const finishReason = model === 'long-reasoning-no-usage' ? 'length' : 'stop';
        if (stream) {
          assert.match(body, /data: \[DONE\]/);
          const events = sseEvents(body);
          assert.ok(events.every(event => !event.error && !Object.hasOwn(event, 'usage')));
          assert.equal(events.at(-1).choices[0].finish_reason, finishReason);
          if (model === 'long-reasoning-no-usage') assert.equal(events.filter(event => event.choices[0].delta.reasoning_content).length, 9);
        } else {
          const result = JSON.parse(body);
          assert.equal(result.choices[0].finish_reason, finishReason);
          assert.equal(Object.hasOwn(result, 'usage'), false);
          if (model !== 'unreported') assert.match(result.choices[0].message.reasoning_content, /private-thinking-content/);
        }
      });
    }
    for (const model of ['reasoning-truncated', 'step-truncated', 'empty-finished', 'reasoning-zero']) {
      await t.test(`${model} stream=${stream}: report the actual terminal failure`, async () => {
        const {response, body} = await f.request('/v1/chat/completions', model, stream);
        const record = (await f.snapshot()).requests[0];
        const expected = model === 'reasoning-zero' ? 'zero_output' : model === 'empty-finished' ? 'empty_response' : 'upstream_incomplete';
        assert.equal(record.status, 'error');
        assert.equal(record.error, expected, body);
        assert.equal(record.upstreamFinishReceived, ['empty-finished', 'reasoning-zero'].includes(model));
        assert.equal(record.hasUpstreamOutput, model !== 'empty-finished');
        assert.doesNotMatch(body, /data: \[DONE\]/);
        if (stream && response.headers.get('content-type')?.includes('event-stream')) {
          const events = sseEvents(body);
          assert.ok(events.some(event => event.error));
          assert.ok(events.every(event => !event.choices?.[0]?.finish_reason), 'failed stream must not first emit successful finish');
        } else {
          assert.ok(response.status >= 400);
          assert.ok(JSON.parse(body).error);
        }
        if (model === 'reasoning-truncated') {
          assert.equal(record.outputTokens, null);
          assert.equal(record.lastUpstreamEvent, 'reasoning-delta');
        }
      });
    }
  }
});

test('OpenAI terminal boundaries and nested usage are stable across protocols', {timeout: 10000}, async t => {
  const f = await fixture(t);
  for (const stream of [true, false]) {
    for (const model of ['reported', 'cache-details', 'finish-tail', 'step-error']) {
      const {response, body} = await f.request('/v1/chat/completions', model, stream);
      const r = (await f.snapshot()).requests[0];
      if (model === 'step-error') {
        assert.equal(r.status, 'error');
        assert.equal(r.error, 'upstream_error');
        assert.doesNotMatch(body, /data: \[DONE\]/);
        continue;
      }
      assert.equal(response.status, 200);
      assert.equal(r.status, 'success');
      assert.doesNotMatch(body, /must-not-forward|ignored after finish/);
      const result = stream
        ? body.split('\n').filter(line => line.startsWith('data: {')).map(line => JSON.parse(line.slice(6))).find(event => event.usage)
        : JSON.parse(body);
      assert.equal(result.usage.prompt_tokens_details.cached_tokens, 80);
      if (model !== 'cache-details') assert.equal(result.usage.completion_tokens_details.reasoning_tokens, 10);
    }
  }
});

test('HTTP logs capture IP for scans and model failures, with explicit proxy trust', {timeout: 10000}, async t => {
  for (const trusted of [false, true]) {
    const f = await fixture(t, {CC_TRUSTED_PROXIES: trusted ? '127.0.0.1' : ''});
    const headers = {'x-forwarded-for': '198.51.100.99'};
    await fetch(f.base + '/backup.sql', {headers});
    await fetch(f.base + '/v1/messages', {method: 'POST', headers, body: '{'});
    const expectedIp = trusted ? '198.51.100.99' : '127.0.0.1';
    const response = await fetch(f.base + '/monitor/api/query?' + new URLSearchParams({ip: expectedIp}));
    assert.equal(response.status, 200);
    const data = await response.json();
    assert.equal(data.total, 2);
    assert.equal(data.summary.total, 1);
    assert.equal(data.summary.error, 1);
    assert.equal(data.summary.excludedTotal, 1);
    assert.equal(data.summary.failureRate, 1);
    assert.ok(data.requests.every(r => r.clientIp === expectedIp && r.peerIp === '127.0.0.1'));
    const scans = await (await fetch(f.base + '/monitor/api/query?requestKind=other')).json();
    assert.equal(scans.total, 1);
    assert.equal(scans.summary.failureRate, null);
    assert.equal(scans.requests[0].path, '/backup.sql');
    const exported = await (await fetch(f.base + '/monitor/api/export?' + new URLSearchParams({ip: expectedIp, requestKind: 'other'}))).json();
    assert.equal(exported.requests.length, 1);
    assert.equal(exported.requests[0].clientIp, expectedIp);
  }
});

test('admission-rejected model requests retain IP and count as model failures', {timeout: 10000}, async t => {
  const f = await fixture(t, {CC_MAX_INFLIGHT: '1', CC_STREAM_IDLE_MS: '5000'});
  const controller = new AbortController();
  const active = f.request('/v1/chat/completions', 'hold', true, {signal: controller.signal}).catch(() => {});
  try {
    await eventually(async () => assert.equal((await f.snapshot()).requests[0]?.status, 'pending'));
    const rejected = await f.request('/v1/messages', 'reported', false);
    assert.equal(rejected.response.status, 503);
    const data = await (await fetch(f.base + '/monitor/api/query')).json();
    assert.equal(data.requests[0].clientIp, '127.0.0.1');
    assert.equal(data.requests[0].requestKind, 'model');
    assert.equal(data.summary.total, 2);
    assert.equal(data.summary.pending, 1);
    assert.equal(data.summary.error, 1);
    assert.equal(data.summary.failureRate, 1);
  } finally { controller.abort(); await active; }
});
