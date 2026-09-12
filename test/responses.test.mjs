import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';

const root = new URL('../', import.meta.url);
const apiKey = 'user_responses_fixture_private_key';
const usage = {
  inputTokens: 120, outputTokens: 24, cachedInputTokens: 80,
  inputTokenDetails: { cacheWriteTokens: 12 },
  outputTokenDetails: { reasoningTokens: 10 },
};
const answer = 'The answer is 42.';
const thinking = 'Consider the available evidence.';
const schema = { type: 'object', properties: { city: { type: 'string' } }, required: ['city'] };
const tool = { type: 'function', name: 'weather', description: 'Read weather', parameters: schema };

async function listen(server) {
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  return server.address().port;
}

async function eventually(fn) {
  let error;
  for (let i = 0; i < 150; i++) {
    try { return await fn(); } catch (e) { error = e; await delay(20); }
  }
  throw error;
}

async function fixture(t) {
  const forwarded = [];
  const upstream = http.createServer(async (req, res) => {
    let text = '';
    for await (const chunk of req) text += chunk;
    if (req.url !== '/alpha/generate') { res.end('{}'); return; }
    const envelope = JSON.parse(text);
    const { params } = envelope;
    forwarded.push({ params, envelope, headers: { ...req.headers } });
    res.setHeader('Content-Type', 'application/x-ndjson');
    const event = value => res.write(JSON.stringify(value) + '\n');
    event({ type: 'start' });
    event({ type: 'reasoning-delta', text: thinking });
    if (params.model === 'reasoning-eof') { res.end(); return; }
    if (params.model === 'tool') {
      event({ type: 'tool-call', toolCallId: 'call_weather', toolName: 'weather', input: { city: 'Paris' } });
    } else {
      event({ type: 'text-delta', text: 'The answer ' });
      event({ type: 'text-delta', text: 'is 42.' });
    }
    if (params.model === 'explicit-error') {
      // The explicit error must win over the zero-output finish that follows.
      event({ type: 'error', message: '<429> fixture upstream failure' });
    }
    if (params.model === 'step-error') event({ type: 'finish-step', finishReason: 'error' });
    const finish = {
      type: 'finish',
      finishReason: params.model === 'length' ? 'length' : params.model === 'tool' ? 'tool-calls' : 'stop',
      ...(['missing-usage', 'finish-open'].includes(params.model) ? {} : {
        totalUsage: ['zero', 'explicit-error'].includes(params.model) ? { ...usage, outputTokens: 0 } : usage,
      }),
    };
    if (params.model === 'finish-open') { event(finish); return; }
    if (params.model === 'finish-tail') {
      event(finish);
      event({ type: 'error', message: 'ignored after finish' });
      event({ type: 'text-delta', text: 'must-not-forward' });
      res.end();
      return;
    }
    // A final NDJSON record without a newline is a valid terminal record.
    res.end(JSON.stringify(finish));
  });
  const upstreamPort = await listen(upstream);
  const reserve = http.createServer();
  const port = await listen(reserve);
  await new Promise(resolve => reserve.close(resolve));
  const scratch = await mkdtemp(join(process.env.PI_SCRATCH_DIR || tmpdir(), 'responses-http-'));
  const upstreamBase = `http://127.0.0.1:${upstreamPort}`;
  // Permit only the mock upstream. Stub the startup registry lookup and fail any
  // unexpected network request; no provider or real API key is needed.
  const bootstrap = `
    const originalFetch = globalThis.fetch;
    globalThis.fetch = (input, options) => {
      const url = new URL(typeof input === 'string' || input instanceof URL ? input : input.url);
      if (url.origin === ${JSON.stringify(upstreamBase)}) return originalFetch(input, options);
      if (url.hostname === 'registry.npmjs.org') return Promise.resolve(new Response('{"version":"0.32.3"}', {status: 200}));
      return Promise.reject(new Error('Unexpected external fetch: ' + url.origin));
    };
    await import(${JSON.stringify(new URL('proxy.mjs', root).href)});
  `;
  const child = spawn(process.execPath, ['--input-type=module', '-e', bootstrap], {
    cwd: scratch,
    env: {
      ...process.env, PORT: String(port), HOST: '127.0.0.1', CC_API_BASE: upstreamBase,
      CC_USE_PROVIDER_MODELS: 'false', CC_MONITOR_TOKEN: '', CC_MONITOR_DIR: 'off',
      CC_MONITOR_MAX_RECORDS: '1000', CC_TRUSTED_PROXIES: '', CC_MAX_INFLIGHT: '0',
      CC_STREAM_IDLE_MS: '2000', CC_NONSTREAM_IDLE_MS: '2000', LOG_FILE: '',
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let output = '';
  child.stdout.on('data', data => { output = (output + data).slice(-6000); });
  child.stderr.on('data', data => { output = (output + data).slice(-6000); });
  t.after(async () => {
    if (child.exitCode === null && child.signalCode === null) {
      const exited = once(child, 'exit');
      child.kill('SIGTERM');
      const force = setTimeout(() => child.kill('SIGKILL'), 1000);
      try { await exited; } finally { clearTimeout(force); }
    }
    upstream.closeAllConnections();
    await new Promise(resolve => upstream.close(resolve));
    await rm(scratch, { recursive: true, force: true });
  });
  const base = `http://127.0.0.1:${port}`;
  await eventually(async () => {
    assert.equal(child.exitCode, null, output);
    assert.equal((await fetch(base + '/health', { signal: AbortSignal.timeout(500) })).status, 200);
  });
  const get = async path => {
    const response = await fetch(base + path, { signal: AbortSignal.timeout(3000) });
    assert.equal(response.status, 200);
    return response.json();
  };
  const request = async (body, { path = '/v1/responses', headers = {}, auth = true, timeout = 4000 } = {}) => {
    const response = await fetch(base + path, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json', ...(auth ? { Authorization: `Bearer ${apiKey}` } : {}),
        'x-forwarded-for': '198.51.100.99', ...headers,
      },
      body: JSON.stringify(body), signal: AbortSignal.timeout(timeout),
    });
    return { response, body: await response.text() };
  };
  const record = () => eventually(async () => {
    const latest = (await get('/monitor/api/snapshot')).requests[0];
    assert.ok(latest?.endedAt, 'latest request must have finalized');
    return latest;
  });
  return { forwarded, request, record, get };
}

function sseEvents(body) {
  return body.replaceAll('\r\n', '\n').split('\n\n').filter(block => block.trim()).map(block => {
    const lines = block.split('\n');
    const name = lines.find(line => line.startsWith('event:'))?.slice(6).trim();
    const text = lines.filter(line => line.startsWith('data:')).map(line => line.slice(5).trimStart()).join('\n');
    assert.ok(name, `SSE event must be named: ${block}`);
    const data = JSON.parse(text);
    assert.equal(data.type, name);
    return data;
  });
}

function orderedEvents(events) {
  assert.ok(events.length > 0);
  for (let i = 0; i < events.length; i++) {
    assert.ok(Number.isInteger(events[i].sequence_number), 'sequence_number must be an integer');
    if (i) assert.ok(events[i].sequence_number > events[i - 1].sequence_number, 'sequence_number must strictly increase');
  }
}

function completed(result, stream, terminal = 'completed') {
  assert.equal(result.response.status, 200, result.body);
  if (!stream) return JSON.parse(result.body);
  assert.match(result.response.headers.get('content-type'), /text\/event-stream/);
  const events = sseEvents(result.body);
  orderedEvents(events);
  assert.equal(events[0].type, 'response.created');
  assert.equal(events[1].type, 'response.in_progress');
  assert.equal(events.at(-1).type, `response.${terminal}`, result.body);
  assert.equal(events.filter(event => ['response.completed', 'response.incomplete'].includes(event.type)).length, 1);
  const response = events.at(-1).response;
  assert.equal(response.id, events[0].response.id);
  assert.deepEqual(response.output, events.filter(event => event.type === 'response.output_item.done').map(event => event.item));
  return response;
}

function reportedUsage(response) {
  assert.equal(response.usage.input_tokens, 120);
  assert.equal(response.usage.output_tokens, 24);
  assert.equal(response.usage.total_tokens, 144);
  assert.equal(response.usage.input_tokens_details.cached_tokens, 80);
  if (Object.hasOwn(response.usage.input_tokens_details, 'cache_write_tokens')) {
    assert.equal(response.usage.input_tokens_details.cache_write_tokens, 12);
  }
  assert.equal(response.usage.output_tokens_details.reasoning_tokens, 10);
}

function replay(params) {
  const assistant = params.messages.find(message => message.role === 'assistant');
  assert.ok(assistant, JSON.stringify(params.messages));
  assert.deepEqual(assistant.content.map(part => part.type), ['reasoning', 'text', 'tool-call']);
  assert.equal(assistant.content[0].text, 'Historical reasoning');
  assert.equal(assistant.content[1].text, 'Checking weather');
  assert.deepEqual(assistant.content[2], {
    type: 'tool-call', toolCallId: 'call_history', toolName: 'weather', input: { city: 'Paris' },
  });
  const result = params.messages.find(message => message.role === 'tool');
  assert.ok(result);
  assert.ok(params.messages.indexOf(result) > params.messages.indexOf(assistant));
  assert.deepEqual(result.content, [{
    type: 'tool-result', toolCallId: 'call_history', toolName: 'weather', output: { type: 'text', value: 'Sunny' },
  }]);
  assert.equal(params.messages.flatMap(message => message.content).filter(part => part.type === 'reasoning').length, 1);
}

test('Responses HTTP compatibility and terminal semantics', { timeout: 45000 }, async t => {
  const f = await fixture(t);
  for (const stream of [false, true]) {
    await t.test(`string input, instructions, effort and monitor metadata stream=${stream}`, async () => {
      const result = await f.request({
        model: 'reported', input: 'Explain the answer', instructions: 'Be concise',
        reasoning: { effort: 'max' }, max_output_tokens: 128, stream,
      });
      const response = completed(result, stream);
      assert.equal(response.object, 'response');
      assert.equal(response.status, 'completed');
      assert.equal(response.model, 'reported');
      assert.deepEqual(response.output.map(item => item.type), ['reasoning', 'message']);
      assert.equal(response.output[0].summary.map(part => part.text).join(''), thinking);
      assert.equal(response.output[1].content.map(part => part.text).join(''), answer);
      reportedUsage(response);
      const { params, envelope, headers } = f.forwarded.at(-1);
      assert.equal(envelope.params, params);
      assert.equal(params.system, 'Be concise');
      assert.deepEqual(params.messages, [{ role: 'user', content: [{ type: 'text', text: 'Explain the answer' }] }]);
      assert.equal(params.max_tokens, 128);
      assert.equal(params.reasoning_effort, 'max');
      assert.equal(params.stream, true, 'CC always receives NDJSON streaming requests');
      assert.equal(headers.authorization, `Bearer ${apiKey}`);
      const record = await f.record();
      assert.equal(record.path, '/v1/responses');
      assert.equal(record.requestKind, 'model');
      assert.equal(record.model, 'reported');
      assert.equal(record.requestedReasoningEffort, 'max');
      assert.equal(record.reasoningEffort, 'max');
      assert.equal(record.stream, stream);
      assert.equal(record.status, 'success');
      assert.equal(record.clientIp, '127.0.0.1');
      assert.equal(record.peerIp, '127.0.0.1');
      assert.ok(record.keyId);
      assert.ok(record.keyLabel);
      assert.doesNotMatch(JSON.stringify(record), new RegExp(apiKey));
      assert.equal(record.inputTokens, 120);
      assert.equal(record.outputTokens, 24);
      assert.equal(record.cachedInputTokens, 80);
      assert.equal(record.cacheWriteTokens, 12);
      assert.equal(record.reasoningTokens, 10);
      if (stream) {
        const events = sseEvents(result.body);
        for (const name of ['response.output_item.added', 'response.content_part.added', 'response.output_text.delta',
          'response.output_text.done', 'response.content_part.done', 'response.reasoning_summary_text.delta']) {
          assert.ok(events.some(event => event.type === name), `missing ${name}`);
        }
        assert.equal(events.filter(event => event.type === 'response.output_text.delta').map(event => event.delta).join(''), answer);
      }
    });

    await t.test(`length finishes as incomplete stream=${stream}`, async () => {
      const result = await f.request({ model: 'length', input: 'Hi', stream });
      const response = completed(result, stream, 'incomplete');
      assert.equal(response.status, 'incomplete');
      assert.deepEqual(response.incomplete_details, { reason: 'max_output_tokens' });
      assert.equal(response.output.find(item => item.type === 'message').status, 'incomplete');
      assert.doesNotMatch(result.body, /event: response\.completed/);
    });

    await t.test(`valid text finish without usage stays unknown stream=${stream}`, async () => {
      const response = completed(await f.request({ model: 'missing-usage', input: 'Hi', stream }), stream);
      assert.equal(response.status, 'completed');
      assert.ok(response.usage == null, JSON.stringify(response.usage));
      assert.equal(response.output.find(item => item.type === 'message').content[0].text, answer);
      const record = await f.record();
      assert.equal(record.status, 'success');
      assert.equal(record.usageReported, false);
      assert.equal(record.inputTokens, null);
      assert.equal(record.outputTokens, null);
    });

    await t.test(`newline finish completes while upstream HTTP remains open stream=${stream}`, async () => {
      // Client deadline precedes the configured 2s upstream idle watchdog.
      const result = await f.request({ model: 'finish-open', input: 'Hi', stream }, { timeout: 1200 });
      const response = completed(result, stream);
      assert.equal(response.status, 'completed');
      assert.ok(response.usage == null, JSON.stringify(response.usage));
      assert.equal((await f.record()).status, 'success');
    });

    await t.test(`terminal finish ignores trailing upstream events stream=${stream}`, async () => {
      const result = await f.request({ model: 'finish-tail', input: 'Hi', stream });
      assert.equal(completed(result, stream).status, 'completed');
      assert.doesNotMatch(result.body, /must-not-forward|ignored after finish/);
      assert.equal((await f.record()).status, 'success');
    });

    for (const [model, error] of [
      ['reasoning-eof', 'upstream_incomplete'], ['zero', 'zero_output'],
      ['explicit-error', 'upstream_error'], ['step-error', 'upstream_error'],
    ]) {
      await t.test(`${model} fails without successful terminal event stream=${stream}`, async () => {
        const result = await f.request({ model, input: 'Hi', stream });
        assert.doesNotMatch(result.body, /event: response\.(completed|incomplete)/);
        if (result.response.headers.get('content-type')?.includes('event-stream')) {
          const events = sseEvents(result.body);
          orderedEvents(events);
          assert.ok(events.some(event => event.type === 'response.failed' || event.type === 'error'), result.body);
        } else {
          assert.ok(result.response.status >= 400, result.body);
          assert.ok(JSON.parse(result.body).error);
        }
        const record = await f.record();
        assert.equal(record.status, 'error');
        assert.equal(record.error, error, result.body);
        if (model === 'reasoning-eof') {
          assert.equal(record.upstreamFinishReceived, false);
          assert.equal(record.outputTokens, null);
        }
        if (model === 'zero') assert.equal(record.outputTokens, 0);
      });
    }

    await t.test(`function history and tool definitions round trip stream=${stream}`, async () => {
      const result = await f.request({ model: 'tool', stream, tools: [tool], input: [
        { type: 'reasoning', summary: [{ type: 'summary_text', text: 'Historical reasoning' }] },
        { type: 'message', role: 'assistant', content: [{ type: 'output_text', text: 'Checking weather' }] },
        { type: 'function_call', call_id: 'call_history', name: 'weather', arguments: '{"city":"Paris"}' },
        { type: 'function_call_output', call_id: 'call_history', output: 'Sunny' },
      ] });
      const response = completed(result, stream);
      const { params } = f.forwarded.at(-1);
      replay(params);
      assert.deepEqual(params.tools, [{ type: 'function', name: 'weather', description: 'Read weather', input_schema: schema }]);
      const call = response.output.find(item => item.type === 'function_call');
      assert.ok(call, result.body);
      assert.equal(call.call_id, 'call_weather');
      assert.equal(call.name, 'weather');
      assert.equal(call.status, 'completed');
      assert.deepEqual(JSON.parse(call.arguments), { city: 'Paris' });
      if (stream) {
        const events = sseEvents(result.body);
        const deltas = events.filter(event => event.type === 'response.function_call_arguments.delta');
        assert.ok(deltas.length);
        assert.equal(deltas.map(event => event.delta).join(''), call.arguments);
        const done = events.find(event => event.type === 'response.function_call_arguments.done');
        assert.equal(done?.arguments, call.arguments);
        assert.equal(done?.item_id, call.id);
      }
    });
  }

  for (const [name, headers, expected] of [
    ['prompt cache key', {}, 'responses-prompt-cache-session'],
    ['explicit session header', { 'x-session-id': 'responses-explicit-session' }, 'responses-explicit-session'],
  ]) {
    await t.test(`${name} survives CC forwarding`, async () => {
      const result = await f.request({ model: 'reported', input: 'Hi', prompt_cache_key: 'responses-prompt-cache-session' }, { headers });
      assert.equal(result.response.status, 200, result.body);
      assert.equal(f.forwarded.at(-1).headers['x-session-id'], expected);
    });
  }

  const invalid = [
    ['previous_response_id', { previous_response_id: 'resp_prior' }],
    ['stored responses', { store: true }],
    ['built-in web search', { tools: [{ type: 'web_search' }] }],
    ['numeric input', { input: 42 }],
    ['object input', { input: { role: 'user', content: 'Hi' } }],
    ['null input item', { input: [null] }],
    ['numeric message content', { input: [{ type: 'message', role: 'user', content: 42 }] }],
    ['tools object', { tools: {} }],
    ['null tool', { tools: [null] }],
    ['function missing name', { tools: [{ type: 'function', parameters: schema }] }],
  ];
  for (const [name, extra] of invalid) {
    await t.test(`${name} returns 400 and preserves monitor metadata`, async () => {
      const before = f.forwarded.length;
      const result = await f.request({ model: 'validation-model', input: 'Hi', reasoning: { effort: 'max' }, ...extra });
      assert.equal(result.response.status, 400, result.body);
      assert.ok(JSON.parse(result.body).error);
      assert.equal(f.forwarded.length, before, 'invalid requests must not reach upstream');
      const record = await f.record();
      assert.equal(record.path, '/v1/responses');
      assert.equal(record.requestKind, 'model');
      assert.equal(record.model, 'validation-model');
      assert.equal(record.requestedReasoningEffort, 'max');
      assert.equal(record.status, 'error');
      assert.equal(record.clientIp, '127.0.0.1');
      assert.ok(record.keyId);
      assert.ok(record.keyLabel);
    });
  }

  await t.test('auth failure retains requested model, effort and IP', async () => {
    const before = f.forwarded.length;
    const result = await f.request({ model: 'auth-model', input: 'Hi', reasoning: { effort: 'max' } }, { auth: false });
    assert.equal(result.response.status, 401, result.body);
    assert.equal(f.forwarded.length, before);
    const record = await f.record();
    assert.equal(record.requestKind, 'model');
    assert.equal(record.model, 'auth-model');
    assert.equal(record.requestedReasoningEffort, 'max');
    assert.equal(record.clientIp, '127.0.0.1');
    assert.equal(record.keyId, null);
    assert.equal(record.status, 'error');
  });

  await t.test('successful Responses requests count in model summaries', async () => {
    const result = await f.request({ model: 'counted-response', input: 'Hi' });
    assert.equal(result.response.status, 200, result.body);
    const data = await f.get('/monitor/api/query?' + new URLSearchParams({ model: 'counted-response' }));
    assert.equal(data.summary.total, 1);
    assert.equal(data.summary.success, 1);
    assert.equal(data.summary.error, 0);
    assert.equal(data.summary.excludedTotal, 0);
  });

  for (const contentArray of [false, true]) {
    await t.test(`legacy OpenAI reasoning history is forwarded once, content array=${contentArray}`, async () => {
      const result = await f.request({ model: 'reported', messages: [
        { role: 'assistant', reasoning_content: 'Historical reasoning',
          content: contentArray ? [{ type: 'reasoning', text: 'Historical reasoning' }, { type: 'text', text: 'Checking weather' }] : 'Checking weather',
          tool_calls: [{ id: 'call_history', type: 'function', function: { name: 'weather', arguments: '{"city":"Paris"}' } }] },
        { role: 'tool', tool_call_id: 'call_history', content: 'Sunny' },
      ] }, { path: '/v1/chat/completions' });
      assert.equal(result.response.status, 200, result.body);
      replay(f.forwarded.at(-1).params);
    });
  }

  await t.test('legacy Anthropic thinking history is forwarded once', async () => {
    const result = await f.request({ model: 'reported', max_tokens: 128, messages: [
      { role: 'assistant', content: [
        { type: 'thinking', thinking: 'Historical reasoning', signature: 'fixture-signature' },
        { type: 'text', text: 'Checking weather' },
        { type: 'tool_use', id: 'call_history', name: 'weather', input: { city: 'Paris' } },
      ] },
      { role: 'user', content: [{ type: 'tool_result', tool_use_id: 'call_history', content: 'Sunny' }] },
    ] }, { path: '/v1/messages', auth: false, headers: { 'x-api-key': apiKey, 'anthropic-version': '2023-06-01' } });
    assert.equal(result.response.status, 200, result.body);
    replay(f.forwarded.at(-1).params);
  });
});
