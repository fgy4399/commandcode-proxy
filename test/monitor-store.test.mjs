import test from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { createMonitorStore, createMonitorHandler, readReportedUsage } from '../monitor.mjs';
import { createCompletionState } from '../completion-state.mjs';

function tracked(store) {
  const res = Object.assign(new EventEmitter(), { headersSent: false, statusCode: 200, writableFinished: false });
  const tracker = store.trackRequest({ method: 'POST', url: '/v1/messages?private=value' }, res);
  return { res, tracker };
}

test('step usage accumulates, partial totals replace only reported fields', () => {
  const store = createMonitorStore();
  const { tracker } = tracked(store);
  tracker.observeEvent({ type: 'finish-step', usage: { inputTokens: 60, outputTokens: 12, cachedInputTokens: 40, inputTokenDetails: { cacheWriteTokens: 6 } } });
  tracker.observeEvent({ type: 'finish-step', usage: { inputTokens: 60, outputTokens: 12, cachedInputTokens: 40, inputTokenDetails: { cacheWriteTokens: 6 } } });
  tracker.observeEvent({ type: 'finish', totalUsage: { inputTokens: 120, outputTokens: 24 } });
  let record = store.snapshot().requests[0];
  assert.equal(record.inputTokens, 120);
  assert.equal(record.outputTokens, 24);
  assert.equal(record.cachedInputTokens, 80);
  assert.equal(record.cacheWriteTokens, 12);
  tracker.observeEvent({ type: 'finish', totalUsage: { cachedInputTokens: 0, inputTokenDetails: { cacheWriteTokens: 0 } } });
  record = store.snapshot().requests[0];
  assert.equal(record.cachedInputTokens, 0);
  assert.equal(record.cacheWriteTokens, 0);
  assert.equal(record.inputTokens, 120);
});

test('invalid usage stays unknown and explicit zero remains reported', () => {
  assert.equal(readReportedUsage({ inputTokens: -1, outputTokens: NaN, cachedInputTokens: '100' }), null);
  assert.deepEqual(readReportedUsage({ inputTokens: 0, outputTokens: 0 }), {
    inputTokens: 0, outputTokens: 0, cachedInputTokens: null, cacheWriteTokens: null, reasoningTokens: null,
  });
});

test('bounded metadata and finish/close finalization preserve a single immutable result', () => {
  let now = 1000;
  const store = createMonitorStore({ now: () => now });
  const { tracker, res } = tracked(store);
  tracker.setMetadata({ model: 'm'.repeat(2 * 1024 * 1024), stream: true });
  assert.equal(store.snapshot().requests[0].model.length, 256);
  assert.equal(store.snapshot().requests[0].path, '/v1/messages');
  now = 1500;
  assert.equal(store.snapshot().requests[0].durationMs, 500);
  res.headersSent = true;
  res.writableFinished = true;
  res.emit('finish');
  now = 2000;
  res.emit('close');
  tracker.markError('stream_error');
  assert.equal(store.snapshot().requests[0].durationMs, 500);
  assert.equal(store.snapshot().requests[0].status, 'success');
  assert.equal(res.listenerCount('close'), 0);
});

test('remote callers cannot spoof local access with forwarded headers', async () => {
  const handler = createMonitorHandler(createMonitorStore(), { token: '' });
  const req = { method: 'GET', url: '/monitor/api/snapshot', socket: { remoteAddress: '192.0.2.10' }, headers: { 'x-forwarded-for': '127.0.0.1', host: 'localhost' } };
  let status;
  const res = { setHeader() {}, writeHead(code) { status = code; }, end() {} };
  await handler(req, res);
  assert.equal(status, 403);
  req.socket.remoteAddress = '::ffff:127.0.0.1';
  await handler(req, res);
  assert.equal(status, 200);
});

test('reasoning tokens preserve missing vs zero, sum steps, and replace totals', () => {
  assert.equal(readReportedUsage({ reasoningTokens: -1 }), null);
  assert.equal(readReportedUsage({ outputTokenDetails: { reasoningTokens: '10' } }), null);
  assert.equal(readReportedUsage({ reasoningTokens: 0, outputTokenDetails: { reasoningTokens: 10 } }).reasoningTokens, 0);
  const store = createMonitorStore();
  const { tracker } = tracked(store);
  tracker.observeEvent({ type: 'reasoning-delta', text: 'No token estimate from this content' });
  assert.equal(store.snapshot().requests[0].reasoningTokens, null);
  tracker.observeEvent({ type: 'finish-step', usage: { outputTokenDetails: { reasoningTokens: 3 } } });
  tracker.observeEvent({ type: 'finish-step', usage: { reasoningTokens: 4 } });
  assert.equal(store.snapshot().requests[0].reasoningTokens, 7);
  tracker.observeEvent({ type: 'finish', totalUsage: { inputTokens: 120, outputTokens: 24 } });
  assert.equal(store.snapshot().requests[0].reasoningTokens, 7);
  tracker.observeEvent({ type: 'finish', totalUsage: { reasoningTokens: 10 } });
  assert.equal(store.snapshot().requests[0].reasoningTokens, 10);
  tracker.observeEvent({ type: 'finish', totalUsage: { outputTokenDetails: { reasoningTokens: 0 } } });
  const record = store.snapshot().requests[0];
  assert.equal(record.reasoningTokens, 0);
  assert.equal(record.inputTokens + record.outputTokens, 144);
});

test('monitor and protocol counters merge partial finish reports with the same precedence', () => {
  const store = createMonitorStore();
  const {tracker} = tracked(store);
  const state = createCompletionState();
  const event = {type: 'finish', finishReason: 'stop',
    usage: {inputTokens: 100, outputTokens: 24, cachedInputTokens: 80, cacheWriteTokens: 12, reasoningTokens: 10},
    totalUsage: {inputTokens: 120, cachedInputTokens: 0}};
  tracker.observeEvent(event); state.observe(event);
  const record = store.snapshot().requests[0];
  for (const field of ['inputTokens', 'outputTokens', 'cachedInputTokens', 'cacheWriteTokens', 'reasoningTokens']) {
    assert.equal(record[field], state[field], field);
  }
  assert.equal(record.outputTokens, 24);
  assert.equal(record.cachedInputTokens, 0);
});
