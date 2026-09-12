import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { EventEmitter } from 'node:events';
import { mkdtemp, mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createMonitorStore } from '../monitor.mjs';
import {
  apiKeyIdentity, maskApiKey, sanitizeFinalRecord,
  parseMonitorFilters, filterMonitorRequests, queryMonitorSnapshot,
} from '../monitor-history.mjs';

const DAY = 86400000;
const NOW = Date.parse('2026-05-20T12:00:00.000Z');
const KEY_A = 'user_4jGGJKmH8SOMETHINGxMyDZch';
const KEY_B = 'user_4jGGDIFFERENT_MIDDLEDZch';
const identityA = apiKeyIdentity(KEY_A);
const identityB = apiKeyIdentity(KEY_B);
const iso = time => new Date(time).toISOString();
const params = values => new URLSearchParams(values);

function record(id, overrides = {}) {
  return {
    id, startedAt: iso(NOW), endedAt: iso(NOW + 10),
    method: 'POST', path: '/v1/messages', model: 'model-a', stream: false,
    status: 'success', httpStatus: 200, durationMs: 10,
    requestedReasoningEffort: null, reasoningEffort: null, thinkingType: null,
    thinkingBudgetTokens: null, keyId: null, keyLabel: null,
    usageReported: false, error: null, inputTokens: null, outputTokens: null,
    cachedInputTokens: null, cacheWriteTokens: null, reasoningTokens: null,
    upstreamFinishReceived: null, hasUpstreamOutput: null,
    lastUpstreamEvent: null, lastUpstreamEventAt: null, upstreamFinishReason: null,
    requestKind: 'model', clientIp: null, peerIp: null, ipSource: null,
    ...overrides,
  };
}

function track(store, { key = KEY_A, model = 'model-a', finish = true, usage } = {}) {
  const res = Object.assign(new EventEmitter(), { statusCode: 200, headersSent: true });
  const tracker = store.trackRequest({
    method: 'POST', url: `/v1/messages?api_key=${key}#private`,
    headers: { authorization: `Bearer ${key}` }, body: { prompt: 'private-prompt' },
  }, res);
  tracker.setApiKey(key);
  tracker.setMetadata({ model, messages: [{ content: 'private-prompt' }] });
  if (usage) tracker.observeEvent({ type: 'finish', totalUsage: usage, text: 'private-response' });
  if (finish) res.emit('finish');
  return { tracker, res, id: store.snapshot().requests[0].id };
}

async function temporaryHistory(t) {
  const directory = await mkdtemp(join(process.env.PI_SCRATCH_DIR || tmpdir(), 'monitor-history-test-'));
  const stores = [];
  t.after(async () => {
    try { for (const store of stores) await store.flush(); }
    finally { await rm(directory, { recursive: true, force: true }); }
  });
  return {
    directory,
    store(options = {}) {
      const store = createMonitorStore({ now: () => NOW, persistence: directory, ...options });
      stores.push(store);
      return store;
    },
  };
}

function assertNoSecrets(value) {
  const serialized = typeof value === 'string' ? value : JSON.stringify(value);
  for (const secret of [KEY_A, KEY_B, 'private-prompt', 'private-response', 'Bearer ']) {
    assert.equal(serialized.includes(secret), false, `leaked ${secret}`);
  }
}

test('API key labels strip user_, hide short suffixes, and identities hash the complete key', () => {
  assert.equal(maskApiKey(KEY_A), '4jGG****DZch');
  assert.equal(maskApiKey(KEY_B), '4jGG****DZch');
  for (const suffix of ['a', '1234567', '12345678']) assert.equal(maskApiKey(`user_${suffix}`), '****');
  assert.equal(maskApiKey('user_123456789'), '1234****6789');
  assert.equal(identityA.keyId, createHash('sha256').update(KEY_A).digest('hex'));
  assert.equal(identityB.keyId, createHash('sha256').update(KEY_B).digest('hex'));
  assert.notEqual(identityA.keyId, identityB.keyId);
  for (const invalid of [null, undefined, 42, {}, [], '', 'user_', 'sk-secret', ' user_123456789', 'user_bad key', 'user_bad\n']) {
    assert.equal(maskApiKey(invalid), null);
    assert.deepEqual(apiKeyIdentity(invalid), { keyId: null, keyLabel: null });
  }
});

test('flush/reload preserves finalized records and usage without credentials or content', async t => {
  const fixture = await temporaryHistory(t);
  const store = fixture.store();
  await store.initialize();
  const completed = track(store, { usage: { inputTokens: 100, outputTokens: 20, reasoningTokens: 5, cachedInputTokens: 0 } });
  const pending = track(store, { key: KEY_B, finish: false });
  await store.flush();
  const raw = await readFile(join(fixture.directory, 'history-v1.json'), 'utf8');
  const saved = JSON.parse(raw);
  assert.equal(saved.version, 1);
  assert.deepEqual(saved.requests.map(item => item.id), [completed.id]);
  assertNoSecrets(raw);
  assert.equal(store.snapshot().requests.length, 2);
  const reloaded = fixture.store();
  await reloaded.initialize();
  assert.deepEqual(reloaded.snapshot().requests, [store.getRequest(completed.id)]);
  assert.equal(reloaded.getRequest(pending.id), null);
  assert.equal(reloaded.snapshot().persistenceError, null);
  assert.equal(reloaded.snapshot().persistenceEnabled, true);
});

test('default 30-day retention includes the cutoff and expires it one millisecond later', () => {
  let clock = NOW;
  const store = createMonitorStore({ now: () => clock, persistence: false });
  const old = track(store);
  clock += 30 * DAY;
  assert.equal(store.getRequest(old.id)?.id, old.id);
  const recent = track(store);
  clock += 1;
  const snapshot = store.snapshot();
  assert.deepEqual(snapshot.requests.map(item => item.id), [recent.id]);
  assert.equal(store.getRequest(old.id), null);
  assert.equal(snapshot.retentionDays, 30);
  assert.equal(snapshot.retentionLimit, 1000);
  assert.equal(snapshot.historyTruncated, true);
  assert.equal(snapshot.oldestAvailableAt, iso(NOW + 30 * DAY));
});

test('retention cap evicts oldest records and late finalization cannot resurrect them', () => {
  let clock = NOW;
  const store = createMonitorStore({ now: () => clock, retentionLimit: 2 });
  const evicted = track(store, { finish: false });
  clock += 1;
  const middle = track(store);
  clock += 1;
  const newest = track(store);
  evicted.res.emit('finish');
  assert.deepEqual(store.snapshot().requests.map(item => item.id), [newest.id, middle.id]);
  assert.equal(store.snapshot().historyTruncated, true);
  assert.equal(store.getRequest(evicted.id), null);
});

test('reload sorts and prunes expired and excess records, then compacts persisted history', async t => {
  const fixture = await temporaryHistory(t);
  const requests = [
    record('expired', { startedAt: iso(NOW - 30 * DAY - 1) }),
    record('oldest-retained', { startedAt: iso(NOW - DAY) }),
    record('newest'),
    record('over-cap', { startedAt: iso(NOW - 2 * DAY) }),
  ];
  await writeFile(join(fixture.directory, 'history-v1.json'), JSON.stringify({ version: 1, requests }));
  const store = fixture.store({ retentionLimit: 2 });
  await store.initialize();
  assert.deepEqual(store.snapshot().requests.map(item => item.id), ['newest', 'oldest-retained']);
  assert.equal(store.snapshot().historyTruncated, true);
  await store.flush();
  const reloaded = fixture.store({ retentionLimit: 2 });
  await reloaded.initialize();
  assert.deepEqual(reloaded.snapshot().requests, store.snapshot().requests);
  const saved = JSON.parse(await readFile(join(fixture.directory, 'history-v1.json'), 'utf8'));
  assert.deepEqual(saved.requests.map(item => item.id), ['newest', 'oldest-retained']);
});

test('sanitization and reload whitelist metadata and reject invalid or unfinished records', async t => {
  const fixture = await temporaryHistory(t);
  const dirty = record('safe', {
    ...identityA, path: `/v1/messages?key=${KEY_A}#private-prompt`,
    apiKey: KEY_A, request: { prompt: 'private-prompt' },
    headers: { authorization: `Bearer ${KEY_A}` }, response: { text: 'private-response' },
    body: 'private-prompt', error: 'private-response',
  });
  const expected = record('safe', identityA);
  assert.deepEqual(sanitizeFinalRecord(dirty), expected);
  for (const invalid of [null, [], {}, record('pending', { status: 'pending' }),
    record('unfinished', { endedAt: null }), record('bad-date', { startedAt: 'nonsense' }),
    record('bad-status', { status: 'unknown' })]) {
    assert.equal(sanitizeFinalRecord(invalid), null);
  }
  await writeFile(join(fixture.directory, 'history-v1.json'), JSON.stringify({
    version: 1, requests: [dirty, record('pending', { status: 'pending', endedAt: null }), null],
  }));
  const store = fixture.store();
  await store.initialize();
  assert.deepEqual(store.snapshot().requests, [expected]);
  assertNoSecrets(store.snapshot());
  await store.flush();
  assertNoSecrets(await readFile(join(fixture.directory, 'history-v1.json'), 'utf8'));
});

test('malformed history reports persistenceError and can recover without losing new requests', async t => {
  const fixture = await temporaryHistory(t);
  await writeFile(join(fixture.directory, 'history-v1.json'), '{broken json');
  const store = fixture.store();
  await assert.doesNotReject(store.initialize());
  assert.equal(store.snapshot().persistenceError, 'history_persistence_unavailable');
  const completed = track(store);
  await assert.doesNotReject(store.flush());
  assert.equal(store.getRequest(completed.id)?.status, 'success');
  assert.equal(store.snapshot().persistenceError, null);
  const reloaded = fixture.store();
  await reloaded.initialize();
  assert.deepEqual(reloaded.snapshot().requests, store.snapshot().requests);
});

for (const obstruction of ['directory-is-file', 'snapshot-is-directory']) {
  test(`persistence ${obstruction} failures leave finalized requests available in memory`, async t => {
    const fixture = await temporaryHistory(t);
    let persistence = fixture.directory;
    if (obstruction === 'directory-is-file') {
      persistence = join(fixture.directory, 'ordinary-file');
      await writeFile(persistence, 'existing file');
    } else {
      await mkdir(join(fixture.directory, 'history-v1.json'));
    }
    const store = fixture.store({ persistence });
    await assert.doesNotReject(store.initialize());
    assert.equal(store.snapshot().persistenceError, 'history_persistence_unavailable');
    const completed = track(store);
    await assert.doesNotReject(store.flush());
    assert.equal(store.snapshot().persistenceError, 'history_persistence_unavailable');
    assert.equal(store.getRequest(completed.id)?.status, 'success');
    assert.equal(store.snapshot().requests.length, 1);
    if (obstruction === 'directory-is-file') assert.equal(await readFile(persistence, 'utf8'), 'existing file');
  });
}

test('combined filters use inclusive dates with offsets, preserve collision facets, and remain pure', () => {
  const from = NOW - DAY;
  const requests = [
    record('before', { ...identityA, startedAt: iso(from - 1) }),
    record('lower', { ...identityA, startedAt: iso(from) }),
    record('upper', identityA),
    record('after', { ...identityA, startedAt: iso(NOW + 1) }),
    record('other-key', identityB),
    record('other-model', { ...identityA, model: 'model-b' }),
    record('other-status', { ...identityA, status: 'error', httpStatus: 500 }),
    record('no-key'),
  ];
  const snapshot = { requests, retentionDays: 30 };
  const before = structuredClone(snapshot);
  const filters = params({ keyId: identityA.keyId, model: 'model-a', status: 'success',
    from: '2026-05-19T14:00:00+02:00', to: '2026-05-20T07:00:00-05:00', pageSize: '1', page: '2' });
  const result = queryMonitorSnapshot(snapshot, filters, NOW);
  assert.equal(parseMonitorFilters(filters).from, from);
  assert.equal(parseMonitorFilters(filters).to, NOW);
  assert.equal(result.total, 2);
  assert.equal(result.pages, 2);
  assert.equal(result.page, 2);
  assert.deepEqual(result.requests.map(item => item.id), ['lower']);
  assert.equal(result.summary.total, 2);
  assert.deepEqual(result.facets.models, ['model-a', 'model-b']);
  assert.deepEqual(result.facets.keys.map(key => key.id).sort(), [identityA.keyId, identityB.keyId].sort());
  assert.deepEqual(result.facets.keys.map(key => key.label), ['4jGG****DZch', '4jGG****DZch']);
  assert.deepEqual(queryMonitorSnapshot(snapshot, params({ keyId: 'none' }), NOW).requests.map(item => item.id), ['no-key']);
  assert.deepEqual(snapshot, before);
});

for (const days of [1, 7, 30]) {
  test(`${days}-day range uses injected time and includes both boundaries in totals and trend`, () => {
    const start = NOW - days * DAY;
    const snapshot = { requests: [
      record('before', { startedAt: iso(start - 1), inputTokens: 999 }),
      record('start', { startedAt: iso(start), inputTokens: 2, outputTokens: 3 }),
      record('end', { inputTokens: 5, outputTokens: 7 }),
      record('future', { startedAt: iso(NOW + 1), inputTokens: 999 }),
    ] };
    const result = queryMonitorSnapshot(snapshot, params({ from: iso(start), to: iso(NOW) }), NOW);
    assert.deepEqual(result.requests.map(item => item.id), ['end', 'start']);
    assert.equal(result.summary.inputTokens, 7);
    assert.equal(result.trend[0].startedAt, iso(start));
    assert.equal(result.trend[0].inputTokens, 2);
    assert.equal(result.trend.at(-1).inputTokens, 5);
    assert.equal(result.trend.reduce((sum, bucket) => sum + bucket.outputTokens, 0), 10);
    const openEnded = queryMonitorSnapshot(snapshot, params({ from: iso(start) }), NOW);
    assert.equal(openEnded.trend.at(-1).inputTokens, 5);
  });
}

test('summary spans all pages, distinguishes unknown and zero, and counts only eligible cache input', () => {
  const requests = [
    record('cached', { inputTokens: 100, cachedInputTokens: 40, outputTokens: 10, reasoningTokens: 7, usageReported: true }),
    record('cache-unknown', { inputTokens: 900, outputTokens: 20, reasoningTokens: 0, usageReported: true }),
    record('cache-zero', { inputTokens: 100, cachedInputTokens: 0, outputTokens: 0, usageReported: true, status: 'error', durationMs: 20 }),
    record('input-unknown', { cachedInputTokens: 10, status: 'aborted', durationMs: 30 }),
    record('pending', { status: 'pending', endedAt: null, durationMs: 9999 }),
  ];
  const result = queryMonitorSnapshot({ requests }, params({ pageSize: '1', page: '2' }), NOW);
  assert.equal(result.requests.length, 1);
  assert.deepEqual(result.summary, {
    total: 5, trafficTotal: 5, excludedTotal: 0, failureRate: 0.5, success: 2, error: 1, aborted: 1, pending: 1,
    usageReported: 3, reasoningReported: 2, inputTokens: 1100, outputTokens: 30,
    cachedInputTokens: 50, cacheWriteTokens: null, reasoningTokens: 7,
    cacheHits: 2, cacheRate: 0.2, successRate: 0.5, avgDurationMs: 17.5,
  });
  assert.equal(result.trend.reduce((sum, bucket) => sum + bucket.outputTokens, 0), 30);
  assert.deepEqual(queryMonitorSnapshot({ requests }, params({ pageSize: '1' }), NOW).summary, result.summary);
  const unknown = queryMonitorSnapshot({ requests: [record('unknown')] }, undefined, NOW).summary;
  const zero = queryMonitorSnapshot({ requests: [record('zero', {
    inputTokens: 0, outputTokens: 0, cachedInputTokens: 0, reasoningTokens: 0, usageReported: true,
  })] }, undefined, NOW).summary;
  assert.equal(unknown.inputTokens, null);
  assert.equal(unknown.reasoningTokens, null);
  assert.equal(unknown.usageReported, 0);
  assert.equal(zero.inputTokens, 0);
  assert.equal(zero.outputTokens, 0);
  assert.equal(zero.reasoningTokens, 0);
  assert.equal(zero.usageReported, 1);
  assert.equal(zero.cacheRate, null);
  const empty = queryMonitorSnapshot({ requests: [] }, undefined, NOW);
  assert.equal(empty.summary.total, 0);
  assert.equal(empty.summary.successRate, null);
  assert.equal(empty.summary.avgDurationMs, null);
  assert.equal(empty.trend[0].startedAt, iso(NOW - 30 * 60000));
});

test('invalid dates, reversed ranges, pages, filters, and duplicate parameters are rejected', () => {
  const invalid = [
    { from: '2026-02-30T00:00:00Z' }, { from: '2026-05-20' }, { from: 'not-a-date' },
    { from: '2026-05-20T12:00:00' }, { to: '2026-05-20T24:00:00Z' },
    { from: iso(NOW), to: iso(NOW - 1) }, { range: '2d' },
    { page: '0' }, { page: '-1' }, { page: '1.5' }, { page: '01' }, { page: '9007199254740992' },
    { pageSize: '0' }, { pageSize: '101' }, { status: 'complete' }, { keyId: KEY_A },
    { keyId: 'a'.repeat(63) }, { model: '' }, { model: 'm'.repeat(257) }, { q: '\n' },
  ].map(params);
  for (const [key, value] of Object.entries({ keyId: identityA.keyId, model: 'model-a', status: 'success',
    q: 'model', from: iso(NOW), to: iso(NOW), page: '1', pageSize: '15' })) {
    invalid.push(new URLSearchParams([[key, value], [key, value]]));
  }
  for (const query of invalid) {
    assert.throws(() => parseMonitorFilters(query), { name: 'TypeError', message: 'invalid_query' }, query.toString());
  }
});

test('export helper applies filters across pages and exports only safe key identities', () => {
  let clock = NOW;
  const store = createMonitorStore({ now: () => clock });
  const first = track(store);
  clock += 1;
  const second = track(store);
  clock += 1;
  track(store, { key: KEY_B });
  track(store, { model: 'model-b' });
  const query = params({ keyId: identityA.keyId, model: 'model-a', status: 'success',
    from: iso(NOW), to: iso(clock), q: 'MODEL-A', page: '2', pageSize: '1' });
  const exported = store.export(query);
  assert.deepEqual(exported.requests.map(item => item.id), [second.id, first.id]);
  assert.equal(exported.exportedAt, iso(clock));
  assert.deepEqual(exported.requests, filterMonitorRequests(store.snapshot().requests, parseMonitorFilters(query)));
  assert.equal(store.query(query).requests.length, 1);
  assertNoSecrets(exported);
  assert.equal(store.export(params({ q: KEY_A })).requests.length, 0);
  assert.equal(store.export(params({ q: '4jGG****DZch' })).requests.length, 4);
});

test('stream diagnostics survive restart without retaining reasoning text', async t => {
  const fixture = await temporaryHistory(t), store = fixture.store();
  await store.initialize();
  const active = track(store, {finish: false});
  active.tracker.observeEvent({type: 'reasoning-delta', text: 'private-thinking-never-persist'});
  active.tracker.markError('upstream_incomplete');
  active.res.emit('finish');
  await store.flush();
  const reloaded = fixture.store();
  await reloaded.initialize();
  const r = reloaded.getRequest(active.id);
  assert.equal(r.hasUpstreamOutput, true);
  assert.equal(r.upstreamFinishReceived, false);
  assert.equal(r.lastUpstreamEvent, 'reasoning-delta');
  assert.equal(r.lastUpstreamEventAt, iso(NOW));
  assert.equal(r.error, 'upstream_incomplete');
  assert.doesNotMatch(await readFile(join(fixture.directory, 'history-v1.json'), 'utf8'), /private-thinking-never-persist/);
  const old = sanitizeFinalRecord(record('legacy'));
  assert.equal(old.upstreamFinishReceived, null);
  assert.equal(old.hasUpstreamOutput, null);
});
