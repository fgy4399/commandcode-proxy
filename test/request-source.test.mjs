import test from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { createMonitorStore, createMonitorHandler } from '../monitor.mjs';

function add(store, {method = 'POST', path = '/v1/messages', status = 200, peer = '203.0.113.8', headers = {}, pending = false, aborted = false, usage} = {}) {
  const res = Object.assign(new EventEmitter(), {headersSent: true, statusCode: status, writableFinished: !aborted});
  const tracker = store.trackRequest({method, url: path, headers, socket: {remoteAddress: peer}}, res);
  if (usage) tracker.observeEvent({type: 'finish', totalUsage: usage});
  if (!pending) res.emit(aborted ? 'close' : 'finish');
  return store.snapshot().requests[0];
}
const query = (store, values = {}) => store.query(new URLSearchParams(values));

test('scan errors stay in logs but do not alter model rates, latency or token totals', () => {
  let now = Date.now();
  const store = createMonitorStore({now: () => now});
  add(store, {usage: {inputTokens: 100, outputTokens: 20}});
  now++;
  add(store, {status: 503});
  now++;
  add(store, {aborted: true});
  now++;
  add(store, {pending: true});
  const before = query(store).summary;
  for (const path of ['/backup.sql', '/.git/config', '/config.xml', '/robots.txt', '/v1/models', '/v1/messages']) {
    now++; add(store, {method: 'GET', path, status: 404, usage: {inputTokens: 900, outputTokens: 900}});
  }
  const result = query(store, {pageSize: '1'});
  assert.equal(result.total, 10);
  assert.equal(result.requests.length, 1);
  assert.equal(result.summary.trafficTotal, 10);
  assert.equal(result.summary.excludedTotal, 6);
  assert.equal(result.summary.total, 4);
  assert.equal(result.summary.successRate, 1 / 3);
  assert.equal(result.summary.failureRate, 2 / 3);
  assert.equal(result.summary.error, 1);
  assert.equal(result.summary.pending, 1);
  for (const key of ['avgDurationMs', 'inputTokens', 'outputTokens', 'successRate', 'failureRate']) assert.equal(result.summary[key], before[key]);
  assert.equal(result.trend.reduce((n, item) => n + item.outputTokens, 0), 20);
  const other = query(store, {requestKind: 'other'});
  assert.equal(other.total, 6);
  assert.equal(other.summary.total, 0);
  assert.equal(other.summary.successRate, null);
  assert.equal(other.summary.failureRate, null);
  assert.equal(query(store, {requestKind: 'model'}).total, 4);
  assert.equal(store.export(new URLSearchParams({requestKind: 'other'})).requests.length, 6);
});

test('real model route validation and authentication failures remain failures', () => {
  const store = createMonitorStore();
  for (const status of [400, 401, 404, 429, 503]) add(store, {status});
  add(store, {method: 'POST', path: '/v1/chat/completions?unused=1', status: 200});
  const summary = query(store).summary;
  assert.equal(summary.total, 6);
  assert.equal(summary.error, 5);
  assert.equal(summary.failureRate, 5 / 6);
});

test('default address uses socket, normalizes mapped IPv4, and ignores spoofed headers', () => {
  const store = createMonitorStore();
  const record = add(store, {peer: '::ffff:203.0.113.8', headers: {'x-forwarded-for': '198.51.100.5', 'x-real-ip': '198.51.100.6', 'cf-connecting-ip': '198.51.100.7'}});
  assert.equal(record.clientIp, '203.0.113.8');
  assert.equal(record.peerIp, '203.0.113.8');
  assert.equal(record.ipSource, 'socket');
  assert.equal(query(store, {ip: '203.0.113.8'}).total, 1);
  assert.equal(query(store, {ip: '198.51.100.5'}).total, 0);
  assert.equal(query(store, {q: '203.0.113.8'}).total, 1);
});

test('trusted chains select the nearest untrusted address, not a spoofed leftmost address', () => {
  const store = createMonitorStore({trustedProxies: '10.0.0.0/24,2001:db8:1::/48'});
  const record = add(store, {peer: '10.0.0.2', headers: {'x-forwarded-for': '192.0.2.99, 198.51.100.7, 10.0.0.3'}});
  assert.equal(record.clientIp, '198.51.100.7');
  assert.equal(record.peerIp, '10.0.0.2');
  assert.equal(record.ipSource, 'x-forwarded-for');
  const ipv6 = add(store, {peer: '2001:db8:1::1', headers: {'x-forwarded-for': '2001:db8:2::42'}});
  assert.equal(ipv6.clientIp, '2001:db8:2::42');
  assert.equal(query(store, {ip: '2001:0db8:0002::42'}).total, 1);
  const untrusted = add(store, {peer: '10.0.1.2', headers: {'x-forwarded-for': '198.51.100.7'}});
  assert.equal(untrusted.clientIp, '10.0.1.2');
});

test('trusted single-header fallback and malformed forwarding fail closed', () => {
  const store = createMonitorStore({trustedProxies: '127.0.0.1'});
  assert.equal(add(store, {peer: '127.0.0.1', headers: {'x-real-ip': '198.51.100.8'}}).clientIp, '198.51.100.8');
  for (const value of ['invalid, 198.51.100.8', '198.51.100.8,', 'unknown', ['198.51.100.8'], '1'.repeat(5000), Array(33).fill('198.51.100.8').join(',')]) {
    const r = add(store, {peer: '127.0.0.1', headers: {'x-forwarded-for': value}});
    assert.equal(r.clientIp, '127.0.0.1');
    assert.equal(r.ipSource, 'socket');
  }
  const r = add(store, {peer: '127.0.0.1', headers: {'x-forwarded-for': '10.20.30.40'}});
  assert.equal(r.clientIp, '10.20.30.40', 'must not skip private clients to find a claimed public IP');
});

test('invalid proxy trust and IP queries are rejected', () => {
  for (const value of ['true', '*', 'localhost', '10.0.0.1/33', '2001:db8::/129', 'not-an-ip']) assert.throws(() => createMonitorStore({trustedProxies: value}));
  const store = createMonitorStore();
  for (const values of [{ip: 'not-ip'}, {ip: '203.0.113.1:8080'}, {requestKind: 'scan'}]) assert.throws(() => query(store, values));
});

test('IP metadata persists and legacy scan records are classified without inventing IPs', async t => {
  const directory = await mkdtemp(join(process.env.PI_SCRATCH_DIR || tmpdir(), 'monitor-source-'));
  const stores = [];
  t.after(async () => {for (const store of stores) await store.flush(); await rm(directory, {recursive: true, force: true});});
  const store = createMonitorStore({persistence: directory, trustedProxies: '10.0.0.1'}); stores.push(store);
  await store.initialize();
  add(store, {peer: '10.0.0.1', method: 'GET', path: '/backup.sql', status: 404, headers: {'x-forwarded-for': '198.51.100.7'}});
  await store.flush();
  const saved = JSON.parse(await readFile(join(directory, 'history-v1.json'), 'utf8'));
  const legacy = {...saved.requests[0], id: 'legacy-scan', requestKind: 'model'};
  delete legacy.clientIp; delete legacy.peerIp; delete legacy.ipSource;
  saved.requests.push(legacy);
  await writeFile(join(directory, 'history-v1.json'), JSON.stringify(saved));
  const reloaded = createMonitorStore({persistence: directory}); stores.push(reloaded);
  await reloaded.initialize();
  const result = query(reloaded);
  assert.equal(result.summary.total, 0);
  assert.equal(result.total, 2);
  assert.equal(reloaded.getRequest(saved.requests[0].id).clientIp, '198.51.100.7');
  assert.equal(reloaded.getRequest('legacy-scan').requestKind, 'other');
  assert.equal(reloaded.getRequest('legacy-scan').clientIp, null);
});

test('forwarded IP metadata never grants local monitor access', async () => {
  const store = createMonitorStore({trustedProxies: '198.51.100.10'});
  const handler = createMonitorHandler(store, {token: ''});
  let status;
  await handler({method: 'GET', url: '/monitor/api/query', socket: {remoteAddress: '198.51.100.10'}, headers: {'x-forwarded-for': '127.0.0.1'}}, {
    setHeader() {}, writeHead(code) {status = code;}, end() {},
  });
  assert.equal(status, 403);
});

test('duplicate physical headers and malformed XFF cannot fall back to forged X-Real-IP', () => {
  const store = createMonitorStore({trustedProxies: '127.0.0.1'});
  for (const [headers, rawHeaders] of [
    [{'x-forwarded-for': '198.51.100.7, 198.51.100.8'}, ['X-Forwarded-For', '198.51.100.7', 'x-forwarded-for', '198.51.100.8']],
    [{'x-real-ip': '198.51.100.7'}, ['X-Real-IP', '198.51.100.7', 'x-real-ip', '198.51.100.8']],
    [{'x-forwarded-for': 'bad-ip', 'x-real-ip': '198.51.100.7'}, []],
  ]) {
    const res = Object.assign(new EventEmitter(), {statusCode: 404, headersSent: true});
    store.trackRequest({method: 'GET', url: '/backup.sql', socket: {remoteAddress: '127.0.0.1'}, headers, rawHeaders}, res);
    res.emit('finish');
    const record = store.snapshot().requests[0];
    assert.equal(record.clientIp, '127.0.0.1');
    assert.equal(record.ipSource, 'socket');
  }
});
