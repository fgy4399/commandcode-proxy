import { createHash, randomUUID, timingSafeEqual } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { isIP } from 'node:net';
import { apiKeyIdentity, createSnapshotPersistence, sanitizeFinalRecord,
  parseMonitorFilters, filterMonitorRequests, queryMonitorSnapshot, UPSTREAM_EVENT_TYPES, UPSTREAM_FINISH_REASONS } from './monitor-history.mjs';
export { maskApiKey, apiKeyIdentity, parseMonitorFilters, filterMonitorRequests, queryMonitorSnapshot } from './monitor-history.mjs';

export const RETENTION_LIMIT = 1000;
const TOKEN_FIELDS = ['inputTokens', 'outputTokens', 'cachedInputTokens', 'cacheWriteTokens', 'reasoningTokens'];
const SAFE_ERRORS = new Set(['invalid_request_error', 'auth_error', 'authentication_error',
  'rate_limit_error', 'not_found', 'upstream_error', 'temporarily_unavailable', 'proxy_error',
  'internal_error', 'server_busy', 'stream_timeout', 'stream_error', 'zero_output', 'empty_response', 'upstream_incomplete']);
// Copy the prefix: V8 sliced strings can otherwise keep a large request body alive.
const bounded = (value, limit) => typeof value === 'string'
  ? Buffer.from(value.slice(0, limit), 'utf8').toString('utf8') : null;
const tokenCount = value => typeof value === 'number' && Number.isFinite(value) && value >= 0
  ? Math.min(value, Number.MAX_SAFE_INTEGER) : null;

export function requestPath(url = '/') {
  // Do not retain query parameters or fragments (which may contain credentials).
  try {
    return bounded(new URL(url, 'http://localhost').pathname, 2048);
  } catch {
    return bounded(typeof url === 'string' ? url.split(/[?#]/, 1)[0] || '/' : '/', 2048);
  }
}

export function isLoopback(address) {
  if (typeof address !== 'string') return false;
  if (address === '::1') return true;
  const ipv4 = address.startsWith('::ffff:') ? address.slice(7) : address;
  return isIP(ipv4) === 4 && ipv4.split('.')[0] === '127';
}

export function tokenMatches(authorization, token) {
  if (typeof authorization !== 'string' || !/^Bearer /i.test(authorization)) return false;
  const hash = value => createHash('sha256').update(value).digest();
  return timingSafeEqual(hash(authorization.slice(7)), hash(token));
}

export function readReportedUsage(usage) {
  if (!usage || typeof usage !== 'object' || Array.isArray(usage)) return null;
  const values = {
    inputTokens: tokenCount(usage.inputTokens),
    outputTokens: tokenCount(usage.outputTokens),
    cachedInputTokens: tokenCount(usage.cachedInputTokens) ?? tokenCount(usage.inputTokenDetails?.cacheReadTokens),
    cacheWriteTokens: tokenCount(usage.cacheWriteTokens) ?? tokenCount(usage.inputTokenDetails?.cacheWriteTokens),
    reasoningTokens: tokenCount(usage.reasoningTokens) ?? tokenCount(usage.outputTokenDetails?.reasoningTokens),
  };
  return TOKEN_FIELDS.some(field => values[field] !== null) ? values : null;
}

// Persistence is opt-in for embedders; production supplies a directory and the 100,000-record cap.
export function createMonitorStore({ now = Date.now, retentionLimit = RETENTION_LIMIT,
  retentionDays = 30, persistence = false } = {}) {
  if (!Number.isInteger(retentionLimit) || retentionLimit < 1 || retentionLimit > 100000) throw new TypeError('invalid retentionLimit');
  if (!Number.isFinite(retentionDays) || retentionDays <= 0 || retentionDays > 30) throw new TypeError('invalid retentionDays');
  if (persistence !== false && (typeof persistence !== 'string' || !persistence)) throw new TypeError('invalid persistence directory');
  const startedAt = new Date(now()).toISOString();
  let historyStartedAt = startedAt;
  let historyTruncated = false;
  const requests = [];
  let initialized = !persistence;
  let initialization;
  const disk = persistence ? createSnapshotPersistence(persistence, () => {
    prune();
    return { version: 1, historyStartedAt, historyTruncated,
      requests: requests.map(({ record }) => sanitizeFinalRecord(record)).filter(Boolean) };
  }) : null;
  const changed = () => { if (initialized) disk?.schedule(); };
  function prune() {
    const cutoff = now() - retentionDays * 86400000;
    let removed = false;
    while (requests.length && (requests.length > retentionLimit || Date.parse(requests[requests.length - 1].record.startedAt) < cutoff)) {
      requests.pop().record = null;
      removed = true;
    }
    if (removed) { historyTruncated = true; changed(); }
  }
  const store = {
    // Await before accepting requests. Only finalized records are restored; pending requests stay memory-only.
    initialize() {
      if (!initialization) initialization = (async () => {
        if (!disk) return;
        const saved = await disk.load();
        if (saved) {
          if (typeof saved.historyStartedAt === 'string' && Number.isFinite(Date.parse(saved.historyStartedAt))) {
            historyStartedAt = new Date(Math.min(Date.parse(saved.historyStartedAt), Date.parse(startedAt))).toISOString();
          }
          historyTruncated = historyTruncated || saved.historyTruncated === true;
          const ids = new Set(requests.map(({ record }) => record.id));
          for (const value of saved.requests) {
            const record = sanitizeFinalRecord(value);
            if (record && !ids.has(record.id)) {
              ids.add(record.id);
              requests.push({ record });
              if (record.startedAt < historyStartedAt) historyStartedAt = record.startedAt;
            }
          }
          requests.sort((a, b) => Date.parse(b.record.startedAt) - Date.parse(a.record.startedAt));
          prune();
        }
        initialized = true;
        // Compact on successful load, including cap/age pruning and stripping unrecognized properties.
        if (saved || requests.length) changed();
      })();
      return initialization;
    },
    async flush() {
      await store.initialize();
      prune();
      await disk?.flush();
    },
    snapshot() {
      prune();
      const time = now();
      return { startedAt, retentionLimit, retentionDays, persistenceEnabled: Boolean(disk),
        persistenceError: disk?.error ?? null, oldestAvailableAt: requests[requests.length - 1]?.record.startedAt ?? null,
        historyStartedAt, historyTruncated, requests: requests.map(({ record }) => ({
          ...record,
          durationMs: record.endedAt === null ? Math.max(0, time - Date.parse(record.startedAt)) : record.durationMs,
        })) };
    },
    query(params) { return queryMonitorSnapshot(store.snapshot(), params, now()); },
    export(params) {
      const filters = parseMonitorFilters(params);
      return { exportedAt: new Date(now()).toISOString(), scope: 'filtered retained history',
        requests: filterMonitorRequests(store.snapshot().requests, filters) };
    },
    getRequest(id) {
      prune();
      const record = requests.find(entry => entry.record.id === id)?.record;
      return record ? { ...record, durationMs: record.endedAt === null
        ? Math.max(0, now() - Date.parse(record.startedAt)) : record.durationMs } : null;
    },
    trackRequest(req, res) {
      const started = now();
      const entry = { record: {
        id: randomUUID(), startedAt: new Date(started).toISOString(), endedAt: null,
        method: bounded(req.method, 32), path: requestPath(req.url), model: null, stream: null,
        status: 'pending', httpStatus: null, durationMs: 0,
        requestedReasoningEffort: null, reasoningEffort: null, thinkingType: null, thinkingBudgetTokens: null,
        inputTokens: null, outputTokens: null, cachedInputTokens: null, cacheWriteTokens: null, reasoningTokens: null,
        usageReported: false, error: null,
        keyId: null, keyLabel: null,
        upstreamFinishReceived: false, hasUpstreamOutput: false,
        lastUpstreamEvent: null, lastUpstreamEventAt: null, upstreamFinishReason: null,
      } };
      if (!requests.length || started >= Date.parse(requests[0].record.startedAt)) requests.unshift(entry);
      else {
        const index = requests.findIndex(item => Date.parse(item.record.startedAt) <= started);
        requests.splice(index < 0 ? requests.length : index, 0, entry);
      }
      if (entry.record.startedAt < historyStartedAt) historyStartedAt = entry.record.startedAt;
      // Active handlers may outlive retention; their trackers must release evicted metadata too.
      prune();
      changed();
      let finalized = false;
      let hasTotal = false;
      const finish = completed => {
        if (finalized) return;
        finalized = true;
        res.off('finish', onFinish);
        res.off('close', onClose);
        const record = entry.record;
        if (!record) return;
        const ended = now();
        record.endedAt = new Date(ended).toISOString();
        record.durationMs = Math.max(0, ended - started);
        record.httpStatus = res.headersSent || completed ? res.statusCode : null;
        if (!record.error && record.httpStatus >= 400) record.error = `http_${record.httpStatus}`;
        record.status = record.error ? 'error' : completed ? 'success' : 'aborted';
        prune();
        changed();
      };
      const onFinish = () => finish(true);
      const onClose = () => finish(res.writableFinished === true);
      res.once('finish', onFinish);
      res.once('close', onClose);
      const tracker = {
        setApiKey(apiKey) {
          const record = entry.record;
          if (record && !finalized) Object.assign(record, apiKeyIdentity(apiKey));
        },
        setMetadata(body, defaultModel) {
          const record = entry.record;
          if (!record || finalized || !body || typeof body !== 'object' || Array.isArray(body)) return;
          record.model = bounded(body.model || defaultModel, 256);
          record.stream = body.stream === true;
          let effort = body.reasoning_effort;
          if (record.path === '/v1/messages' && effort === undefined) {
            effort = body.output_config?.effort !== undefined ? body.output_config.effort : body.thinking?.effort;
          }
          record.requestedReasoningEffort = bounded(effort, 64);
          record.thinkingType = bounded(body.thinking?.type, 64);
          record.thinkingBudgetTokens = tokenCount(body.thinking?.budget_tokens);
        },
        setForwardedMetadata(params) {
          const record = entry.record;
          if (!record || finalized || !params || typeof params !== 'object' || Array.isArray(params)) return;
          record.reasoningEffort = bounded(params.reasoning_effort, 64);
          record.model = bounded(params.model, 256);
        },
        markError(type) {
          const record = entry.record;
          if (record && !finalized && !record.error) record.error = SAFE_ERRORS.has(type) ? type : 'upstream_error';
        },
        observeEvent(event) {
          const record = entry.record;
          if (!record || finalized || !event || typeof event !== 'object') return;
          if (typeof event.type === 'string') {
            record.lastUpstreamEvent = UPSTREAM_EVENT_TYPES.has(event.type) ? event.type : 'other';
            record.lastUpstreamEventAt = new Date(now()).toISOString();
          }
          if (event.type === 'tool-call' || (['text-delta', 'reasoning-delta'].includes(event.type)
              && [event.text, event.delta].some(value => typeof value === 'string' && value.length > 0))) {
            record.hasUpstreamOutput = true;
          }
          if (event.type === 'finish') {
            record.upstreamFinishReceived = true;
            record.upstreamFinishReason = UPSTREAM_FINISH_REASONS.has(event.finishReason) ? event.finishReason : 'unknown';
          }
          if (event.type === 'error') tracker.markError('upstream_error');
          if ((event.type === 'finish' || event.type === 'finish-step') && event.finishReason === 'error') {
            tracker.markError('upstream_error');
          }
          if (event.type !== 'finish-step' && event.type !== 'finish') return;
          const total = readReportedUsage(event.totalUsage);
          const usage = total || readReportedUsage(event.usage);
          if (!usage) return;
          if (total || event.type === 'finish') {
            // Final totals replace step accumulation; never add both representations.
            // Partial final reports must not erase fields already observed in steps.
            for (const field of TOKEN_FIELDS) {
              if (usage[field] !== null) record[field] = usage[field];
            }
            hasTotal = true;
          } else if (!hasTotal) {
            for (const field of TOKEN_FIELDS) {
              if (usage[field] !== null) record[field] = Math.min(Number.MAX_SAFE_INTEGER, (record[field] ?? 0) + usage[field]);
            }
          }
          record.usageReported = true;
        },
      };
      return tracker;
    },
  };
  if (disk) {
    const maintenance = setInterval(prune, 60_000);
    maintenance.unref?.();
  }
  return store;
}

export function createMonitorHandler(store, { token = process.env.CC_MONITOR_TOKEN || '',
  htmlPath = new URL('./monitor.html', import.meta.url) } = {}) {
  return async (req, res) => {
    const path = requestPath(req.url);
    if (!path.startsWith('/monitor')) return false;
    res.setHeader('Cache-Control', 'no-store');
    res.setHeader('X-Content-Type-Options', 'nosniff');
    const json = (status, error) => {
      res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ error }));
    };
    if (!token && !isLoopback(req.socket.remoteAddress)) {
      json(403, 'forbidden');
    } else if (['/monitor/api/snapshot', '/monitor/api/query', '/monitor/api/export', '/monitor/api/request'].includes(path)) {
      if (token && !tokenMatches(req.headers.authorization, token)) {
        res.setHeader('WWW-Authenticate', 'Bearer');
        json(401, 'unauthorized');
      } else if (req.method !== 'GET') {
        json(405, 'method_not_allowed');
      } else {
        try {
          const params = new URL(req.url, 'http://localhost').searchParams;
          let result;
          if (path === '/monitor/api/snapshot') result = store.snapshot();
          else if (path === '/monitor/api/query') result = store.query(params);
          else if (path === '/monitor/api/export') {
            result = store.export(params);
            res.setHeader('Content-Disposition', 'attachment; filename="monitor-history.json"');
          } else {
            const id = params.get('id');
            if ([...params.keys()].some(key => key !== 'id') || params.getAll('id').length !== 1
              || !/^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/i.test(id || '')) {
              throw new TypeError('invalid_query');
            }
            const request = store.getRequest(id);
            if (!request) { json(404, 'not_found'); return true; }
            result = { request };
          }
          res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
          res.end(JSON.stringify(result));
        } catch (error) {
          json(error instanceof TypeError ? 400 : 503, error instanceof TypeError ? 'invalid_query' : 'monitor_unavailable');
        }
      }
    } else if ((path === '/monitor' || path === '/monitor/') && req.method === 'GET') {
      try {
        const html = await readFile(htmlPath);
        res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
        res.end(html);
      } catch {
        json(503, 'monitor_unavailable');
      }
    } else {
      json(404, 'not_found');
    }
    return true;
  };
}
