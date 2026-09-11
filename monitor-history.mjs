import { createHash, randomUUID } from 'node:crypto';
import { mkdir, chmod, open, readFile, readdir, rename, stat, unlink } from 'node:fs/promises';
import { join } from 'node:path';

export const TOKEN_FIELDS = ['inputTokens', 'outputTokens', 'cachedInputTokens', 'cacheWriteTokens', 'reasoningTokens'];
const STATUSES = new Set(['pending', 'success', 'error', 'aborted']);
const text = (value, limit) => typeof value === 'string' ? Buffer.from(value.slice(0, limit)).toString() : null;
const number = value => typeof value === 'number' && Number.isFinite(value) && value >= 0
  ? Math.min(value, Number.MAX_SAFE_INTEGER) : null;
const date = value => typeof value === 'string' && Number.isFinite(Date.parse(value))
  ? new Date(value).toISOString() : null;

export function maskApiKey(apiKey) {
  if (typeof apiKey !== 'string' || !/^user_[a-zA-Z0-9_-]+$/.test(apiKey)) return null;
  const suffix = apiKey.slice(5);
  return suffix.length <= 8 ? '****' : `${suffix.slice(0, 4)}****${suffix.slice(-4)}`;
}

export function apiKeyIdentity(apiKey) {
  const keyLabel = maskApiKey(apiKey);
  return { keyId: keyLabel === null ? null : createHash('sha256').update(apiKey).digest('hex'), keyLabel };
}

// Explicit whitelist on both write and reload. Persist no request, header, body, or response objects.
export function sanitizeFinalRecord(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return null;
  const startedAt = date(value.startedAt);
  const endedAt = date(value.endedAt);
  if (!startedAt || !endedAt || !STATUSES.has(value.status) || value.status === 'pending'
      || typeof value.id !== 'string' || !value.id || value.id.length > 128) return null;
  const keyId = typeof value.keyId === 'string' && /^[a-f0-9]{64}$/.test(value.keyId) ? value.keyId : null;
  const keyLabel = keyId && typeof value.keyLabel === 'string'
    && /^(?:\*{4}|[a-zA-Z0-9_-]{4}\*{4}[a-zA-Z0-9_-]{4})$/.test(value.keyLabel) ? value.keyLabel : null;
  const record = {
    id: text(value.id, 128), startedAt, endedAt,
    method: text(value.method, 32), path: text(value.path, 2048)?.split(/[?#]/, 1)[0] ?? null,
    model: text(value.model, 256), stream: typeof value.stream === 'boolean' ? value.stream : null,
    status: value.status, httpStatus: Number.isInteger(value.httpStatus) && value.httpStatus >= 100 && value.httpStatus <= 599 ? value.httpStatus : null,
    durationMs: number(value.durationMs) ?? 0,
    requestedReasoningEffort: text(value.requestedReasoningEffort, 64), reasoningEffort: text(value.reasoningEffort, 64),
    thinkingType: text(value.thinkingType, 64), thinkingBudgetTokens: number(value.thinkingBudgetTokens),
    keyId: keyLabel === null ? null : keyId, keyLabel,
    usageReported: value.usageReported === true,
    error: typeof value.error === 'string' && /^(?:http_[45]\d\d|invalid_request_error|auth_error|authentication_error|rate_limit_error|not_found|upstream_error|temporarily_unavailable|proxy_error|internal_error|server_busy|stream_timeout|stream_error|zero_output)$/.test(value.error) ? value.error : null,
  };
  for (const field of TOKEN_FIELDS) record[field] = number(value[field]);
  return record;
}

// One in-flight atomic snapshot and one dirty bit; no queue of captured history arrays.
export function createSnapshotPersistence(directory, getSnapshot) {
  const filename = join(directory, 'history-v1.json');
  let error = null;
  let timer = null;
  let writing = null;
  let dirty = false;
  const warn = () => {
    if (!error) console.warn('[monitor] History persistence unavailable; requests remain available in memory.');
    error = 'history_persistence_unavailable';
  };
  async function prepare() {
    await mkdir(directory, { recursive: true, mode: 0o700 });
    await chmod(directory, 0o700);
  }
  async function write() {
    const temporary = join(directory, `history-v1.${randomUUID()}.tmp`);
    try {
      await prepare();
      const snapshot = getSnapshot();
      const handle = await open(temporary, 'wx', 0o600);
      try {
        await handle.writeFile(JSON.stringify(snapshot));
        await handle.sync();
      } finally {
        await handle.close();
      }
      await rename(temporary, filename);
      error = null;
      return true;
    } catch {
      warn();
      return false;
    } finally {
      await unlink(temporary).catch(() => {});
    }
  }
  function arm() {
    if (dirty && !timer && !writing) {
      timer = setTimeout(() => { timer = null; void writeOnce(); }, 1000);
      timer.unref?.();
    }
  }
  function schedule() { dirty = true; arm(); }
  function writeOnce() {
    if (writing) return writing;
    if (!dirty) return Promise.resolve();
    dirty = false;
    writing = write().then(success => {
      if (!success) dirty = true;
    }).finally(() => {
      writing = null;
      // Background changes and failed writes both wait before retrying.
      arm();
    });
    return writing;
  }
  async function flush() {
    if (timer) { clearTimeout(timer); timer = null; }
    if (writing) await writing;
    if (timer) { clearTimeout(timer); timer = null; }
    if (dirty) await writeOnce();
  }
  return {
    get error() { return error; },
    schedule, flush,
    async load() {
      try {
        await prepare();
        // A interrupted atomic write can leave a temp file. Remove only our exact temp filename format.
        for (const name of await readdir(directory)) {
          if (/^history-v1\.[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}\.tmp$/.test(name)) {
            await unlink(join(directory, name));
          }
        }
        let info;
        try { info = await stat(filename); } catch (cause) { if (cause.code === 'ENOENT') return null; throw cause; }
        if (!info.isFile() || info.size > 512 * 1024 * 1024) throw new Error('invalid history');
        const value = JSON.parse(await readFile(filename, 'utf8'));
        if (value?.version !== 1 || !Array.isArray(value.requests) || value.requests.length > 100000) throw new Error('invalid history');
        return value;
      } catch {
        warn();
        return null;
      }
    },
  };
}

function invalidQuery() { throw new TypeError('invalid_query'); }
export function parseMonitorFilters(params = new URLSearchParams()) {
  const allowed = new Set(['keyId', 'model', 'status', 'q', 'from', 'to', 'page', 'pageSize']);
  for (const key of params.keys()) if (!allowed.has(key) || params.getAll(key).length !== 1) invalidQuery();
  const getText = (key, limit) => {
    const value = params.get(key);
    if (value !== null && (!value || value.length > limit || /[\u0000-\u001f\u007f]/.test(value))) invalidQuery();
    return value;
  };
  const keyId = getText('keyId', 64);
  if (keyId !== null && keyId !== 'none' && !/^[a-f0-9]{64}$/.test(keyId)) invalidQuery();
  const status = getText('status', 16);
  if (status !== null && !STATUSES.has(status)) invalidQuery();
  const timestamp = key => {
    const value = getText(key, 40);
    if (value === null) return null;
    // Require a complete ISO timestamp with a zone; reject normalized invalid calendar dates.
    if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?(?:Z|[+-]\d{2}:\d{2})$/.test(value)) invalidQuery();
    const calendar = value.slice(0, 10);
    const time = Date.parse(value);
    if (!Number.isFinite(time) || new Date(`${calendar}T00:00:00Z`).toISOString().slice(0, 10) !== calendar
        || Number(value.slice(11, 13)) > 23 || Number(value.slice(14, 16)) > 59 || Number(value.slice(17, 19)) > 59) invalidQuery();
    return time;
  };
  const from = timestamp('from');
  const to = timestamp('to');
  if (from !== null && to !== null && from > to) invalidQuery();
  const integer = (key, fallback, max) => {
    const value = params.get(key);
    if (value === null) return fallback;
    if (!/^[1-9]\d*$/.test(value) || !Number.isSafeInteger(Number(value)) || Number(value) > max) invalidQuery();
    return Number(value);
  };
  return { keyId, model: getText('model', 256), status, q: getText('q', 512)?.toLowerCase() ?? null,
    from, to, page: integer('page', 1, Number.MAX_SAFE_INTEGER), pageSize: integer('pageSize', 15, 100) };
}

export function filterMonitorRequests(requests, filters) {
  return requests.filter(record => {
    const time = Date.parse(record.startedAt);
    return (filters.keyId === null || (filters.keyId === 'none' ? record.keyId === null : record.keyId === filters.keyId))
      && (filters.model === null || record.model === filters.model)
      && (filters.status === null || record.status === filters.status)
      && (filters.from === null || time >= filters.from) && (filters.to === null || time <= filters.to)
      && (filters.q === null || ['model', 'path', 'id', 'requestedReasoningEffort', 'reasoningEffort', 'keyLabel']
        .some(field => typeof record[field] === 'string' && record[field].toLowerCase().includes(filters.q)));
  }).sort((a, b) => Date.parse(b.startedAt) - Date.parse(a.startedAt));
}

export function queryMonitorSnapshot(snapshot, params = new URLSearchParams(), now = Date.now()) {
  const filters = parseMonitorFilters(params);
  const matching = filterMonitorRequests(snapshot.requests, filters);
  const summary = { total: matching.length, success: 0, error: 0, aborted: 0, pending: 0,
    usageReported: 0, reasoningReported: 0, inputTokens: null, outputTokens: null,
    cachedInputTokens: null, cacheWriteTokens: null, reasoningTokens: null,
    cacheHits: 0, cacheRate: null, successRate: null, avgDurationMs: null };
  let eligibleInput = 0;
  let eligibleCache = 0;
  let duration = 0;
  for (const record of matching) {
    summary[record.status]++;
    if (record.usageReported) summary.usageReported++;
    if (record.reasoningTokens !== null) summary.reasoningReported++;
    for (const field of TOKEN_FIELDS) if (record[field] !== null) summary[field] = (summary[field] ?? 0) + record[field];
    if (record.cachedInputTokens > 0) summary.cacheHits++;
    if (record.inputTokens !== null && record.cachedInputTokens !== null) {
      eligibleInput += record.inputTokens;
      eligibleCache += record.cachedInputTokens;
    }
    if (record.status !== 'pending') duration += record.durationMs;
  }
  const completed = summary.total - summary.pending;
  summary.cacheRate = eligibleInput > 0 ? eligibleCache / eligibleInput : null;
  summary.successRate = completed ? summary.success / completed : null;
  summary.avgDurationMs = completed ? duration / completed : null;
  const models = [...new Set(snapshot.requests.map(record => record.model).filter(value => value !== null))].sort();
  const keys = [...new Map(snapshot.requests.filter(record => record.keyId !== null)
    .map(record => [record.keyId, { id: record.keyId, label: record.keyLabel }])).values()]
    .sort((a, b) => a.label.localeCompare(b.label) || a.id.localeCompare(b.id));
  let end = filters.to ?? now;
  let start = filters.from ?? (matching.length ? Date.parse(matching[matching.length - 1].startedAt) : end - 30 * 60_000);
  if (filters.from === null && filters.to === null) start = Math.min(start, end - 30 * 60_000);
  if (start > end) end = start;
  const width = Math.max(1, end - start) / 30;
  const trend = Array.from({ length: 30 }, (_, i) => ({ startedAt: new Date(start + i * width).toISOString(), inputTokens: 0, outputTokens: 0 }));
  for (const record of matching) {
    const time = Date.parse(record.startedAt);
    if (time < start || time > end) continue;
    const bucket = trend[time === end ? 29 : Math.min(29, Math.max(0, Math.floor((time - start) / width)))];
    bucket.inputTokens += record.inputTokens ?? 0;
    bucket.outputTokens += record.outputTokens ?? 0;
  }
  const total = matching.length;
  const pages = Math.max(1, Math.ceil(total / filters.pageSize));
  const page = Math.min(filters.page, pages);
  const { requests, ...metadata } = snapshot;
  return { ...metadata, requests: matching.slice((page - 1) * filters.pageSize, page * filters.pageSize),
    page, pages, total, facets: { models, keys }, summary, trend };
}
