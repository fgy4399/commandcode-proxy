import test from 'node:test';
import assert from 'node:assert/strict';
import { toAnthropicInputUsage } from '../completion-state.mjs';

test('Anthropic input buckets partition CC input without modifying raw usage', () => {
  const raw = {inputTokens: 6786, outputTokens: 450, cachedInputTokens: 6528};
  const original = structuredClone(raw);
  assert.deepEqual(toAnthropicInputUsage(raw), {input_tokens: 258, cache_read_input_tokens: 6528, cache_creation_input_tokens: 0});
  assert.deepEqual(raw, original);
  assert.deepEqual(toAnthropicInputUsage({inputTokens: 120, inputTokenDetails: {cacheReadTokens: 80, cacheWriteTokens: 12}}), {
    input_tokens: 28, cache_read_input_tokens: 80, cache_creation_input_tokens: 12,
  });
});

test('invalid and inconsistent cache counters never produce negative or double-counted input', () => {
  for (const raw of [
    {inputTokens: 10, cachedInputTokens: 20, cacheWriteTokens: 30},
    {inputTokens: 10, cachedInputTokens: 8, cacheWriteTokens: 9},
    {inputTokens: 10, cachedInputTokens: NaN, cacheWriteTokens: -1},
    {inputTokens: 0, cachedInputTokens: 80, cacheWriteTokens: 12},
  ]) {
    const result = toAnthropicInputUsage(raw);
    assert.ok(Object.values(result).every(value => Number.isFinite(value) && value >= 0));
    assert.equal(result.input_tokens + result.cache_read_input_tokens + result.cache_creation_input_tokens, raw.inputTokens);
  }
  assert.deepEqual(toAnthropicInputUsage({inputTokens: 20, cachedInputTokens: 0, inputTokenDetails: {cacheReadTokens: 15}}), {
    input_tokens: 20, cache_read_input_tokens: 0, cache_creation_input_tokens: 0,
  });
  assert.deepEqual(toAnthropicInputUsage(null), {input_tokens: 0, cache_read_input_tokens: 0, cache_creation_input_tokens: 0});
});

test('upstream noCacheTokens is retained without an inclusive total and checked when totals exist', () => {
  const raw = {outputTokens: 450, inputTokenDetails: {noCacheTokens: 258, cacheReadTokens: 6528}};
  const original = structuredClone(raw);
  assert.deepEqual(toAnthropicInputUsage(raw), {input_tokens: 258, cache_read_input_tokens: 6528, cache_creation_input_tokens: 0});
  assert.deepEqual(raw, original);
  assert.deepEqual(toAnthropicInputUsage({inputTokenDetails: {noCacheTokens: 0, cacheReadTokens: 100}}), {
    input_tokens: 0, cache_read_input_tokens: 100, cache_creation_input_tokens: 0,
  });
  for (const noCacheTokens of [NaN, Infinity, -1, '258']) {
    assert.equal(toAnthropicInputUsage({inputTokens: 6786, cachedInputTokens: 6528, inputTokenDetails: {noCacheTokens}}).input_tokens, 258);
  }
  const consistent = {inputTokens: 120, cachedInputTokens: 80, inputTokenDetails: {cacheWriteTokens: 12, noCacheTokens: 28}};
  assert.equal(toAnthropicInputUsage(consistent).input_tokens, 28);
  // A contradictory optional detail must not create extra billed input beyond a known total.
  const bounded = toAnthropicInputUsage({...consistent, inputTokenDetails: {cacheWriteTokens: 12, noCacheTokens: 120}});
  assert.equal(bounded.input_tokens + bounded.cache_read_input_tokens + bounded.cache_creation_input_tokens, 120);
});
