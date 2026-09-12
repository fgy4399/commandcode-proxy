// Completion validity is independent of whether the provider reports token usage.
const counters = ['inputTokens', 'outputTokens', 'cachedInputTokens', 'reasoningTokens', 'cacheWriteTokens'];
const isCount = value => typeof value === 'number' && Number.isFinite(value) && value >= 0;
function reportedCount(usage, key) {
  if (isCount(usage?.[key])) return usage[key];
  const nested = key === 'cachedInputTokens' ? usage?.inputTokenDetails?.cacheReadTokens
    : key === 'reasoningTokens' ? usage?.outputTokenDetails?.reasoningTokens
    : key === 'cacheWriteTokens' ? usage?.inputTokenDetails?.cacheWriteTokens : undefined;
  return isCount(nested) ? nested : null;
}

export function createCompletionState() {
  return {
    sawFinish: false,
    hasOutput: false,
    lastEvent: null,
    finishReason: null,
    errorEvent: null,
    finishError: false,
    inputTokens: null,
    outputTokens: null,
    cachedInputTokens: null,
    reasoningTokens: null,
    cacheWriteTokens: null,
    observe(event) {
      if (!event || typeof event !== 'object' || this.sawFinish || this.errorEvent) return;
      if (typeof event.type === 'string') this.lastEvent = event.type;
      if (event.type === 'text-delta' || event.type === 'reasoning-delta') {
        const text = event.text || event.delta;
        if (typeof text === 'string' && text.trim()) this.hasOutput = true;
      } else if (event.type === 'tool-call') {
        if (typeof event.toolName === 'string' && event.toolName.trim()) this.hasOutput = true;
      } else if (event.type === 'error') {
        this.errorEvent = event;
      }
      if (event.type === 'finish-step') {
        if (event.finishReason) this.finishReason = event.finishReason;
        if (event.finishReason === 'error') this.finishError = true;
        for (const key of counters) {
          const value = reportedCount(event.usage, key);
          if (value !== null) this[key] = (this[key] ?? 0) + value;
        }
      } else if (event.type === 'finish') {
        this.sawFinish = true;
        if (event.finishReason) this.finishReason = event.finishReason;
        this.finishError ||= event.finishReason === 'error';
        // A partial final report only overwrites fields actually reported.
        for (const usage of [event.usage, event.totalUsage]) {
          for (const key of counters) {
            const value = reportedCount(usage, key);
            if (value !== null) this[key] = value;
          }
        }
      }
    },
    rejection() {
      if (this.errorEvent || this.finishError) return 'upstream_error';
      if (!this.sawFinish) return 'upstream_incomplete';
      if (this.outputTokens === 0) return 'zero_output';
      if (!this.hasOutput && !(this.outputTokens > 0)) return 'empty_response';
      return null;
    },
    openAIUsage() {
      if (this.inputTokens === null || this.outputTokens === null) return undefined;
      const usage = {
        prompt_tokens: this.inputTokens,
        completion_tokens: this.outputTokens,
        total_tokens: this.inputTokens + this.outputTokens,
      };
      if (this.cachedInputTokens !== null) usage.prompt_tokens_details = { cached_tokens: this.cachedInputTokens };
      if (this.reasoningTokens !== null) usage.completion_tokens_details = { reasoning_tokens: this.reasoningTokens };
      return usage;
    },
    diagnostics() {
      return {
        lastEvent: this.lastEvent,
        sawFinish: this.sawFinish,
        hasOutput: this.hasOutput,
        ...Object.fromEntries(counters.map(key => [key, this[key]])),
      };
    },
  };
}

// CC/OpenAI input is inclusive; Anthropic's three input buckets are disjoint.
// Do not mutate the raw usage retained by monitoring.
export function toAnthropicInputUsage(usage) {
  const total = reportedCount(usage, 'inputTokens');
  let read = reportedCount(usage, 'cachedInputTokens') ?? 0;
  let write = isCount(usage?.cacheWriteTokens) ? usage.cacheWriteTokens
    : isCount(usage?.inputTokenDetails?.cacheWriteTokens) ? usage.inputTokenDetails.cacheWriteTokens : 0;
  if (total !== null) {
    // Inconsistent provider details must neither create negative input nor exceed the total.
    read = Math.min(read, total);
    write = Math.min(write, total - read);
  }
  const noCache = usage?.inputTokenDetails?.noCacheTokens;
  // Explicit uncached usage wins, including zero. Keep cache buckets authoritative;
  // cap contradictory uncached reports to the remaining inclusive total. A lower
  // explicit report may leave some total unclassified rather than inventing usage.
  const uncached = isCount(noCache)
    ? (total === null ? noCache : Math.min(noCache, total - read - write))
    : (total === null ? 0 : total - read - write);
  return {
    input_tokens: uncached,
    cache_read_input_tokens: read,
    cache_creation_input_tokens: write,
  };
}
