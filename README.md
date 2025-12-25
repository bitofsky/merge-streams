# @bitofsky/merge-streams

**When Databricks gives you 90+ presigned URLs, merge them into one.**

> *Because nobody wants to explain to their MCP client why it needs to juggle dozens of chunk URLs.*

---

## Why I Made This

I was building an MCP Server that queries Databricks SQL for large datasets. I chose External Links format because INLINE would blow up memory.

But then Databricks handed me back something like this:

```
chunk_0.arrow (presigned URL)
chunk_1.arrow (presigned URL)
chunk_2.arrow (presigned URL)
...
chunk_89.arrow (presigned URL)
```

My client would have to:
1. Fetch each chunk sequentially
2. Parse and merge them correctly (CSV headers? JSON array brackets? Arrow EOS markers?)
3. Handle errors across 90 HTTP requests
4. Pray nothing times out

That was unacceptable. So I built this.

---

## The Solution

`merge-streams` takes those chunked External Links and merges them into a single, unified stream.

```
90+ presigned URLs → merge-streams → 1 clean stream → S3 → 1 presigned URL
```

Now my MCP client gets one URL. Done.

### What Makes It Fast

- **Pre-connected**: Next chunk's connection opens while current chunk streams. No idle time.
- **Zero accumulation**: Pure stream piping. Memory stays flat regardless of data size.
- **Format-aware**: Not byte concatenation — actual format understanding.

---

## Features

- **CSV**: Automatically deduplicates headers across chunks
- **JSON_ARRAY**: Properly concatenates JSON arrays (handles brackets and commas)
- **ARROW_STREAM**: Merges Arrow IPC streams batch-by-batch (doesn't just byte-concat)
- **Memory-efficient**: Streaming-based, never loads entire files into memory
- **AbortSignal support**: Cancel mid-stream when needed
- **Progress tracking**: Monitor merge progress with byte-level granularity

---

## Installation

```bash
npm install @bitofsky/merge-streams
```

Requires Node.js 20+ (uses native `fetch()` and `Readable.fromWeb()`)

---

## Quick Start: The Databricks Use Case

See [test/databricks.spec.ts](test/databricks.spec.ts) for a complete working example.

```bash
# Run the integration test
DATABRICKS_TOKEN=dapi... \
DATABRICKS_HOST=xxx.cloud.databricks.com \
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/xxx \
npm test -- test/databricks.spec.ts
```

---

## API

### URL-based (for Databricks External Links)

```ts
import { mergeStreamsFromUrls } from '@bitofsky/merge-streams'

await mergeStreamsFromUrls('CSV', { urls, output })
await mergeStreamsFromUrls('JSON_ARRAY', { urls, output })
await mergeStreamsFromUrls('ARROW_STREAM', { urls, output })
```

### With AbortSignal

```ts
const controller = new AbortController()

await mergeStreamsFromUrls('CSV', {
  urls,
  output,
  signal: controller.signal,
})

// Cancel anytime
controller.abort()
```

### With Progress Tracking

```ts
await mergeStreamsFromUrls('CSV', {
  urls,
  output,
  onProgress: ({ inputIndex, totalInputs, inputedBytes, mergedBytes }) => {
    console.log(`Processing ${inputIndex + 1}/${totalInputs}: ${inputedBytes} bytes read, ${mergedBytes} bytes merged`)
  },
})
```

### Stream-based (for custom input sources)

```ts
import { mergeStreams, mergeCsv, mergeJson, mergeArrow } from '@bitofsky/merge-streams'

// Using unified API
await mergeStreams('CSV', { inputs, output })

// Or use format-specific functions directly
await mergeCsv({ inputs, output, signal })
await mergeJson({ inputs, output, signal })
await mergeArrow({ inputs, output, signal })
```

Inputs can be:
- `Readable` streams directly
- Sync factories: `() => Readable`
- Async factories: `() => Promise<Readable>` (recommended for lazy fetching)

---

## Format Details

| Format | Behavior |
|--------|----------|
| `CSV` | Writes header once, skips duplicate headers from subsequent chunks |
| `JSON_ARRAY` | Wraps in `[]`, strips brackets from chunks, inserts commas |
| `ARROW_STREAM` | Re-encodes RecordBatches into single IPC stream (not byte-concat) |

---

## Types

```ts
import type { Readable, Writable } from 'node:stream'

type MergeFormat = 'ARROW_STREAM' | 'CSV' | 'JSON_ARRAY'
type InputSource = Readable | (() => Readable) | (() => Promise<Readable>)

interface MergeOptions {
  inputs: InputSource[]
  output: Writable
  signal?: AbortSignal
  onProgress?: (progress: MergeOptionsProgress) => void
  progressIntervalMs?: number  // Throttle interval (default: 1000, 0 = no throttle)
}

interface MergeOptionsProgress {
  inputIndex: number    // Index of the input being processed
  totalInputs: number   // Total number of inputs
  inputedBytes: number  // Total bytes read from all inputs
  mergedBytes: number   // Total bytes written to output
}

function mergeStreams(
  format: MergeFormat,
  options: MergeOptions
): Promise<void>

function mergeStreamsFromUrls(
  format: MergeFormat,
  options: { urls: string[]; output: Writable; signal?: AbortSignal; onProgress?: (progress: MergeOptionsProgress) => void; progressIntervalMs?: number }
): Promise<void>
```

---

## Why Not Just Byte-Concatenate?

- **CSV**: You'd get duplicate headers scattered throughout
- **JSON_ARRAY**: `[1,2][3,4]` is not valid JSON
- **Arrow**: Most Arrow readers stop at the first EOS marker

Each format needs format-aware merging. That's what this library does.

---

## Scope

This library was born from a specific pain point: making Databricks External Links usable in MCP Server development. It does that one thing well.

If you have other use cases in mind, PRs are welcome.

---

## License

MIT
