import type { Readable, Writable } from 'node:stream'
import type { MergeOptions } from './types.js'
import {
  assertNonEmptyArray,
  endWritable,
  resolveInputStream,
  throwIfAborted,
  writeToWritable,
} from './util.js'

function isWhitespaceChar(value: string): boolean {
  return value === ' ' || value === '\n' || value === '\r' || value === '\t'
}

async function streamJsonArrayContent(
  src: Readable,
  output: Writable,
  state: { outputHasContent: boolean },
  signal?: AbortSignal,
): Promise<boolean> {
  src.setEncoding('utf8')

  let started = false
  let finished = false
  let depth = 0
  let inString = false
  let escape = false
  let buffer = ''
  let inputHasContent = false

  const flush = async () => {
    if (!buffer) return
    if (!inputHasContent) {
      if (state.outputHasContent) await writeToWritable(output, ',')
      inputHasContent = true
      state.outputHasContent = true
    }
    await writeToWritable(output, buffer)
    buffer = ''
  }

  for await (const chunk of src) {
    throwIfAborted(signal, 'mergeJson')

    const text = String(chunk)
    for (let i = 0; i < text.length; i += 1) {
      const ch = text[i]

      if (!started) {
        if (isWhitespaceChar(ch)) continue
        if (ch !== '[') throw new Error('[mergeJson] Expected JSON array input')
        started = true
        depth = 1
        continue
      }

      if (finished) {
        if (!isWhitespaceChar(ch)) {
          throw new Error('[mergeJson] Unexpected data after JSON array end')
        }
        continue
      }

      if (inString) {
        buffer += ch
        if (escape) {
          escape = false
        } else if (ch === '\\') {
          escape = true
        } else if (ch === '"') {
          inString = false
        }
      } else {
        if (ch === '"') {
          inString = true
          buffer += ch
        } else if (ch === '[') {
          depth += 1
          buffer += ch
        } else if (ch === ']') {
          depth -= 1
          if (depth === 0) {
            finished = true
          } else {
            buffer += ch
          }
        } else {
          buffer += ch
        }
      }

      if (buffer.length >= 64 * 1024) {
        await flush()
      }
    }
  }

  if (!started) throw new Error('[mergeJson] Empty input')
  if (!finished || depth !== 0) throw new Error('[mergeJson] Unterminated JSON array')
  if (buffer.length > 0) await flush()

  return inputHasContent
}

/**
 * Merge multiple JSON array streams into a single JSON array stream.
 *
 * Behavior:
 * - Reads each input sequentially (preserves input order)
 * - Writes '[' once, then streams array contents from each input
 * - For each input, strips the outer '[' and ']' and concatenates elements
 * - Inserts commas between inputs when needed
 */
export async function mergeJson({ inputs, output, signal }: MergeOptions): Promise<void> {
  assertNonEmptyArray(inputs, 'mergeJson')

  const state = { outputHasContent: false }
  await writeToWritable(output, '[')

  for (let i = 0; i < inputs.length; i += 1) {
    throwIfAborted(signal, 'mergeJson')

    const src = await resolveInputStream(inputs[i])
    await streamJsonArrayContent(src, output, state, signal)
  }

  await writeToWritable(output, ']')
  await endWritable(output)
}
