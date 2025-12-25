import type { Writable } from 'node:stream'
import type { InputSource } from './types.js'
import { once } from 'node:events'
import { Readable } from 'node:stream'

export function assertNonEmptyArray(inputs: unknown[], label: string): void {
  if (!Array.isArray(inputs) || inputs.length === 0)
    throw new Error(`[${label}] inputs must be a non-empty array`)
}

export function throwIfAborted(signal: AbortSignal | undefined, label: string): void {
  if (signal?.aborted) throw new Error(`[${label}] Aborted`)
}

export function isHttpUrl(value: string): boolean {
  return /^https?:\/\//i.test(value)
}

function toNodeReadable(body: unknown, label = '[merge-streams]'): Readable {
  if (!body)
    throw new Error(`${label} fetch response body is empty`)

  // If it's already a Node.js stream, return it as-is.
  if (body instanceof Readable || (body && typeof (body as Readable).pipe === 'function' && typeof (body as Readable).on === 'function'))
    return body as Readable

  // Otherwise, expect a WHATWG ReadableStream and convert to Node Readable.
  const fromWeb = (Readable as unknown as { fromWeb?: (stream: unknown) => Readable }).fromWeb

  if (!fromWeb)
    throw new Error(`${label} Readable.fromWeb is not available (Node 18+ required)`)

  return fromWeb(body)
}

export async function openUrlAsReadable(
  url: string,
  signal?: AbortSignal,
  label = '[merge-streams]',
): Promise<Readable> {
  if (!isHttpUrl(url)) {
    throw new Error(`${label} Expected http(s) URL but got: ${url}`)
  }

  if (typeof fetch !== 'function') {
    throw new Error(`${label} fetch is not available`)
  }

  const res = await fetch(url, { method: 'GET', redirect: 'follow', signal })
  if (!res.ok) throw new Error(`${label} Failed to fetch '${url}': ${res.status} ${res.statusText}`)

  const body = toNodeReadable(res.body, label)

  // Ensure we don't eagerly buffer body bytes before the consumer is ready.
  // (Readable.fromWeb may start flowing once piped; keep it paused by default.)
  if (typeof body.pause === 'function')
    body.pause()

  return body
}

export async function writeToWritable(output: Writable, chunk: string | Buffer): Promise<void> {
  const o = output as Writable & { destroyed?: boolean; writableDestroyed?: boolean }
  if (o.destroyed || o.writableDestroyed) throw new Error('[merge-streams] output is destroyed')
  const ok = output.write(chunk)
  if (!ok) await once(output, 'drain')
}

export async function endWritable(output: Writable): Promise<void> {
  if (output.writableEnded || output.writableFinished) return

  const done = Promise.race([
    once(output, 'finish'),
    once(output, 'close'),
    once(output, 'error').then(([e]) => {
      throw e
    }),
  ])
  output.end()
  await done
}

export async function* readUtf8Lines(src: Readable): AsyncGenerator<string> {
  src.setEncoding('utf8')

  let carry = ''
  for await (const chunk of src) {
    carry += String(chunk)

    while (true) {
      const lf = carry.indexOf('\n')
      if (lf === -1) break

      let line = carry.slice(0, lf)
      if (line.endsWith('\r')) line = line.slice(0, -1)
      yield line
      carry = carry.slice(lf + 1)
    }
  }

  if (carry.length > 0) {
    if (carry.endsWith('\r')) carry = carry.slice(0, -1)
    yield carry
  }
}

/**
 * Normalize input sources to readable streams.
 * Supports: Readable, sync factory, async factory
 */
export async function resolveInputStream(source: InputSource): Promise<Readable> {
  if (source instanceof Readable) {
    return source
  }
  if (typeof source === 'function') {
    const result = source()
    return result instanceof Promise ? await result : result
  }
  throw new Error('[merge-streams] Invalid input source')
}
