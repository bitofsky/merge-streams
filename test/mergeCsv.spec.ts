import { PassThrough } from 'node:stream'
import { describe, it, expect } from 'vitest'
import { mergeCsv } from '../src/mergeCsv.js'
import { mergeStreamsFromUrls } from '../src/mergeStreams.js'
import { createLocalHttpServer } from '../src/util.js'

async function collectToString(stream: NodeJS.ReadableStream): Promise<string> {
  const chunks: Buffer[] = []
  for await (const chunk of stream as AsyncIterable<Buffer | string>) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(String(chunk)))
  }
  return Buffer.concat(chunks).toString('utf8')
}

describe('mergeCsv (Readable[] -> Writable)', () => {
  it('merges csv from readable streams', async () => {
    const routes = new Map<string, string>([
      ['/c0.csv', 'a,b\n1,2\n3,4\n'],
      ['/c1.csv', 'a,b\n5,6\n7,8'],
    ])

    const { baseUrl, close } = await createLocalHttpServer(routes, {
      contentType: 'text/csv; charset=utf-8',
    })

    try {
      const pass = new PassThrough()
      const outPromise = collectToString(pass)

      // Use factory functions to create streams
      await mergeCsv(
        [
          async () => {
            const res = await fetch(`${baseUrl}/c0.csv`)
            const { Readable } = await import('node:stream')
            return Readable.fromWeb(res.body as any)
          },
          async () => {
            const res = await fetch(`${baseUrl}/c1.csv`)
            const { Readable } = await import('node:stream')
            return Readable.fromWeb(res.body as any)
          },
        ],
        pass,
      )

      const out = await outPromise
      expect(out).toBe('a,b\n1,2\n3,4\n5,6\n7,8\n')
    } finally {
      await close()
    }
  })
})

describe('mergeStreamsFromUrls CSV (http URLs -> Writable)', () => {
  it('merges csv chunks when subsequent chunks are headerless', async () => {
    const routes = new Map<string, string>([
      ['/c0.csv', 'a,b\n1,2\n3,4\n'],
      ['/c1.csv', '5,6\n7,8'],
    ])

    const { baseUrl, close } = await createLocalHttpServer(routes, {
      contentType: 'text/csv; charset=utf-8',
    })
    const urls = [`${baseUrl}/c0.csv`, `${baseUrl}/c1.csv`]

    try {
      const pass = new PassThrough()
      const outPromise = collectToString(pass)

      await mergeStreamsFromUrls('CSV', urls, pass)

      const out = await outPromise
      expect(out).toBe('a,b\n1,2\n3,4\n5,6\n7,8\n')
    } finally {
      await close()
    }
  })

  it('deduplicates headers when subsequent chunks have the same header', async () => {
    const routes = new Map<string, string>([
      ['/c0.csv', 'a,b\n1,2\n'],
      ['/c1.csv', 'a,b\n3,4\n'],
    ])

    const { baseUrl, close } = await createLocalHttpServer(routes, {
      contentType: 'text/csv; charset=utf-8',
    })
    const urls = [`${baseUrl}/c0.csv`, `${baseUrl}/c1.csv`]

    try {
      const pass = new PassThrough()
      const outPromise = collectToString(pass)

      await mergeStreamsFromUrls('CSV', urls, pass)

      const out = await outPromise
      expect(out).toBe('a,b\n1,2\n3,4\n')
    } finally {
      await close()
    }
  })

  it('rejects non-http inputs', async () => {
    const pass = new PassThrough()
    await expect(mergeStreamsFromUrls('CSV', ['file:///tmp/a.csv'], pass)).rejects.toThrow(
      /Expected http\(s\) URL/,
    )
  })
})
