import { PassThrough } from 'node:stream'
import { describe, it, expect } from 'vitest'
import { mergeJson } from '../src/mergeJson.js'
import { mergeStreamsFromUrls } from '../src/mergeStreams.js'
import { collectToString, createLocalHttpServer } from './testUtil.js'

describe('mergeJson (Readable[] -> Writable)', () => {
  it('merges json arrays from readable streams', async () => {
    const routes = new Map<string, string>([
      ['/j0.json', '[{"a":1},{"b":2}]'],
      ['/j1.json', '[{"c":3}]'],
    ])

    const { baseUrl, close } = await createLocalHttpServer(routes, {
      contentType: 'application/json; charset=utf-8',
    })

    try {
      const pass = new PassThrough()
      const outPromise = collectToString(pass)

      await mergeJson({
        inputs: [
          async () => {
            const res = await fetch(`${baseUrl}/j0.json`)
            const { Readable } = await import('node:stream')
            return Readable.fromWeb(res.body as any)
          },
          async () => {
            const res = await fetch(`${baseUrl}/j1.json`)
            const { Readable } = await import('node:stream')
            return Readable.fromWeb(res.body as any)
          },
        ],
        output: pass,
      })

      const out = await outPromise
      expect(JSON.parse(out)).toEqual([{ a: 1 }, { b: 2 }, { c: 3 }])
    } finally {
      await close()
    }
  })
})

describe('mergeStreamsFromUrls JSON_ARRAY (http URLs -> Writable)', () => {
  it('merges json array chunks preserving element order', async () => {
    const routes = new Map<string, string>([
      ['/j0.json', '[["a",1],["b",2]]'],
      ['/j1.json', '[["c",3]]'],
    ])

    const { baseUrl, close } = await createLocalHttpServer(routes, {
      contentType: 'application/json; charset=utf-8',
    })
    const urls = [`${baseUrl}/j0.json`, `${baseUrl}/j1.json`]

    try {
      const pass = new PassThrough()
      const outPromise = collectToString(pass)

      await mergeStreamsFromUrls('JSON_ARRAY', { urls, output: pass })

      const out = await outPromise
      expect(JSON.parse(out)).toEqual([
        ['a', 1],
        ['b', 2],
        ['c', 3],
      ])
    } finally {
      await close()
    }
  })

  it('handles empty arrays correctly', async () => {
    const routes = new Map<string, string>([
      ['/j0.json', '[]'],
      ['/j1.json', '[1,2]'],
    ])

    const { baseUrl, close } = await createLocalHttpServer(routes, {
      contentType: 'application/json; charset=utf-8',
    })
    const urls = [`${baseUrl}/j0.json`, `${baseUrl}/j1.json`]

    try {
      const pass = new PassThrough()
      const outPromise = collectToString(pass)

      await mergeStreamsFromUrls('JSON_ARRAY', { urls, output: pass })

      const out = await outPromise
      expect(JSON.parse(out)).toEqual([1, 2])
    } finally {
      await close()
    }
  })

  it('rejects non-http inputs', async () => {
    const pass = new PassThrough()
    await expect(mergeStreamsFromUrls('JSON_ARRAY', { urls: ['file:///tmp/a.json'], output: pass })).rejects.toThrow(
      /Expected http\(s\) URL/,
    )
  })
})
