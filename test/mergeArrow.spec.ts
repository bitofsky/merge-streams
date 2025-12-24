import fs from 'node:fs'
import fsp from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'
import { Readable } from 'node:stream'

import { RecordBatchReader, RecordBatchStreamWriter, Table, Utf8, vectorFromArray } from 'apache-arrow'
import { describe, it, expect } from 'vitest'

import { mergeArrow } from '../src/mergeArrow.js'
import { mergeStreamsFromUrls } from '../src/mergeStreams.js'
import { createLocalHttpServer } from '../src/util.js'

async function countArrowStreamRows(src: Readable | NodeJS.ReadableStream): Promise<number> {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const reader = await RecordBatchReader.from(src as any)

  let rows = 0
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  for await (const batch of reader as any) {
    rows += batch.numRows
  }

  return rows
}

describe('mergeArrow (Readable[] -> Writable)', () => {
  it('merges arrow streams from factory functions', async () => {
    const t1 = new Table({
      a: vectorFromArray([1, 2]),
      b: vectorFromArray(['x', 'y'], new Utf8()),
    })
    const t2 = new Table({
      a: vectorFromArray([3, 4]),
      b: vectorFromArray(['z', 'w'], new Utf8()),
    })

    const bytes1 = new RecordBatchStreamWriter({ autoDestroy: true }).writeAll(t1).toUint8Array(true)
    const bytes2 = new RecordBatchStreamWriter({ autoDestroy: true }).writeAll(t2).toUint8Array(true)

    const routes = new Map<string, Buffer>([
      ['/chunk0.arrow', Buffer.from(bytes1)],
      ['/chunk1.arrow', Buffer.from(bytes2)],
    ])

    const { baseUrl, close } = await createLocalHttpServer(routes, {
      contentType: 'application/vnd.apache.arrow.stream',
    })

    const outPath = path.join(
      os.tmpdir(),
      `arrow-merge-${Date.now()}-${Math.random().toString(16).slice(2)}.arrow`,
    )

    try {
      // Use async factory functions
      const dst = fs.createWriteStream(outPath)
      await mergeArrow(
        [
          async () => {
            const res = await fetch(`${baseUrl}/chunk0.arrow`)
            return Readable.fromWeb(res.body as any)
          },
          async () => {
            const res = await fetch(`${baseUrl}/chunk1.arrow`)
            return Readable.fromWeb(res.body as any)
          },
        ],
        dst,
      )

      const src = fs.createReadStream(outPath)
      const rows = await countArrowStreamRows(src)
      expect(rows).toBe(4)
    } finally {
      await close()
      await fsp.rm(outPath, { force: true })
    }
  })
})

describe('mergeStreamsFromUrls ARROW_STREAM (http URLs -> Writable)', () => {
  it('merges arrow chunks in order and results in 4 rows', async () => {
    const t1 = new Table({
      a: vectorFromArray([1, 2]),
      b: vectorFromArray(['x', 'y'], new Utf8()),
    })
    const t2 = new Table({
      a: vectorFromArray([3, 4]),
      b: vectorFromArray(['z', 'w'], new Utf8()),
    })

    const bytes1 = new RecordBatchStreamWriter({ autoDestroy: true }).writeAll(t1).toUint8Array(true)
    const bytes2 = new RecordBatchStreamWriter({ autoDestroy: true }).writeAll(t2).toUint8Array(true)
    const tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), 'arrow-merge-'))

    try {
      const routes = new Map<string, Buffer>([
        ['/chunk0.arrow', Buffer.from(bytes1)],
        ['/chunk1.arrow', Buffer.from(bytes2)],
      ])

      const { baseUrl, close } = await createLocalHttpServer(routes, {
        contentType: 'application/vnd.apache.arrow.stream',
      })

      const outPath = path.join(
        tmpDir,
        `arrow-merge-${Date.now()}}.arrow`,
      )

      try {
        const urls = [`${baseUrl}/chunk0.arrow`, `${baseUrl}/chunk1.arrow`]
        const dst = fs.createWriteStream(outPath)
        await mergeStreamsFromUrls('ARROW_STREAM', urls, dst)

        const src = fs.createReadStream(outPath)
        const rows = await countArrowStreamRows(src)
        expect(rows).toBe(4)
      } finally {
        await close()
        await fsp.rm(outPath, { force: true })
      }
    } finally {
      await fsp.rm(tmpDir, { recursive: true, force: true })
    }
  })
})
