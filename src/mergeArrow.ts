import type { MergeOptions } from './types.js'
import { pipeline } from 'node:stream/promises'
import { RecordBatchReader, RecordBatchStreamWriter } from 'apache-arrow'
import { assertNonEmptyArray, resolveInputStream, throwIfAborted } from './util.js'

/**
 * Merge multiple Apache Arrow IPC streams into one IPC stream.
 *
 * - Reads each input sequentially as a stream
 * - Decodes record batches and re-encodes them into a single output Arrow IPC stream
 * - Memory efficient (batch-by-batch); avoids materializing full tables
 *
 * Notes:
 * - This does NOT byte-concatenate inputs (consumers usually stop at first EOS).
 * - Assumes all input streams have identical schema.
 */
export async function mergeArrow({ inputs, output, signal }: MergeOptions): Promise<void> {
  assertNonEmptyArray(inputs, 'mergeArrow')

  const writer = new RecordBatchStreamWriter({ autoDestroy: true })
  const encoded = writer.toNodeStream({ objectMode: false })
  const pipePromise = pipeline(encoded, output)

  async function* batches() {
    for (let i = 0; i < inputs.length; i += 1) {
      throwIfAborted(signal, 'mergeArrow')
      const input = inputs[i]!
      const src = await resolveInputStream(input)

      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const reader = await RecordBatchReader.from(src as any)
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const it = (reader as any)[Symbol.asyncIterator]?.() as AsyncIterator<any> | undefined
      if (!it)
        throw new Error('[mergeArrow] Reader is not async-iterable')

      while (true) {
        throwIfAborted(signal, 'mergeArrow')
        const next = await it.next()

        if (next.done) break
        const batch = next.value

        yield batch
      }
    }
  }

  const writeAllPromise = (async () => {
    try {
      await writer.writeAll(batches())
    } catch (e) {
      writer.abort(e as Error)
      throw e
    }
  })()

  try {
    await Promise.all([pipePromise, writeAllPromise])
  } catch (e) {
    try {
      writer.abort(e as Error)
    } catch { /* ignore */ }
    throw e
  }
}
