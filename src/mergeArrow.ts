import type { MergeOptions } from './types.js'
import { pipeline } from 'node:stream/promises'
import { RecordBatchReader, RecordBatchStreamWriter } from 'apache-arrow'
import { assertNonEmptyArray, createByteCounter, ProgressTracker, resolveInputStream, throwIfAborted } from './util.js'

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
export async function mergeArrow(options: MergeOptions): Promise<void> {
  const { inputs, output, signal } = options
  assertNonEmptyArray(inputs, 'mergeArrow')

  const tracker = new ProgressTracker(options)
  const writer = new RecordBatchStreamWriter({ autoDestroy: true })
  const encoded = writer.toNodeStream({ objectMode: false })
  const outputCounter = createByteCounter((n) => tracker.addBytes(0, n))
  const pipePromise = pipeline(encoded, outputCounter, output)

  async function* batches() {
    for (let i = 0; i < inputs.length; i += 1) {
      throwIfAborted(signal, 'mergeArrow')
      if (i > 0) tracker.nextInput()

      const input = inputs[i]!
      const src = await resolveInputStream(input)
      const inputCounter = createByteCounter((n) => tracker.addBytes(n, 0))
      src.pipe(inputCounter)

      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const reader = await RecordBatchReader.from(inputCounter as any)
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
    tracker.flush()
  } catch (e) {
    try {
      writer.abort(e as Error)
    } catch { /* ignore */ }
    throw e
  }
}
