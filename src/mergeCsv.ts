import type { MergeOptions } from './types.js'
import {
  assertNonEmptyArray,
  createByteCounter,
  endWritable,
  ProgressTracker,
  readUtf8Lines,
  resolveInputStream,
  throwIfAborted,
  writeToWritable,
} from './util.js'

/**
 * Merge multiple CSV streams into a single CSV stream.
 *
 * Behavior:
 * - Reads each input sequentially (preserves input order)
 * - Writes the first line (header) from the first input
 * - Skips the first line (header) for subsequent inputs if it matches the first input's header
 * - Writes all remaining lines, always ending lines with '\n'
 */
export async function mergeCsv(options: MergeOptions): Promise<void> {
  const { inputs, output, signal } = options
  assertNonEmptyArray(inputs, 'mergeCsv')

  const tracker = new ProgressTracker(options)
  let firstLine: string | undefined

  for (let i = 0; i < inputs.length; i += 1) {
    throwIfAborted(signal, 'mergeCsv')
    if (i > 0) tracker.nextInput()

    const input = inputs[i]!
    const src = await resolveInputStream(input)
    const counter = createByteCounter((n) => tracker.addBytes(n, 0))
    src.pipe(counter)

    const lines = readUtf8Lines(counter)
    const head = await lines.next()

    if (!head.done) {
      if (i === 0) {
        firstLine = head.value
        const chunk = `${head.value}\n`
        await writeToWritable(output, chunk)
        tracker.addBytes(0, Buffer.byteLength(chunk))
      } else {
        // Skip repeated header only if it matches the first chunk's first line.
        if (firstLine === undefined || head.value !== firstLine) {
          const chunk = `${head.value}\n`
          await writeToWritable(output, chunk)
          tracker.addBytes(0, Buffer.byteLength(chunk))
        }
      }
    }

    for await (const line of lines) {
      throwIfAborted(signal, 'mergeCsv')
      const chunk = `${line}\n`
      await writeToWritable(output, chunk)
      tracker.addBytes(0, Buffer.byteLength(chunk))
    }
  }

  tracker.flush()
  await endWritable(output)
}
