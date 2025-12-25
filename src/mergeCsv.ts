import type { MergeOptions } from './types.js'
import {
  assertNonEmptyArray,
  endWritable,
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
export async function mergeCsv({ inputs, output, signal }: MergeOptions): Promise<void> {
  assertNonEmptyArray(inputs, 'mergeCsv')

  let firstLine: string | undefined

  for (let i = 0; i < inputs.length; i += 1) {
    throwIfAborted(signal, 'mergeCsv')

    const src = await resolveInputStream(inputs[i])
    const lines = readUtf8Lines(src)
    const head = await lines.next()

    if (!head.done) {
      if (i === 0) {
        firstLine = head.value
        await writeToWritable(output, `${head.value}\n`)
      } else {
        // Skip repeated header only if it matches the first chunk's first line.
        if (firstLine === undefined || head.value !== firstLine) {
          await writeToWritable(output, `${head.value}\n`)
        }
      }
    }

    for await (const line of lines) {
      throwIfAborted(signal, 'mergeCsv')
      await writeToWritable(output, `${line}\n`)
    }
  }

  await endWritable(output)
}
