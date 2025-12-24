import { Readable, Writable } from 'node:stream'
import {
  endWritable,
  readUtf8Lines,
  writeToWritable,
  type InputSource,
  resolveInputStream,
} from './util.js'

export interface MergeCsvOptions {
  signal?: AbortSignal
}

/**
 * Merge multiple CSV streams into a single CSV stream.
 *
 * Behavior:
 * - Reads each input sequentially (preserves input order)
 * - Writes the first line (header) from the first input
 * - Skips the first line (header) for subsequent inputs if it matches the first input's header
 * - Writes all remaining lines, always ending lines with '\n'
 *
 * @param inputs - Array of Readable streams or factory functions that return Readable streams
 * @param output - Writable stream for the merged output
 * @param options - Optional settings (e.g., AbortSignal)
 */
export async function mergeCsv(
  inputs: InputSource[],
  output: Writable,
  options: MergeCsvOptions = {},
): Promise<void> {
  if (!Array.isArray(inputs) || inputs.length === 0)
    throw new Error('[mergeCsv] inputs must be a non-empty array')

  let firstLine: string | undefined

  for (let i = 0; i < inputs.length; i += 1) {
    if (options.signal?.aborted) throw new Error('[mergeCsv] Aborted')

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
      if (options.signal?.aborted) throw new Error('[mergeCsv] Aborted')
      await writeToWritable(output, `${line}\n`)
    }
  }

  await endWritable(output)
}

