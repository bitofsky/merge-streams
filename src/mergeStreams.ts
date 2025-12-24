import { Writable } from 'node:stream'

import { mergeArrow } from './mergeArrow.js'
import { mergeCsv } from './mergeCsv.js'
import { mergeJson } from './mergeJson.js'
import { type InputSource, isHttpUrl, openUrlAsReadable } from './util.js'

export type MergeFormat = 'ARROW_STREAM' | 'CSV' | 'JSON_ARRAY'

export interface MergeStreamsOptions {
  signal?: AbortSignal
}

/**
 * Unified entry point for merging multiple data streams into a single output stream.
 *
 * @param format - The format of the input data ('ARROW_STREAM', 'CSV', or 'JSON_ARRAY')
 * @param inputs - Array of Readable streams or factory functions that return Readable streams
 * @param output - Writable stream for the merged output
 * @param options - Optional settings (e.g., AbortSignal)
 */
export async function mergeStreams(
  format: MergeFormat,
  inputs: InputSource[],
  output: Writable,
  options: MergeStreamsOptions = {},
): Promise<void> {
  switch (format) {
    case 'ARROW_STREAM':
      return mergeArrow(inputs, output, options)
    case 'CSV':
      return mergeCsv(inputs, output, options)
    case 'JSON_ARRAY':
      return mergeJson(inputs, output, options)
    default: {
      const neverFormat: never = format
      throw new Error(`[mergeStreams] Unsupported format: ${String(neverFormat)}`)
    }
  }
}

/**
 * Unified entry point for merging multiple data files from URLs into a single output stream.
 *
 * Convenience wrapper around mergeStreams that fetches from http(s) URLs.
 *
 * @param format - The format of the input files ('ARROW_STREAM', 'CSV', or 'JSON_ARRAY')
 * @param urls - Array of http(s) URLs
 * @param output - Writable stream for the merged output
 * @param options - Optional settings (e.g., AbortSignal)
 */
export async function mergeStreamsFromUrls(
  format: MergeFormat,
  urls: string[],
  output: Writable,
  options: MergeStreamsOptions = {},
): Promise<void> {
  if (!Array.isArray(urls) || urls.length === 0)
    throw new Error('[mergeStreamsFromUrls] urls must be a non-empty array')

  for (const url of urls) {
    if (!isHttpUrl(url)) throw new Error(`[mergeStreamsFromUrls] Expected http(s) URL but got: ${url}`)
  }

  const inputs: InputSource[] = urls.map(url => () => openUrlAsReadable(url, options.signal, `[mergeStreams:${format}]`))

  return mergeStreams(format, inputs, output, options)
}

// Re-export for convenience
export { isHttpUrl, openUrlAsReadable, type InputSource }
