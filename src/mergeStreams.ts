import type { InputSource, MergeFormat, MergeOptions, MergeUrlsOptions } from './types.js'
import { mergeArrow } from './mergeArrow.js'
import { mergeCsv } from './mergeCsv.js'
import { mergeJson } from './mergeJson.js'
import { isHttpUrl, openUrlAsReadable } from './util.js'

/**
 * Unified entry point for merging multiple data streams into a single output stream.
 */
export async function mergeStreams(format: MergeFormat, options: MergeOptions): Promise<void> {
  switch (format) {
    case 'ARROW_STREAM':
      return mergeArrow(options)
    case 'CSV':
      return mergeCsv(options)
    case 'JSON_ARRAY':
      return mergeJson(options)
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
 */
export async function mergeStreamsFromUrls(format: MergeFormat, { urls, ...options }: MergeUrlsOptions): Promise<void> {
  if (!Array.isArray(urls) || urls.length === 0)
    throw new Error('[mergeStreamsFromUrls] urls must be a non-empty array')

  for (const url of urls) {
    if (!isHttpUrl(url)) throw new Error(`[mergeStreamsFromUrls] Expected http(s) URL but got: ${url}`)
  }

  const inputs: InputSource[] = urls.map(url => () => openUrlAsReadable(url, options.signal, `[mergeStreams:${format}]`))
  return mergeStreams(format, { inputs, ...options })
}
