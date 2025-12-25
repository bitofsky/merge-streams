// Types
export type {
  InputSource,
  MergeFormat,
  MergeOptions,
} from './types.js'

// Core merge functions (stream-based)
export { mergeArrow } from './mergeArrow.js'
export { mergeCsv } from './mergeCsv.js'
export { mergeJson } from './mergeJson.js'

// Unified API
export { mergeStreams, mergeStreamsFromUrls } from './mergeStreams.js'

// Utilities
export { openUrlAsReadable, isHttpUrl } from './util.js'
