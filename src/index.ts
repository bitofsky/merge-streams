// Core merge functions (stream-based)
export { mergeArrow, type MergeArrowOptions } from './mergeArrow.js'
export { mergeCsv, type MergeCsvOptions } from './mergeCsv.js'
export { mergeJson, type MergeJsonOptions } from './mergeJson.js'

// Unified API
export {
  mergeStreams,
  mergeStreamsFromUrls,
  type MergeFormat,
  type MergeStreamsOptions,
  type InputSource,
} from './mergeStreams.js'

// Utilities
export { createLocalHttpServer, openUrlAsReadable, isHttpUrl } from './util.js'
