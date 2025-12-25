import type { Readable, Writable } from 'node:stream'

/** Input source types */
export type InputSource = Readable | (() => Readable) | (() => Promise<Readable>)

/** Format types */
export type MergeFormat = 'ARROW_STREAM' | 'CSV' | 'JSON_ARRAY'

/** Options types */
export type MergeOptions = {
    /** Input sources */
    inputs: InputSource[]
    /** Output writable stream */
    output: Writable
    /** Optional abort signal */
    signal?: AbortSignal
    /** Optional progress callback */
    onProgress?: (progress: MergeOptionsProgress) => void
    /** Progress callback interval in milliseconds (default: 1000, 0 = emit on every update) */
    progressIntervalMs?: number
}

/** Progress callback parameter types */
export type MergeOptionsProgress = {
    /** Index of the input being processed */
    inputIndex: number
    /** Total number of inputs */
    totalInputs: number
    /** Total number of bytes read from all inputs so far */
    inputedBytes: number
    /** Total number of bytes written to output so far */
    mergedBytes: number
}

/** URL-based options types */
export type MergeUrlsOptions = Omit<MergeOptions, 'inputs'> & {
    urls: string[]
}
