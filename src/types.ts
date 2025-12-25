import type { Readable, Writable } from 'node:stream'

/** Input source types */
export type InputSource = Readable | (() => Readable) | (() => Promise<Readable>)

/** Format types */
export type MergeFormat = 'ARROW_STREAM' | 'CSV' | 'JSON_ARRAY'

/** Options types */
export type MergeOptions = {
    inputs: InputSource[]
    output: Writable
    signal?: AbortSignal
}

/** URL-based options types */
export type MergeUrlsOptions = Omit<MergeOptions, 'inputs'> & {
    urls: string[]
}
