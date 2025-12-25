import { createWriteStream } from 'node:fs'
import fsp from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'
import { describe, it, expect, beforeAll } from 'vitest'
import { mergeStreamsFromUrls, MergeFormat } from '../src/index.js'

const DATABRICKS_TOKEN = process.env.DATABRICKS_TOKEN
const DATABRICKS_HOST = process.env.DATABRICKS_HOST
const DATABRICKS_HTTP_PATH = process.env.DATABRICKS_HTTP_PATH

interface ExternalLink {
  chunk_index: number
  external_link: string
  row_count: number
  byte_count: number
  next_chunk_index?: number
}

interface DatabricksResult {
  statement_id: string
  status: { state: string }
  manifest: {
    format: MergeFormat
    total_chunk_count: number
    total_row_count: number
  }
  result: {
    external_links: ExternalLink[]
  }
}

async function executeQuery(statement: string, format: MergeFormat): Promise<DatabricksResult> {
  const warehouseId = DATABRICKS_HTTP_PATH!.split('/').pop()!

  const response = await fetch(`https://${DATABRICKS_HOST}/api/2.0/sql/statements`, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${DATABRICKS_TOKEN}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      warehouse_id: warehouseId,
      statement,
      format,
      disposition: 'EXTERNAL_LINKS',
      wait_timeout: '50s',
    }),
  })

  return response.json()
}

async function getAllExternalLinks(result: DatabricksResult): Promise<string[]> {
  const links: string[] = [result.result.external_links[0].external_link]

  for (let i = 1; i < result.manifest.total_chunk_count; i++) {
    const response = await fetch(
      `https://${DATABRICKS_HOST}/api/2.0/sql/statements/${result.statement_id}/result/chunks/${i}`,
      {
        headers: {
          'Authorization': `Bearer ${DATABRICKS_TOKEN}`,
        },
      }
    )
    const chunk = await response.json()
    links.push(chunk.external_links[0].external_link)
  }

  return links
}

async function countLines(filePath: string): Promise<number> {
  const content = await fsp.readFile(filePath, 'utf8')
  return content.split('\n').filter(line => line.length > 0).length
}

describe.skipIf(!DATABRICKS_TOKEN || !DATABRICKS_HOST || !DATABRICKS_HTTP_PATH)(
  'Databricks External Links Integration',
  () => {
    let tmpDir: string

    beforeAll(async () => {
      tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), 'databricks-test-'))
    })

    it('merges CSV chunks from Databricks', async () => {
      const result = await executeQuery('SELECT * FROM samples.tpch.lineitem LIMIT 500000', 'CSV')

      expect(result.status.state).toBe('SUCCEEDED')
      expect(result.manifest.total_chunk_count).toBeGreaterThan(0)

      console.log(`Total chunks: ${result.manifest.total_chunk_count}`)
      console.log(`Total rows: ${result.manifest.total_row_count}`)

      const urls = await getAllExternalLinks(result)
      expect(urls.length).toBe(result.manifest.total_chunk_count)

      const outputPath = path.join(tmpDir, 'merged.csv')
      const output = createWriteStream(outputPath)

      await mergeStreamsFromUrls('CSV', { urls, output })

      const lineCount = await countLines(outputPath)
      // +1 for header
      expect(lineCount).toBe(result.manifest.total_row_count + 1)

      await fsp.rm(outputPath, { force: true })
    }, 180000)

    it('merges JSON_ARRAY chunks from Databricks', async () => {
      const result = await executeQuery('SELECT * FROM samples.tpch.lineitem LIMIT 100000', 'JSON_ARRAY')

      expect(result.status.state).toBe('SUCCEEDED')

      console.log(`Total chunks: ${result.manifest.total_chunk_count}`)
      console.log(`Total rows: ${result.manifest.total_row_count}`)

      const urls = await getAllExternalLinks(result)

      const outputPath = path.join(tmpDir, 'merged.json')
      const output = createWriteStream(outputPath)

      await mergeStreamsFromUrls('JSON_ARRAY', { urls, output })

      const content = await fsp.readFile(outputPath, 'utf8')
      const parsed = JSON.parse(content)
      expect(Array.isArray(parsed)).toBe(true)
      expect(parsed.length).toBe(result.manifest.total_row_count)

      await fsp.rm(outputPath, { force: true })
    }, 180000)

    it('merges ARROW_STREAM chunks from Databricks', async () => {
      const result = await executeQuery('SELECT * FROM samples.tpch.lineitem LIMIT 100000', 'ARROW_STREAM')

      expect(result.status.state).toBe('SUCCEEDED')

      console.log(`Total chunks: ${result.manifest.total_chunk_count}`)
      console.log(`Total rows: ${result.manifest.total_row_count}`)

      const urls = await getAllExternalLinks(result)

      const outputPath = path.join(tmpDir, 'merged.arrow')
      const output = createWriteStream(outputPath)

      await mergeStreamsFromUrls('ARROW_STREAM', { urls, output })

      const stats = await fsp.stat(outputPath)
      expect(stats.size).toBeGreaterThan(0)

      await fsp.rm(outputPath, { force: true })
    }, 180000)
  }
)
