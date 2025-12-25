import http from 'node:http'

export async function collectToString(stream: NodeJS.ReadableStream): Promise<string> {
  const chunks: Buffer[] = []
  for await (const chunk of stream as AsyncIterable<Buffer | string>) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(String(chunk)))
  }
  return Buffer.concat(chunks).toString('utf8')
}

/**
 * Create a local HTTP server for testing purposes.
 */
export async function createLocalHttpServer(
  routes: Map<string, string | Buffer>,
  options: {
    host?: string
    contentType?: string
  } = {},
): Promise<{
  baseUrl: string
  server: http.Server
  close: () => Promise<void>
}> {
  const host = options.host ?? '127.0.0.1'
  const contentType = options.contentType ?? 'text/plain; charset=utf-8'

  const server = http.createServer((req, res) => {
    const body = routes.get(req.url ?? '')
    if (!body) {
      res.statusCode = 404
      res.end('not found')
      return
    }

    res.statusCode = 200
    res.setHeader('content-type', contentType)
    res.end(body)
  })

  await new Promise<void>((resolve, reject) => {
    server.once('error', reject)
    server.listen(0, host, () => resolve())
  })

  const addr = server.address()
  if (!addr || typeof addr === 'string') throw new Error('Unexpected server address')

  return {
    baseUrl: `http://${host}:${addr.port}`,
    server,
    close: () => new Promise<void>((resolve) => server.close(() => resolve())),
  }
}
