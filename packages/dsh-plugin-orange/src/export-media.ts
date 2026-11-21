/** Original attachment export. STORE ZIP entries are streamed, never image-decoded.
 * One active download bounds memory; failed/duplicate entries are rolled back on disk.
 */
import { createHash } from 'node:crypto'
import { mkdtemp, open, realpath, rename, rmdir, unlink } from 'node:fs/promises'
import type { FileHandle } from 'node:fs/promises'
import { join } from 'node:path'
import { crc32 } from 'node:zlib'

export interface ExportMedia { id: number, kind: string }
export interface ExportEntry { id: number, name: string, bytes: number, sha256: string, duplicateOf?: number }
export interface ExportResult {
  path: string, requested: number, exported: number, duplicates: number,
  failed: { id: number, reason: string }[], entries: ExportEntry[]
}
export const EXPORT_LIMITS = { items: 500, itemBytes: 30 * 1024 * 1024, totalBytes: 512 * 1024 * 1024 }

/** Only platform CDNs, no local files, data URLs, arbitrary hosts or redirects. */
export function assertExportUrl(value: string): void {
  const url = new URL(value)
  if (url.protocol !== 'https:' || url.username || url.password || (url.port && url.port !== '443')
    || !(url.hostname === 'multimedia.nt.qq.com' || url.hostname.endsWith('.qpic.cn')))
    throw new Error('unsupported media source (requires an HTTPS QQ image CDN)')
}

function extension(data: Uint8Array): string | undefined {
  const b = Buffer.from(data)
  if (b.subarray(0, 6).toString() === 'GIF87a' || b.subarray(0, 6).toString() === 'GIF89a') return 'gif'
  if (b.subarray(0, 8).equals(Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]))) return 'png'
  if (b[0] === 255 && b[1] === 216 && b[2] === 255) return 'jpg'
  if (b.subarray(0, 4).toString() === 'RIFF' && b.subarray(8, 12).toString() === 'WEBP') return 'webp'
  return undefined
}

/** Tiny ZIP32 STORE writer: sizes are capped far below ZIP64. */
class StreamZip {
  offset = 0
  central: Buffer[] = []
  constructor(private file: FileHandle) {}
  async write(data: Uint8Array): Promise<void> {
    let at = 0
    while (at < data.length) {
      const { bytesWritten } = await this.file.write(data, at, data.length - at, this.offset)
      if (!bytesWritten) throw new Error('ZIP write made no progress')
      at += bytesWritten
      this.offset += bytesWritten
    }
  }
  async rollback(offset: number): Promise<void> {
    await this.file.truncate(offset)
    this.offset = offset
  }
  async header(name: string): Promise<Buffer> {
    const bytes = Buffer.from(name)
    const h = Buffer.alloc(30)
    h.writeUInt32LE(0x04034b50, 0)
    h.writeUInt16LE(20, 4)
    h.writeUInt16LE(0x808, 6) // UTF-8 and trailing data descriptor
    h.writeUInt16LE(33, 12) // 1980-01-01
    h.writeUInt16LE(bytes.length, 26)
    await this.write(h)
    await this.write(bytes)
    return bytes
  }
  async endEntry(name: Buffer, start: number, crc: number, size: number): Promise<void> {
    const d = Buffer.alloc(16)
    d.writeUInt32LE(0x08074b50, 0)
    d.writeUInt32LE(crc, 4)
    d.writeUInt32LE(size, 8)
    d.writeUInt32LE(size, 12)
    await this.write(d)
    const c = Buffer.alloc(46)
    c.writeUInt32LE(0x02014b50, 0)
    c.writeUInt16LE(20, 4)
    c.writeUInt16LE(20, 6)
    c.writeUInt16LE(0x808, 8)
    c.writeUInt16LE(33, 14)
    c.writeUInt32LE(crc, 16)
    c.writeUInt32LE(size, 20)
    c.writeUInt32LE(size, 24)
    c.writeUInt16LE(name.length, 28)
    c.writeUInt32LE(start, 42)
    this.central.push(Buffer.concat([c, name]))
  }
  async finish(): Promise<void> {
    const start = this.offset
    for (const entry of this.central) await this.write(entry)
    const end = Buffer.alloc(22)
    end.writeUInt32LE(0x06054b50, 0)
    end.writeUInt16LE(this.central.length, 8)
    end.writeUInt16LE(this.central.length, 10)
    end.writeUInt32LE(this.offset - start, 12)
    end.writeUInt32LE(start, 16)
    await this.write(end)
  }
}

export async function exportMediaZip(options: {
  workspace: string,
  rows: ExportMedia[],
  /** Must resolve ONLY already-authorized rows. Verification consumes the stream directly. */
  resolve: (row: ExportMedia, consume: (url: string) => Promise<void>) => Promise<{ url: string } | { error: string }>,
  fetcher?: typeof fetch,
}): Promise<ExportResult> {
  const { rows } = options
  if (!rows.length || rows.length > EXPORT_LIMITS.items) throw new Error('select 1 to 500 attachments')
  const root = await realpath(options.workspace)
  const dir = await mkdtemp(join(root, 'attachment-export-'))
  const partial = join(dir, 'originals.zip.partial')
  const path = join(dir, 'originals.zip')
  let file: FileHandle | undefined
  try {
    file = await open(partial, 'wx', 0o600)
    const zip = new StreamZip(file)
    const result: ExportResult = { path, requested: rows.length, exported: 0, duplicates: 0, failed: [], entries: [] }
    const hashes = new Map<string, ExportEntry>()
    const ids = new Map<number, ExportEntry>()
    let transferred = 0
    // Intentionally sequential: concurrency=1, no aggregate image buffers or unbounded promises.
    for (const row of rows) {
      const previous = ids.get(row.id)
      if (previous) {
        result.entries.push({ ...previous, duplicateOf: previous.duplicateOf ?? previous.id })
        result.duplicates++
        continue
      }
      let completed: ExportEntry | undefined
      const found = await options.resolve(row, async (url) => {
        assertExportUrl(url)
        const start = zip.offset
        const signal = AbortSignal.timeout(60_000)
        const response = await (options.fetcher ?? fetch)(url, { signal, redirect: 'error' })
        if (!response.ok || !response.body) {
          await response.body?.cancel()
          throw new Error('media download failed')
        }
        let size = 0
        let crc = 0
        let prefix = Buffer.alloc(0)
        let name: Buffer | undefined
        const hash = createHash('sha256')
        const writePrefix = async () => {
          const ext = extension(prefix)
          if (row.kind === 'image' && !ext) throw new Error('source is not an original supported image')
          name = await zip.header(`${String(result.exported + 1).padStart(4, '0')}.${ext ?? 'bin'}`)
          await zip.write(prefix)
        }
        try {
          for await (const chunk of response.body) {
            size += chunk.length
            transferred += chunk.length
            if (size > EXPORT_LIMITS.itemBytes || transferred > EXPORT_LIMITS.totalBytes) throw new Error('export byte limit exceeded')
            hash.update(chunk)
            crc = crc32(chunk, crc)
            if (!name) {
              const take = Math.min(12 - prefix.length, chunk.length)
              prefix = Buffer.concat([prefix, chunk.subarray(0, take)])
              if (prefix.length === 12) {
                await writePrefix()
                await zip.write(chunk.subarray(take))
              }
            } else await zip.write(chunk)
          }
          if (!name) await writePrefix()
          const sha256 = hash.digest('hex')
          const duplicate = hashes.get(sha256)
          if (duplicate) {
            await zip.rollback(start)
            completed = { ...duplicate, id: row.id, duplicateOf: duplicate.id }
          } else {
            await zip.endEntry(name!, start, crc, size)
            completed = { id: row.id, name: name!.toString(), bytes: size, sha256 }
          }
        } catch (error) {
          await zip.rollback(start)
          throw error
        }
      })
      if (!('url' in found) || !completed) {
        // Resolver errors can contain signed URLs. Do not return them in a public manifest.
        result.failed.push({ id: row.id, reason: 'original unavailable or unsupported; download failed or limit exceeded' })
      } else {
        result.entries.push(completed)
        ids.set(row.id, completed)
        if (completed.duplicateOf !== undefined) result.duplicates++
        else { hashes.set(completed.sha256, completed); result.exported++ }
      }
      if (transferred > EXPORT_LIMITS.totalBytes) throw new Error('export total byte limit exceeded')
    }
    if (!result.exported) throw new Error('no original attachments could be exported')
    const manifest = Buffer.from(JSON.stringify({ ...result, path: undefined }, null, 2))
    const start = zip.offset
    const name = await zip.header('manifest.json')
    await zip.write(manifest)
    await zip.endEntry(name, start, crc32(manifest), manifest.length)
    await zip.finish()
    await file.close()
    file = undefined
    await rename(partial, path)
    return result
  } catch (error) {
    await file?.close().catch(() => {})
    await unlink(partial).catch(() => {})
    await rmdir(dir).catch(() => {})
    throw error
  }
}
