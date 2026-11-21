import { test } from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, readdir, stat } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { execFileSync } from 'node:child_process'
import { exportMediaZip, assertExportUrl, EXPORT_LIMITS } from '../src/export-media.ts'
import { OrangeBot } from '../src/bot.ts'
import { registerTools } from '../src/tools.ts'

// Two-frame GIF; the export must not decode/re-encode even a single byte.
const gif = Buffer.from('47494638396101000100800000000000ffffff21f904000a0000002c000000000100010000020244010021f904000a0000002c00000000010001000002024c01003b', 'hex')
const png = Buffer.concat([Buffer.from([137,80,78,71,13,10,26,10]), Buffer.from('different original PNG bytes')])
const url = 'https://gchat.qpic.cn/original'
const workspace = () => mkdtemp(join(tmpdir(), 'orange-export-test-'))
function response(data = gif, step = 3) {
  let at = 0
  const r = new Response(new ReadableStream({ pull(c) {
    if (at === data.length) { c.close(); return }
    c.enqueue(data.subarray(at, at += Math.min(step, data.length - at)))
  } }))
  r.arrayBuffer = async () => { throw new Error('must never buffer original') }
  return r
}
const row = (id: number) => ({ id, kind: 'image' })
const resolve = async (_row: any, consume: any) => {
  try { await consume(url); return { url } } catch { return { error: 'failed' } }
}
function unzip(path: string) {
  return JSON.parse(execFileSync('python3', ['-c', `import zipfile,json,base64,sys
with zipfile.ZipFile(sys.argv[1]) as z:
 assert z.testzip() is None
 print(json.dumps({n:base64.b64encode(z.read(n)).decode() for n in z.namelist()}))`, path], { encoding: 'utf8' }))
}

test('streamed GIF keeps both frames and exact bytes; content and id dedup counts agree with ZIP', async () => {
  let calls = 0
  let active = 0
  let peak = 0
  const result = await exportMediaZip({ workspace: await workspace(), rows: [row(1), row(1), row(2), row(3)],
    resolve: async (r, consume) => {
      active++; peak = Math.max(peak, active)
      try { return await resolve(r, consume) } finally { active-- }
    },
    fetcher: async () => response(++calls === 3 ? png : gif, 1),
  })
  assert.equal(peak, 1)
  assert.equal(calls, 3)
  assert.equal(result.requested, 4)
  assert.equal(result.exported, 2)
  assert.equal(result.duplicates, 2)
  assert.deepEqual(result.failed, [])
  assert.equal((await stat(result.path)).mode & 0o777, 0o600)
  const files = unzip(result.path)
  assert.deepEqual(Object.keys(files), ['0001.gif', '0002.png', 'manifest.json'])
  assert.deepEqual(Buffer.from(files['0001.gif'], 'base64'), gif)
  assert.deepEqual(Buffer.from(files['0002.png'], 'base64'), png)
  const manifest = JSON.parse(Buffer.from(files['manifest.json'], 'base64').toString())
  assert.equal(manifest.entries[2].duplicateOf, 1)
  assert.equal(manifest.path, undefined)
})

test('failed stream is rolled back and fallback succeeds without stale ZIP entries', async () => {
  const result = await exportMediaZip({ workspace: await workspace(), rows: [row(1)],
    resolve: async (_r, consume) => {
      await assert.rejects(consume(url + '/broken'))
      await consume(url)
      return { url }
    },
    fetcher: async (u) => String(u).endsWith('/broken') ? new Response(new ReadableStream({
      start(c) { c.enqueue(gif); }, pull(c) { c.error(new Error('connection reset')) },
    })) : response(),
  })
  const files = unzip(result.path)
  assert.deepEqual(Buffer.from(files['0001.gif'], 'base64'), gif)
  assert.equal(Object.keys(files).length, 2)
})

test('partial failure counts occurrences accurately and exposes no signed URL', async () => {
  const result = await exportMediaZip({ workspace: await workspace(), rows: [row(1), row(2), row(2)],
    resolve: async (r, consume) => r.id === 2 ? { error: 'secret signed URL' } : resolve(r, consume),
    fetcher: async () => response(),
  })
  assert.equal(result.failed.length, 2)
  assert.equal(result.requested, result.exported + result.duplicates + result.failed.length)
  assert.ok(!JSON.stringify(result).includes('secret'))
  unzip(result.path)
})

test('unsolicited partial HTTP responses cannot be mistaken for complete originals', async () => {
  const root = await workspace()
  await assert.rejects(exportMediaZip({ workspace: root, rows: [row(1)], resolve,
    fetcher: async () => new Response(gif, { status: 206, headers: { 'content-range': 'bytes 0-62/500' } }),
  }), /no original/)
  assert.deepEqual(await readdir(root), [])
})

test('all failed exports remove the partial ZIP and directory', async () => {
  const root = await workspace()
  await assert.rejects(exportMediaZip({ workspace: root, rows: [row(1)], resolve, fetcher: async () => new Response('{"retcode":-1}') }), /no original/)
  assert.deepEqual(await readdir(root), [])
})

test('byte limits are enforced during streaming and the reader is cancelled', async () => {
  const old = EXPORT_LIMITS.itemBytes
  EXPORT_LIMITS.itemBytes = 20
  let cancelled = false
  try {
    await assert.rejects(exportMediaZip({ workspace: await workspace(), rows: [row(1)], resolve,
      fetcher: async () => new Response(new ReadableStream({
        pull(c) { c.enqueue(gif) }, cancel() { cancelled = true },
      })),
    }), /no original/)
    assert.ok(cancelled)
  } finally { EXPORT_LIMITS.itemBytes = old }
})

test('total transfer limit aborts and removes the whole partial export', async () => {
  const old = EXPORT_LIMITS.totalBytes
  EXPORT_LIMITS.totalBytes = gif.length + 1
  const root = await workspace()
  try {
    await assert.rejects(exportMediaZip({ workspace: root, rows: [row(1), row(2)], resolve, fetcher: async () => response() }), /total byte/)
    assert.deepEqual(await readdir(root), [])
  } finally { EXPORT_LIMITS.totalBytes = old }
})

test('unsafe sources are rejected before fetch; HTTPS CDN redirects are disabled', async () => {
  for (const value of ['/local/file', 'file:///tmp/x', 'data:image/gif;base64,AA==', 'http://gchat.qpic.cn/x',
    'https://127.0.0.1/x', 'https://qpic.cn.evil.test/x', 'https://u:p@gchat.qpic.cn/x', 'https://gchat.qpic.cn:123/x']) {
    assert.throws(() => assertExportUrl(value))
  }
  assert.doesNotThrow(() => assertExportUrl(url))
  await exportMediaZip({ workspace: await workspace(), rows: [row(1)], resolve,
    fetcher: async (_u, init) => { assert.equal(init?.redirect, 'error'); assert.ok(init?.signal); return response() },
  })
})

function fixture() {
  const b: any = Object.create(OrangeBot.prototype)
  const stored = new Map()
  let id = 0
  b.mediaId = (entry: any) => { stored.set(++id, entry); return id }
  b.store = { setForwards() {} }
  b.forwardCache = new Map()
  return { b, stored }
}
const image = { type: 'media', kind: 'image', url }
const item = (parts: any[]) => ({ senderName: 'fixture', parts })
const msg = (forwards: any[]) => ({ conv: 'qq:own', id: 'message', seq: 1, ts: 1, senderName: 'fixture', raw: JSON.stringify({ forwards }) })

test('full forward enumeration includes >200 entries and >3 nested preview lines', async () => {
  const { b, stored } = fixture()
  const m = msg([{ title: 'first', snapshot: [] }, { title: 'root', locator: 'root' }])
  const tree = new Map<any, any>([['root', Array.from({ length: 205 }, () => item([image])).concat([item([{ type: 'forward', locator: 'child' }])])],
    ['child', Array.from({ length: 5 }, () => item([image]))]])
  b.cgClient = () => ({ forward: async (key: string) => tree.get(key) })
  const ids = await b.forwardImageIds(m, '2')
  assert.equal(ids.length, 210)
  const last: any = [...stored.values()].at(-1)
  assert.equal(JSON.parse(last.plan).path, '2.1')
  assert.equal(JSON.parse(last.plan).locator, 'root')
  assert.deepEqual(await b.mediaUrl(last, async (u: string) => { assert.equal(u, url) }), { url })
})

test('deep locator path resolves beyond display depth, without downloading a cache copy', async () => {
  const { b, stored } = fixture()
  const tree = new Map([['root', [item([{ type: 'forward', locator: 'a' }])]], ['a', [item([{ type: 'forward', locator: 'b' }])]],
    ['b', [item([{ type: 'forward', locator: 'c' }])]], ['c', [item([image])]]])
  b.cgClient = () => ({ forward: async (key: string) => tree.get(key) })
  b.fetchMedia = () => { throw new Error('must not read private cache') }
  await b.forwardImageIds(msg([{ title: 'root', locator: 'root' }]), '1')
  const media: any = [...stored.values()][0]
  let consumed = 0
  assert.deepEqual(await b.mediaUrl(media, async () => { consumed++ }), { url })
  assert.equal(consumed, 1)
})

test('forward enumeration rejects rather than silently truncating oversized/unavailable subtrees', async () => {
  const { b } = fixture()
  await assert.rejects(b.forwardImageIds(msg([{ title: 'large', snapshot: Array.from({ length: 501 }, () => item([image])) }]), '1'), /500 images/)
  await assert.rejects(b.forwardImageIds(msg([{ title: 'missing', snapshot: [item([{ type: 'forward' }])] }]), '1'), /unavailable/)
  await assert.rejects(b.forwardImageIds(msg([]), '1.0'), /invalid/)
})

function toolFixture(own: any = { role: 'task', conv: 'qq:own', admin: false }) {
  const tools = new Map<string, any>()
  let downloads = 0
  const b: any = {
    turns: new Map(),
    store: {
      get: () => own,
      group: () => ({ peer: 'cg:peer' }),
      media: (id: number) => ({ id, kind: 'image', conv: id === 2 ? 'qq:other' : id === 3 ? 'cg:peer' : 'qq:own' }),
      bySeq: (seq: number) => ({ conv: seq === 2 ? 'qq:other' : 'qq:own' }),
    },
    workspaceOf: () => { downloads++; throw new Error('reached export') },
    forwardImageIds: async () => { downloads++; return [1] },
  }
  registerTools({ effect: (fn: any) => fn(), tools: { register: (t: any) => { tools.set(t.name, t); return () => {} } } } as any, () => b)
  return { tool: tools.get('export_attachments'), downloads: () => downloads }
}
const exec = { agent: { session: { id: 'task' } } }

test('tool authorizes ALL ids before any disk/network operation; same rules for forwarded seq', async () => {
  const { tool, downloads } = toolFixture()
  assert.ok(tool, 'export_attachments must be registered')
  await assert.rejects(tool.execute({ ids: [1, 2] }, exec), /scope/)
  assert.equal(downloads(), 0)
  await assert.rejects(tool.execute({ seq: 2 }, exec), /scope/)
  assert.equal(downloads(), 0)
})

test('peer scope is retained; unknown sessions and ambiguous selections fail closed', async () => {
  const { tool } = toolFixture()
  await assert.rejects(tool.execute({ ids: [3] }, exec), /reached export/)
  await assert.rejects(tool.execute({ ids: [1], seq: 1 }, exec), /pass ids OR/)
  await assert.rejects(tool.execute({ ids: [], forward: '1' }, exec), /pass ids OR/)
  await assert.rejects(tool.execute({ ids: [0] }, exec), /valid attachment/)
  const unbound = toolFixture({ conv: '', role: 'task', admin: false })
  await assert.rejects(unbound.tool.execute({ ids: [1] }, exec), /not bound/)
})
