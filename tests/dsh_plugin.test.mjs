import assert from 'node:assert/strict'
import { mkdtempSync, mkdirSync, rmSync, statSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import test from 'node:test'

import { apply } from '../lib/index.js'
import {
  extractJsonObjects,
  listDailyLogs,
  loadConfig,
  pickMainReport,
  readDailyLog,
  runControlledStep,
  runPreflightBundle,
  saveConfig,
  validateCommandKey,
} from '../lib/runner.js'

function mockExec(overrides = {}) {
  const responses = {
    preflight: { exitCode: 0, report: { command: 'preflight', status: 'ok' } },
    migrate: { exitCode: 0, report: { command: 'migrate', schema_version: 5, status: 'ok' } },
    qualify: {
      exitCode: 0,
      report: {
        command: 'qualify',
        status: 'ok',
        eligible: [
          { media_id: '283938', media_source: 'tmdb', season: 1, season_air_date: '2026-10-04' },
          { media_id: '311842', media_source: 'tmdb', season: 1, season_air_date: '2026-10-01' },
        ],
      },
    },
    'scope-check': {
      exitCode: 0,
      report: {
        command: 'scope-check',
        status: 'ok',
        quarter: '2026-10',
        eligible: [
          { media_id: '283938', media_source: 'tmdb', season: 1 },
          { media_id: '311842', media_source: 'tmdb', season: 1 },
        ],
        runnable: [
          { media_id: '283938', media_source: 'tmdb', season: 1 },
          { media_id: '311842', media_source: 'tmdb', season: 1 },
        ],
        blocked: [],
        review_counts: { matched: 0, ambiguous: 0, identity_unknown: 0, outside_declared_scope: 85 },
      },
    },
    'plan-resources': {
      exitCode: 0,
      report: {
        command: 'plan-resources',
        status: 'plan_only',
        quarter: '2026-10',
        blockers: [],
        config_changes: [],
        resources: {
          cloud_quarter: 'reuse',
          cloud_source: 'reuse',
          cloud_organized: 'reuse',
          raw_dir: 'reuse',
          published_dir: 'reuse',
          litepan_task: 'reuse',
          litepan_rule: 'reuse',
          emby_library: 'reuse',
        },
        resource_reasons: { emby_library: 'emby_library_exact' },
      },
    },
    status: {
      exitCode: 0,
      report: {
        command: 'status',
        status: 'ok',
        episodes: [
          { id: 1, media_source: 'legacy', media_id: '99', season: 1, episode: 1, status: 'needs_review' },
          { id: 10, media_source: 'themoviedb', media_id: '311842', season: 1, episode: 8, status: 'library_ready' },
          { id: 11, media_source: 'themoviedb', media_id: '283938', season: 1, episode: 8, status: 'submitted' },
        ],
      },
    },
    'dry-run': {
      exitCode: 0,
      report: {
        command: 'dry-run',
        status: 'ok',
        decisions: 1,
        errors: 0,
        backfill: { aired_episodes: 17, deferred_episodes: 0, history_queries: 1, query_errors: 0, source_unavailable: 1 },
        subscription_details: [
          { media_id: '283938', media_source: 'themoviedb', season: 1, season_air_date: '2026-10-04', work: '学生会也有洞！' },
          { media_id: '311842', media_source: 'themoviedb', season: 1, season_air_date: '2026-10-01', work: 'FX战士久留美' },
        ],
      },
    },
    run: { exitCode: 0, report: { command: 'run', status: 'ok', advanced: 1, submitted: 0, reconciled: 1 } },
    ...overrides,
  }

  return async (_bin, args) => {
    const cmd = args.includes('run') ? 'run' : args.find((a) => responses[a]) || args[0]
    const hit = responses[cmd]
    const diag = JSON.stringify({ diagnostic: 'read_only', command: cmd, event: 'end' })
    const body = JSON.stringify(hit.report)
    return { exitCode: hit.exitCode, stdout: `${diag}\n${body}${diag}\n`, stderr: '' }
  }
}

test('extractJsonObjects parses concatenated diagnostic + report objects', () => {
  const raw = '{"diagnostic":"read_only","command":"dry-run"}{"command":"dry-run","status":"ok","decisions":0}{"diagnostic":"read_only","command":"dry-run","event":"end"}'
  const objs = extractJsonObjects(raw)
  assert.equal(objs.length, 3)
  const main = pickMainReport(objs, 'dry-run')
  assert.equal(main.status, 'ok')
  assert.equal(main.decisions, 0)
})

test('validateCommandKey enforces allowlist and write authorization', () => {
  assert.deepEqual(validateCommandKey('dry-run-details').args, ['dry-run', '--details'])
  assert.throws(() => validateCommandKey('run'), /未授权/)
  assert.deepEqual(validateCommandKey('run', { allowWritable: true }).args, ['run'])
  assert.throws(() => validateCommandKey('reviews resolve', { allowWritable: true }), /不允许/)
  assert.throws(() => validateCommandKey('prepare-resources', { allowWritable: true }), /不允许/)
})

test('config persists with 0600 permissions', () => {
  const dir = mkdtempSync(join(tmpdir(), 'dsh-acp-'))
  try {
    const saved = saveConfig({ mode: 'local', autoContext: false }, dir)
    assert.equal(saved.mode, 'local')
    assert.equal(saved.autoContext, false)
    const loaded = loadConfig(dir)
    assert.equal(loaded.mode, 'local')
    const mode = statSync(join(dir, 'anime-cloud-pipeline.json')).mode & 0o777
    assert.equal(mode, 0o600)
  } finally {
    rmSync(dir, { recursive: true, force: true })
  }
})

test('runPreflightBundle maps anime titles and flags source_unavailable', async () => {
  const cfg = { mode: 'wrapper', wrapperPath: '/mock/nas-pipeline', v5Root: '/mock', configPath: '/mock/cfg.json', pythonBin: 'python3', dailyLogDir: '/mock/logs', animeGuard: true, autoContext: true, timeoutMs: 30000 }
  const res = await runPreflightBundle(cfg, { execFn: mockExec() })
  assert.equal(res.ok, true)
  assert.equal(res.quarter, '2026-10')
  assert.equal(res.outsideReviewsCount, 85)
  assert.equal(res.works.find((w) => w.mediaId === '311842').work, 'FX战士久留美')
  assert.equal(res.works.find((w) => w.mediaId === '283938').pendingEpisodes[0].statusLabel, '已提交下载')
  assert.ok(res.blockers.some((b) => b.includes('片源暂不可用')))
})

test('runControlledStep refuses write without confirmation or when resources conflict', async () => {
  const cfg = { mode: 'wrapper', wrapperPath: '/mock/nas-pipeline', v5Root: '/mock', configPath: '/mock/cfg.json', pythonBin: 'python3', dailyLogDir: '/mock/logs', animeGuard: true, autoContext: true, timeoutMs: 30000 }
  const denied = await runControlledStep(cfg, { confirmWrite: false, execFn: mockExec() })
  assert.equal(denied.executed, false)

  const blockedExec = mockExec({
    'plan-resources': {
      exitCode: 2,
      report: {
        command: 'plan-resources',
        status: 'blocked',
        quarter: '2026-10',
        blockers: [],
        config_changes: [{ field: 'runtime.emby_library_name', before: '十月新番', after: '10月新番' }],
        resources: { emby_library: 'conflict' },
      },
    },
  })
  const blocked = await runControlledStep(cfg, { confirmWrite: true, execFn: blockedExec })
  assert.equal(blocked.executed, false)
  assert.match(blocked.reason, /前置门禁未全部通过/)

  const ok = await runControlledStep(cfg, { confirmWrite: true, execFn: mockExec() })
  assert.equal(ok.executed, true)
  assert.equal(ok.ok, true)
  assert.equal(ok.advanced, 1)
})

test('daily logs reader protects against traversal and reads sorted markdown', () => {
  const dir = mkdtempSync(join(tmpdir(), 'dsh-acp-logs-'))
  try {
    mkdirSync(dir, { recursive: true })
    writeFileSync(join(dir, '2026-11-25.md'), 'day1')
    writeFileSync(join(dir, '2026-11-26.md'), 'day2')
    const cfg = { dailyLogDir: dir }
    assert.deepEqual(listDailyLogs(cfg), ['2026-11-26', '2026-11-25'])
    assert.equal(readDailyLog(cfg).content, 'day2')
    assert.equal(readDailyLog(cfg, '../secret').ok, false)
  } finally {
    rmSync(dir, { recursive: true, force: true })
  }
})

test('plugin apply registers 4 tools, conditional context, and lazy webServer routes', () => {
  const dir = mkdtempSync(join(tmpdir(), 'dsh-acp-plugin-'))
  try {
    const tools = []
    const contexts = []
    const routes = []
    const ctx = {
      tools: { register: (t) => tools.push(t) },
      systemPrompt: { context: (c) => contexts.push(c) },
      inject: (_deps, cb) => cb({ webServer: { register: (r) => routes.push(r) } }),
    }
    apply(ctx, { dshHome: dir })
    assert.deepEqual(tools.map((t) => t.name), [
      'anime_pipeline_preflight',
      'anime_pipeline_status',
      'anime_pipeline_run',
      'anime_pipeline_logs',
    ])
    assert.equal(routes.length, 1)
    assert.equal(routes[0].path, '/api/anime-pipeline')
    assert.equal(contexts.length, 1)
    const chatCtx = { agent: { session: { snapshotEvents: () => [{ type: 'user/message', payload: { text: '今天天气不错' } }] } } }
    const animeCtx = { agent: { session: { snapshotEvents: () => [{ type: 'user/message', payload: { text: '帮我看看今天追番入库情况' } }] } } }
    assert.equal(contexts[0].text(chatCtx), '')
    assert.match(contexts[0].text(animeCtx), /追番流水线·DSH 插件操作规范/)
  } finally {
    rmSync(dir, { recursive: true, force: true })
  }
})
