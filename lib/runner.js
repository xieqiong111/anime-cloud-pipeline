/**
 * Pure Node.js ESM runner & config helpers for dsh-anime-cloud-pipeline.
 * Zero external npm dependencies; preserves V5 safety boundaries and allowlists.
 */
import { execFile } from 'node:child_process'
import {
  chmodSync,
  existsSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  renameSync,
  writeFileSync,
} from 'node:fs'
import { homedir } from 'node:os'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { promisify } from 'node:util'

const execFileAsync = promisify(execFile)
const PLUGIN_ROOT = dirname(dirname(fileURLToPath(import.meta.url)))

export const READONLY_COMMANDS = new Map([
  ['preflight', ['preflight']],
  ['migrate-check', ['migrate', '--check-only']],
  ['qualify', ['qualify']],
  ['scope-check', ['scope-check']],
  ['plan-resources', ['plan-resources']],
  ['reviews-list', ['reviews', 'list']],
  ['status', ['status']],
  ['audit', ['audit']],
  ['dry-run', ['dry-run']],
  ['dry-run-details', ['dry-run', '--details']],
  ['cloud-layout', ['cloud-layout']],
  ['cloud-names', ['cloud-names']],
])

export const WRITABLE_COMMANDS = new Map([
  ['run', ['run']],
  ['run-resume', ['run', '--resume-only']],
])

export const RESOURCE_LABELS = {
  cloud_quarter: '云盘季度根目录',
  cloud_source: '云盘未整理目录',
  cloud_organized: '云盘整理目录',
  raw_dir: '本地原始 STRM 目录',
  published_dir: '本地发布 STRM 目录',
  litepan_task: 'LitePan 同步任务',
  litepan_rule: 'LitePan Webhook 规则',
  emby_library: 'Emby 季度媒体库',
}

export const STATE_LABELS = {
  discovered: '已发现',
  download_pending: '待提交下载',
  submitted: '已提交下载',
  cloud_ready: '云盘已就绪',
  organize_pending: '待云盘整理',
  organized: '云盘已平铺整理',
  strm_pending: '待生成 STRM',
  strm_ready: 'STRM 已就绪',
  scrape_pending: '待刮削元数据',
  metadata_ready: '元数据已就绪',
  library_pending: '待 Emby 入库',
  library_ready: '已入库',
  notified: '已入库并通知',
  needs_review: '待人工复核',
}

export function defaultDshHome(env = process.env) {
  return env.DSH_HOME || join(homedir(), '.dsh')
}

export function defaultConfig(env = process.env) {
  const defaultWrapper = '/data/home/追番/.dsh/nas-pipeline'
  const defaultV5Root = '/vol1/1000/tools/moviepilot-v2/config/anime-pipeline-v5'
  const defaultLogDir = '/data/home/追番/.dsh/logs'
  const mode = existsSync(defaultWrapper) ? 'wrapper' : 'local'
  return {
    mode,
    wrapperPath: defaultWrapper,
    v5Root: existsSync(defaultV5Root) ? defaultV5Root : PLUGIN_ROOT,
    configPath: `${defaultV5Root}/config/config.v5.json`,
    pythonBin: '/usr/local/bin/python3.12',
    dailyLogDir: existsSync(defaultLogDir) ? defaultLogDir : join(defaultDshHome(env), 'anime-logs'),
    animeGuard: true,
    autoContext: true,
    timeoutMs: 180000,
  }
}

export function configFilePath(dshHome = defaultDshHome()) {
  return join(dshHome, 'anime-cloud-pipeline.json')
}

export function loadConfig(dshHome = defaultDshHome()) {
  const defaults = defaultConfig({ DSH_HOME: dshHome })
  const file = configFilePath(dshHome)
  if (!existsSync(file)) return defaults
  try {
    const parsed = JSON.parse(readFileSync(file, 'utf8'))
    if (!parsed || typeof parsed !== 'object') return defaults
    return normalizeConfig({ ...defaults, ...parsed }, defaults)
  } catch {
    return defaults
  }
}

export function normalizeConfig(input, base = defaultConfig()) {
  const mode = input?.mode === 'local' ? 'local' : input?.mode === 'wrapper' ? 'wrapper' : base.mode
  const str = (val, fallback) => (typeof val === 'string' && val.trim() ? val.trim() : fallback)
  const timeoutRaw = Number(input?.timeoutMs)
  const timeoutMs = Number.isFinite(timeoutRaw) && timeoutRaw >= 10000 && timeoutRaw <= 600000
    ? Math.round(timeoutRaw)
    : base.timeoutMs
  return {
    mode,
    wrapperPath: str(input?.wrapperPath, base.wrapperPath),
    v5Root: str(input?.v5Root, base.v5Root),
    configPath: str(input?.configPath, base.configPath),
    pythonBin: str(input?.pythonBin, base.pythonBin),
    dailyLogDir: str(input?.dailyLogDir, base.dailyLogDir),
    animeGuard: input?.animeGuard !== false,
    autoContext: input?.autoContext !== false,
    timeoutMs,
  }
}

export function saveConfig(patch, dshHome = defaultDshHome()) {
  const current = loadConfig(dshHome)
  const next = normalizeConfig({ ...current, ...(patch || {}) }, current)
  mkdirSync(dshHome, { recursive: true, mode: 0o700 })
  const file = configFilePath(dshHome)
  const tmp = `${file}.tmp-${process.pid}-${Date.now()}`
  writeFileSync(tmp, `${JSON.stringify(next, null, 2)}\n`, { mode: 0o600 })
  chmodSync(tmp, 0o600)
  renameSync(tmp, file)
  return next
}

export function validateCommandKey(key, { allowWritable = false } = {}) {
  if (READONLY_COMMANDS.has(key)) return { args: [...READONLY_COMMANDS.get(key)], writable: false }
  if (allowWritable && WRITABLE_COMMANDS.has(key)) {
    return { args: [...WRITABLE_COMMANDS.get(key)], writable: true }
  }
  throw new Error(`不允许或未授权的流水线子命令: ${String(key)}`)
}

/**
 * Extract all balanced JSON objects from mixed stdout/stderr text.
 * Handles concatenated diagnostic objects without separating newlines.
 */
export function extractJsonObjects(raw) {
  const text = String(raw || '')
  const objects = []
  let idx = 0
  while (idx < text.length) {
    const start = text.indexOf('{', idx)
    if (start === -1) break
    let depth = 0
    let inString = false
    let escaped = false
    let end = -1
    for (let i = start; i < text.length; i++) {
      const ch = text[i]
      if (inString) {
        if (escaped) escaped = false
        else if (ch === '\\') escaped = true
        else if (ch === '"') inString = false
        continue
      }
      if (ch === '"') inString = true
      else if (ch === '{') depth += 1
      else if (ch === '}') {
        depth -= 1
        if (depth === 0) {
          end = i + 1
          break
        }
      }
    }
    if (end === -1) {
      idx = start + 1
      continue
    }
    try {
      const parsed = JSON.parse(text.slice(start, end))
      if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
        objects.push(parsed)
      }
      idx = end
    } catch {
      idx = start + 1
    }
  }
  return objects
}

export function pickMainReport(objects, expectedCommand) {
  const nonDiag = objects.filter((item) => !('diagnostic' in item))
  if (!nonDiag.length) return null
  if (expectedCommand) {
    const matched = nonDiag.filter((item) => item.command === expectedCommand)
    if (matched.length) return matched[matched.length - 1]
  }
  return nonDiag[nonDiag.length - 1]
}

export async function defaultExec(file, args, options) {
  try {
    const { stdout, stderr } = await execFileAsync(file, args, {
      ...options,
      maxBuffer: 8 * 1024 * 1024,
    })
    return { exitCode: 0, stdout: String(stdout || ''), stderr: String(stderr || '') }
  } catch (err) {
    return {
      exitCode: typeof err?.code === 'number' ? err.code : 1,
      stdout: String(err?.stdout || ''),
      stderr: String(err?.stderr || err?.message || ''),
    }
  }
}

export async function invokePipeline(cfg, commandKey, { allowWritable = false, execFn = defaultExec } = {}) {
  const { args, writable } = validateCommandKey(commandKey, { allowWritable })
  const expectedCommand = args[0]
  let bin
  let cliArgs
  let cwd
  const env = { ...process.env, PYTHONDONTWRITEBYTECODE: '1' }
  if (cfg.animeGuard) env.V5_ANIME_GUARD = '1'

  if (cfg.mode === 'wrapper') {
    bin = cfg.wrapperPath
    cliArgs = args
    cwd = dirname(dirname(cfg.wrapperPath))
  } else {
    bin = cfg.pythonBin
    const entryBin = join(cfg.v5Root, 'bin', 'run_v5.py')
    const entryScript = existsSync(entryBin) ? entryBin : join(cfg.v5Root, 'scripts', 'run_pipeline.py')
    const pos = args[0] === 'reviews' ? 2 : 1
    cliArgs = [entryScript, ...args.slice(0, pos), '--config', cfg.configPath, ...args.slice(pos)]
    cwd = cfg.v5Root
  }

  const res = await execFn(bin, cliArgs, { cwd, env, timeout: cfg.timeoutMs })
  const combined = `${res.stdout || ''}\n${res.stderr || ''}`
  const objects = extractJsonObjects(combined)
  const report = pickMainReport(objects, expectedCommand)
  return {
    commandKey,
    command: expectedCommand,
    writable,
    exitCode: res.exitCode,
    report,
    hasReport: report !== null,
  }
}

function buildWorkIndex(subscriptionDetails) {
  const byKey = new Map()
  for (const sub of subscriptionDetails || []) {
    if (!sub || typeof sub !== 'object') continue
    const source = String(sub.media_source || '').toLowerCase() === 'themoviedb' ? 'tmdb' : String(sub.media_source || '').toLowerCase()
    const key = `${source}:${sub.media_id}:${sub.season}`
    byKey.set(key, {
      work: String(sub.work || ''),
      mediaId: String(sub.media_id || ''),
      season: Number(sub.season || 1),
      airDate: String(sub.season_air_date || ''),
    })
  }
  return byKey
}

function formatAnimeProgress(workIndex, scopeReport, statusReport) {
  const runnable = scopeReport?.runnable || []
  const blocked = scopeReport?.blocked || []
  const blockedSet = new Set(blocked.map((b) => `tmdb:${b.media_id}:${b.season}`))
  const eligible = scopeReport?.eligible || []
  const episodes = statusReport?.episodes || []

  const items = []
  const seen = new Set()
  for (const row of [...eligible, ...runnable, ...blocked]) {
    const key = `tmdb:${row.media_id}:${row.season}`
    if (seen.has(key)) continue
    seen.add(key)
    const meta = workIndex.get(key) || { work: `TMDB#${row.media_id}`, mediaId: String(row.media_id), season: Number(row.season || 1), airDate: '' }
    const matchingEps = episodes.filter((e) => {
      const src = String(e.media_source || '').toLowerCase()
      return (src === 'tmdb' || src === 'themoviedb') && String(e.media_id) === meta.mediaId && Number(e.season) === meta.season
    })
    const readyCount = matchingEps.filter((e) => e.status === 'library_ready' || e.status === 'notified').length
    const pendingList = matchingEps
      .filter((e) => e.status !== 'library_ready' && e.status !== 'notified')
      .map((e) => ({
        episode: Number(e.episode),
        status: e.status,
        statusLabel: STATE_LABELS[e.status] || e.status,
      }))
    items.push({
      work: meta.work || `TMDB#${meta.mediaId}`,
      mediaId: meta.mediaId,
      season: meta.season,
      airDate: meta.airDate,
      blocked: blockedSet.has(key),
      totalTracked: matchingEps.length,
      readyCount,
      pendingEpisodes: pendingList,
      maxReadyEpisode: matchingEps
        .filter((e) => e.status === 'library_ready' || e.status === 'notified')
        .reduce((acc, e) => Math.max(acc, Number(e.episode || 0)), 0),
    })
  }
  return items
}

export async function runPreflightBundle(cfg, { execFn = defaultExec } = {}) {
  const preflight = await invokePipeline(cfg, 'preflight', { execFn })
  const migrate = await invokePipeline(cfg, 'migrate-check', { execFn })
  const qualify = await invokePipeline(cfg, 'qualify', { execFn })
  const scope = await invokePipeline(cfg, 'scope-check', { execFn })
  const plan = await invokePipeline(cfg, 'plan-resources', { execFn })
  const status = await invokePipeline(cfg, 'status', { execFn })
  const dryRun = await invokePipeline(cfg, 'dry-run-details', { execFn })

  const workIndex = buildWorkIndex(dryRun.report?.subscription_details)
  const works = formatAnimeProgress(workIndex, scope.report, status.report)
  const resources = plan.report?.resources || {}
  const resourceItems = Object.entries(RESOURCE_LABELS).map(([key, label]) => ({
    key,
    label,
    state: resources[key] || 'unknown',
    reason: plan.report?.resource_reasons?.[key] || null,
  }))

  const allReuse = resourceItems.length === 8
    && resourceItems.every((r) => r.state === 'reuse')
    && (plan.report?.blockers || []).length === 0
    && (plan.report?.config_changes || []).length === 0

  const gates = {
    preflightOk: preflight.exitCode === 0 && preflight.report?.status === 'ok',
    migrateOk: migrate.exitCode === 0 && migrate.report?.status === 'ok',
    qualifyOk: qualify.exitCode === 0 && qualify.report?.status === 'ok',
    scopeOk: scope.exitCode === 0 && scope.report?.status === 'ok',
    resourcesReady: plan.exitCode === 0 && allReuse,
    dryRunOk: dryRun.exitCode === 0 && dryRun.report?.status === 'ok' && Number(dryRun.report?.errors || 0) === 0,
  }

  const blockers = []
  if (!gates.preflightOk) blockers.push('环境与配置预检未通过')
  if (!gates.migrateOk) blockers.push('数据库结构检查未通过')
  if (!gates.qualifyOk) blockers.push('订阅资格筛选失败')
  if (!gates.scopeOk) blockers.push('当前季度执行范围存在阻塞或未知复核记录')
  if (!gates.resourcesReady) {
    const bad = resourceItems.filter((r) => r.state !== 'reuse').map((r) => `${r.label}(${r.state})`)
    const cfgDiffs = (plan.report?.config_changes || []).map((c) => c.field)
    blockers.push(`季度资源未全部就绪：${bad.join('、') || '状态异常'}${cfgDiffs.length ? `；配置差异：${cfgDiffs.join('、')}` : ''}`)
  }
  if (!gates.dryRunOk) blockers.push('试运行检测到错误')
  const backfill = dryRun.report?.backfill || {}
  if (Number(backfill.source_unavailable || 0) > 0) {
    blockers.push(`有 ${backfill.source_unavailable} 集已到播出日期但片源暂不可用（等待字幕组发布或下次扫描）`)
  }
  if (Number(backfill.query_errors || 0) > 0) {
    blockers.push(`历史补查出现 ${backfill.query_errors} 次片源查询异常`)
  }

  return {
    ok: gates.preflightOk && gates.migrateOk && gates.qualifyOk && gates.scopeOk && gates.resourcesReady && gates.dryRunOk,
    quarter: plan.report?.quarter || scope.report?.quarter || '',
    gates,
    works,
    resources: resourceItems,
    configChanges: plan.report?.config_changes || [],
    backfill: {
      airedEpisodes: Number(backfill.aired_episodes || 0),
      deferredEpisodes: Number(backfill.deferred_episodes || 0),
      historyQueries: Number(backfill.history_queries || 0),
      queryErrors: Number(backfill.query_errors || 0),
      sourceUnavailable: Number(backfill.source_unavailable || 0),
    },
    newCandidates: Number(dryRun.report?.decisions || 0),
    outsideReviewsCount: Number(scope.report?.review_counts?.outside_declared_scope || 0),
    matchedReviewsCount: Number(scope.report?.review_counts?.matched || 0),
    blockers,
  }
}

export async function summarizeStatus(cfg, { execFn = defaultExec } = {}) {
  const scope = await invokePipeline(cfg, 'scope-check', { execFn })
  const status = await invokePipeline(cfg, 'status', { execFn })
  const dryRun = await invokePipeline(cfg, 'dry-run-details', { execFn })
  const workIndex = buildWorkIndex(dryRun.report?.subscription_details)
  const works = formatAnimeProgress(workIndex, scope.report, status.report)
  return {
    ok: scope.exitCode === 0 && status.exitCode === 0,
    quarter: scope.report?.quarter || '',
    works,
    outsideReviewsCount: Number(scope.report?.review_counts?.outside_declared_scope || 0),
    matchedReviewsCount: Number(scope.report?.review_counts?.matched || 0),
    airedEpisodes: Number(dryRun.report?.backfill?.aired_episodes || 0),
    sourceUnavailable: Number(dryRun.report?.backfill?.source_unavailable || 0),
    newCandidates: Number(dryRun.report?.decisions || 0),
  }
}

export async function runControlledStep(cfg, { resumeOnly = false, confirmWrite = false, execFn = defaultExec } = {}) {
  if (confirmWrite !== true) {
    return {
      executed: false,
      ok: false,
      reason: '需要显式确认写入（confirmWrite=true）才能推进流水线。',
    }
  }
  const precheck = await runPreflightBundle(cfg, { execFn })
  if (!precheck.ok) {
    return {
      executed: false,
      ok: false,
      reason: `前置门禁未全部通过，已按安全规则停止：${precheck.blockers.join('；')}`,
      precheck,
    }
  }
  if (precheck.works.every((w) => w.blocked)) {
    return {
      executed: false,
      ok: false,
      reason: '当前季度所有合格订阅均处于复核阻断状态，请先人工核实后再决定。',
      precheck,
    }
  }
  const key = resumeOnly ? 'run-resume' : 'run'
  const runResult = await invokePipeline(cfg, key, { allowWritable: true, execFn })
  const postStatus = await summarizeStatus(cfg, { execFn })
  return {
    executed: true,
    ok: runResult.exitCode === 0 && runResult.report?.status !== 'blocked',
    commandKey: key,
    exitCode: runResult.exitCode,
    stopReason: runResult.report?.stop_reason || null,
    advanced: Number(runResult.report?.advanced || 0),
    submitted: Number(runResult.report?.submitted || 0),
    reconciled: Number(runResult.report?.reconciled || 0),
    postStatus,
  }
}

export function listDailyLogs(cfg) {
  const dir = cfg.dailyLogDir
  if (!dir || !existsSync(dir)) return []
  try {
    return readdirSync(dir)
      .filter((name) => /^\d{4}-\d{2}-\d{2}\.md$/.test(name))
      .sort()
      .reverse()
      .map((name) => name.slice(0, 10))
  } catch {
    return []
  }
}

export function readDailyLog(cfg, date) {
  const dir = cfg.dailyLogDir
  const dates = listDailyLogs(cfg)
  if (!dates.length) return { ok: false, date: null, dates: [], content: '' }
  const target = date ? String(date).trim() : dates[0]
  if (!/^\d{4}-\d{2}-\d{2}$/.test(target)) {
    return { ok: false, error: '日期格式必须为 YYYY-MM-DD', dates, content: '' }
  }
  const full = resolve(dir, `${target}.md`)
  if (!full.startsWith(resolve(dir) + '/') || !existsSync(full)) {
    return { ok: false, error: `未找到 ${target} 的运行日志`, dates, content: '' }
  }
  return { ok: true, date: target, dates, content: readFileSync(full, 'utf8') }
}
