/**
 * dsh-anime-cloud-pipeline — DeepSeek Harness (DSH) dedicated plugin.
 *
 * Provides:
 *   1. Four safe, structured DSH tools (`anime_pipeline_preflight`,
 *      `anime_pipeline_status`, `anime_pipeline_run`, `anime_pipeline_logs`).
 *   2. Conditional systemPrompt guidance when the user asks about anime
 *      subscriptions / Emby / 115 / V5 pipeline.
 *   3. Same-origin HTTP API (`/api/anime-pipeline/*`) for the Web Settings UI.
 */
import { readFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import {
  defaultDshHome,
  listDailyLogs,
  loadConfig,
  readDailyLog,
  runControlledStep,
  runPreflightBundle,
  saveConfig,
  summarizeStatus,
} from './runner.js'

export const name = 'anime-cloud-pipeline'
export const inject = ['tools', 'systemPrompt']

const PLUGIN_ROOT = dirname(dirname(fileURLToPath(import.meta.url)))
const VERSION = (() => {
  try {
    return String(JSON.parse(readFileSync(join(PLUGIN_ROOT, 'package.json'), 'utf8')).version || '0.0.0')
  } catch {
    return '0.0.0'
  }
})()

const ANIME_TOPIC_RE = /(追番|动漫|新番|番剧|入库|emby|115|litepan|moviepilot|anime[-_\s]?cloud|pipeline|s\d{2}e\d{2})/i

const AGENT_GUIDANCE = [
  '[追番流水线·DSH 插件操作规范]',
  '1. 优先调用插件工具 `anime_pipeline_preflight`、`anime_pipeline_status`、`anime_pipeline_logs`、`anime_pipeline_run`，不要手拼 SSH 或 Python 命令。',
  '2. 绝对禁止自动解除人工复核（needs_review）、修改调度 cron、改写季度配置或输出 Cookie/Token/API Key/播放地址。',
  '3. 向用户汇报时必须使用**自然中文 + 规整 Markdown 表格**（执行概览表、已完成与推进明细表、阻塞与待处理事项表），并显示具体番剧中文名（如《FX战士久留美》《学生会也有洞！》），不要堆砌内部命令名或英文 JSON 字段名。',
  '4. 安全只读/受控入口按脱敏规则不输出逐集磁盘/云盘路径、STRM 路径与 Emby 精确路径；当状态记录已确认当前季度已播集全部入库且试运行无缺集、无查询错误、无来源不可用时，只在明细表注明“基于流水线状态记录与只读摘要核验”，不要将“安全入口未展示逐集路径”误报为阻塞项。',
].join('\n')

function defineTool(def) {
  return {
    ...def,
    parameters: {
      type: 'object',
      properties: def.parameters || {},
      required: def.required || [],
      additionalProperties: false,
    },
  }
}

function lastUserMessageText(session) {
  try {
    if (!session || typeof session.snapshotEvents !== 'function') return ''
    const events = session.snapshotEvents() || []
    for (let i = events.length - 1; i >= 0; i--) {
      const e = events[i]
      const kind = e && (e.type || e.kind)
      if (kind !== 'user/message') continue
      const payload = e.payload !== undefined ? e.payload : e.data
      const src = payload?.source?.kind ? String(payload.source.kind) : ''
      if (src && src !== 'user') continue
      if (typeof payload?.content === 'string') return payload.content
      if (typeof payload?.text === 'string') return payload.text
      if (Array.isArray(payload?.content)) {
        const joined = payload.content
          .filter((part) => part && part.type === 'text' && typeof part.text === 'string')
          .map((part) => part.text)
          .join('\n')
        if (joined) return joined
      }
    }
  } catch { /* ignore */ }
  return ''
}

function sendJson(res, status, data) {
  const body = JSON.stringify(data)
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'cache-control': 'no-store',
  })
  res.end(body)
}

async function readJsonBody(req) {
  const chunks = []
  let size = 0
  for await (const chunk of req) {
    size += chunk.length
    if (size > 128 * 1024) throw new Error('请求体过大')
    chunks.push(chunk)
  }
  const text = Buffer.concat(chunks).toString('utf8').trim()
  return text ? JSON.parse(text) : {}
}

function registerWebRoutes(webServer, { dshHome, deps }) {
  let busy = false
  let lastPreflight = null

  webServer.register({
    kind: 'prefix',
    path: '/api/anime-pipeline',
    handler: async (req, res) => {
      const url = new URL(req.url ?? '/', 'http://localhost')
      const route = url.pathname.slice('/api/anime-pipeline'.length) || '/'
      try {
        if (route === '/state' && req.method === 'GET') {
          const cfg = loadConfig(dshHome)
          return sendJson(res, 200, {
            ok: true,
            version: VERSION,
            busy,
            config: cfg,
            logDates: listDailyLogs(cfg),
            lastPreflight,
          })
        }

        if (route === '/config' && req.method === 'POST') {
          const body = await readJsonBody(req)
          const saved = saveConfig(body, dshHome)
          return sendJson(res, 200, { ok: true, config: saved })
        }

        if (route === '/logs' && req.method === 'GET') {
          const cfg = loadConfig(dshHome)
          const date = url.searchParams.get('date') || undefined
          const log = readDailyLog(cfg, date)
          return sendJson(res, log.ok ? 200 : 404, log)
        }

        if (route === '/preflight' && req.method === 'POST') {
          if (busy) return sendJson(res, 409, { ok: false, error: '流水线正在执行中，请稍候' })
          busy = true
          try {
            const cfg = loadConfig(dshHome)
            const result = await deps.runPreflightBundle(cfg)
            lastPreflight = result
            return sendJson(res, 200, { ok: true, result })
          } finally {
            busy = false
          }
        }

        if (route === '/status' && req.method === 'POST') {
          if (busy) return sendJson(res, 409, { ok: false, error: '流水线正在执行中，请稍候' })
          busy = true
          try {
            const cfg = loadConfig(dshHome)
            const result = await deps.summarizeStatus(cfg)
            return sendJson(res, 200, { ok: true, result })
          } finally {
            busy = false
          }
        }

        if (route === '/run' && req.method === 'POST') {
          if (busy) return sendJson(res, 409, { ok: false, error: '流水线正在执行中，请稍候' })
          const body = await readJsonBody(req)
          busy = true
          try {
            const cfg = loadConfig(dshHome)
            const result = await deps.runControlledStep(cfg, {
              resumeOnly: body?.resumeOnly === true,
              confirmWrite: body?.confirmWrite === true,
            })
            return sendJson(res, result.executed ? 200 : 409, { ok: result.ok, result })
          } finally {
            busy = false
          }
        }

        return sendJson(res, 404, { ok: false, error: `未知接口: ${route}` })
      } catch (err) {
        return sendJson(res, 500, { ok: false, error: String(err?.message || err) })
      }
    },
  })
}

export function apply(ctx, options = {}) {
  const dshHome = options.dshHome || defaultDshHome()
  const deps = {
    runPreflightBundle: options.runPreflightBundle || runPreflightBundle,
    summarizeStatus: options.summarizeStatus || summarizeStatus,
    runControlledStep: options.runControlledStep || runControlledStep,
  }
  const renderJson = (_args, value) => [
    { type: 'text', text: typeof value === 'string' ? value : JSON.stringify(value, null, 2) },
  ]

  if (ctx.tools && typeof ctx.tools.register === 'function') {
    ctx.tools.register(defineTool({
      name: 'anime_pipeline_preflight',
      description: '运行追番流水线只读全套门禁检查（环境、数据库版本、订阅资格、范围隔离、8 项季度资源与带番名试运行），绝不修改媒体或数据库。',
      parameters: {},
      output: { schema: { type: 'object' }, render: renderJson },
      async execute() {
        return await deps.runPreflightBundle(loadConfig(dshHome))
      },
    }))

    ctx.tools.register(defineTool({
      name: 'anime_pipeline_status',
      description: '只读查询当前季度合格番剧中文名称、各集入库进度以及未处理复核汇总。',
      parameters: {},
      output: { schema: { type: 'object' }, render: renderJson },
      async execute() {
        return await deps.summarizeStatus(loadConfig(dshHome))
      },
    }))

    ctx.tools.register(defineTool({
      name: 'anime_pipeline_run',
      description: '受控推进一轮追番有界快照。执行前自动核验全部只读门禁，且要求 confirmWrite=true；绝不自动解除人工复核或创建季度资源。',
      parameters: {
        resumeOnly: { type: 'boolean', description: '是否仅续推已有任务而不触发新发现（默认 false）' },
        confirmWrite: { type: 'boolean', description: '必须明确传 true 才会真正推进流水线' },
      },
      required: ['confirmWrite'],
      output: { schema: { type: 'object' }, render: renderJson },
      async execute(args) {
        return await deps.runControlledStep(loadConfig(dshHome), {
          resumeOnly: args?.resumeOnly === true,
          confirmWrite: args?.confirmWrite === true,
        })
      },
    }))

    ctx.tools.register(defineTool({
      name: 'anime_pipeline_logs',
      description: '读取追番流水线每日运行自然语言报告（默认返回最新一天，也可指定 YYYY-MM-DD）。',
      parameters: {
        date: { type: 'string', description: '可选日期，格式 YYYY-MM-DD' },
      },
      output: { schema: { type: 'object' }, render: renderJson },
      async execute(args) {
        return readDailyLog(loadConfig(dshHome), args?.date)
      },
    }))
  }

  if (ctx.systemPrompt && typeof ctx.systemPrompt.context === 'function') {
    const registerCtx = () => ctx.systemPrompt.context({
      name: 'anime-cloud-pipeline:guide',
      order: 940,
      text: (context) => {
        const cfg = loadConfig(dshHome)
        if (!cfg.autoContext) return ''
        const txt = lastUserMessageText(context?.agent?.session)
        if (!txt || !ANIME_TOPIC_RE.test(txt)) return ''
        return AGENT_GUIDANCE
      },
    })
    if (typeof ctx.effect === 'function') ctx.effect(registerCtx, 'dsh-anime-cloud-pipeline: context')
    else registerCtx()
  }

  if (ctx.webServer && typeof ctx.webServer.register === 'function') {
    registerWebRoutes(ctx.webServer, { dshHome, deps })
  } else if (typeof ctx.inject === 'function') {
    ctx.inject(['webServer'], (webCtx) => {
      if (webCtx?.webServer && typeof webCtx.webServer.register === 'function') {
        registerWebRoutes(webCtx.webServer, { dshHome, deps })
      }
    })
  }
}
