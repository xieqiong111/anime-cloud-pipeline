/**
 * dsh-anime-cloud-pipeline — DSH Web UI (browser __ModuleLoader__ bundle, zero build step).
 * Registers a "追番流水线" settings section (`settings.section`).
 */
window.__ModuleLoader__.load({
  id: 'dsh-anime-cloud-pipeline',
  factory: function (require) {
    var module = { exports: {} }
    var exports = module.exports
    var React = require('react')
    var h = React.createElement
    var Component = React.Component
    var useCallback = React.useCallback
    var useEffect = React.useEffect
    var useState = React.useState

    var API = '/api/anime-pipeline'

    function readJson(res) {
      return res.text().then(function (text) {
        var data = null
        try { data = text ? JSON.parse(text) : null } catch (e) { data = null }
        if (!res.ok) throw new Error((data && data.error) || ('HTTP ' + res.status))
        return data
      })
    }
    function apiGet(path) {
      return fetch(API + path, { cache: 'no-store' }).then(readJson)
    }
    function apiPost(path, body) {
      return fetch(API + path, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(body || {}),
      }).then(readJson)
    }

    var CSS = [
      '.acp-root{display:flex;flex-direction:column;gap:14px;padding:4px 2px 24px;font-size:13px}',
      '.acp-top{display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap}',
      '.acp-title{font-size:15px;font-weight:600}',
      '.acp-sub{font-size:12px;opacity:.65}',
      '.acp-tabs{display:flex;gap:6px}',
      '.acp-tab{padding:6px 12px;border-radius:8px;border:1px solid transparent;background:transparent;color:inherit;font:inherit;cursor:pointer;opacity:.65}',
      '.acp-tab.active{opacity:1;border-color:rgba(120,170,255,.45);background:rgba(120,170,255,.14);font-weight:600}',
      '.acp-actions{display:flex;gap:8px;flex-wrap:wrap}',
      '.acp-btn{padding:7px 12px;border-radius:8px;border:1px solid rgba(127,127,127,.34);background:transparent;color:inherit;font:inherit;cursor:pointer}',
      '.acp-btn:hover{background:rgba(127,127,127,.12)}',
      '.acp-btn.primary{border-color:rgba(120,170,255,.75);background:rgba(120,170,255,.16)}',
      '.acp-btn:disabled{opacity:.45;cursor:not-allowed}',
      '.acp-card{border:1px solid rgba(127,127,127,.22);border-radius:10px;padding:12px;display:flex;flex-direction:column;gap:8px;background:rgba(127,127,127,.04)}',
      '.acp-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(185px,1fr));gap:8px}',
      '.acp-badge{display:inline-block;padding:2px 8px;border-radius:999px;font-size:11px;border:1px solid rgba(127,127,127,.3)}',
      '.acp-badge.ok{border-color:rgba(80,200,120,.55);background:rgba(80,200,120,.14)}',
      '.acp-badge.warn{border-color:rgba(255,180,70,.55);background:rgba(255,180,70,.14)}',
      '.acp-badge.err{border-color:rgba(255,110,110,.55);background:rgba(255,110,110,.14)}',
      '.acp-note{font-size:12px;padding:8px 10px;border-radius:8px;background:rgba(120,170,255,.12);border:1px solid rgba(120,170,255,.28)}',
      '.acp-note.err{background:rgba(255,110,110,.12);border-color:rgba(255,110,110,.4)}',
      '.acp-field{display:flex;flex-direction:column;gap:4px}',
      '.acp-field label{font-size:12px;opacity:.7}',
      '.acp-input{padding:7px 10px;border-radius:8px;border:1px solid rgba(127,127,127,.34);background:transparent;color:inherit;font:inherit}',
      '.acp-pre{margin:0;white-space:pre-wrap;word-break:break-word;font-size:12px;line-height:1.55;max-height:46vh;overflow:auto}',
    ].join('\n')

    function Section() {
      var tabState = useState('overview')
      var tab = tabState[0], setTab = tabState[1]
      var stateRes = useState(null)
      var state = stateRes[0], setState = stateRes[1]
      var busyState = useState(false)
      var busy = busyState[0], setBusy = busyState[1]
      var msgState = useState(null)
      var msg = msgState[0], setMsg = msgState[1]
      var logState = useState({ date: '', dates: [], content: '' })
      var logData = logState[0], setLogData = logState[1]
      var draftState = useState(null)
      var draft = draftState[0], setDraft = draftState[1]

      var refresh = useCallback(function () {
        return apiGet('/state').then(function (res) {
          setState(res)
          setDraft(res.config)
          if (res.logDates && res.logDates.length && !logData.date) {
            apiGet('/logs?date=' + encodeURIComponent(res.logDates[0])).then(setLogData).catch(function () {})
          }
        }).catch(function (err) {
          setMsg({ err: true, text: err.message })
        })
      }, [logData.date])

      useEffect(function () { refresh() }, [])

      function runCheck() {
        setBusy(true); setMsg(null)
        apiPost('/preflight', {}).then(function (res) {
          setState(function (prev) { return Object.assign({}, prev || {}, { lastPreflight: res.result }) })
          setMsg({ err: !res.result.ok, text: res.result.ok ? '全部只读门禁通过' : ('发现阻塞：' + (res.result.blockers || []).join('；')) })
        }).catch(function (err) {
          setMsg({ err: true, text: err.message })
        }).finally(function () { setBusy(false) })
      }

      function triggerRun(resumeOnly) {
        if (!window.confirm(resumeOnly ? '确认仅续推已有任务一个有界快照？' : '确认运行一轮完整追番流水线（先核门禁，不解除 review）？')) return
        setBusy(true); setMsg(null)
        apiPost('/run', { resumeOnly: !!resumeOnly, confirmWrite: true }).then(function (res) {
          var r = res.result || {}
          setMsg({ err: !r.ok, text: r.executed ? ('执行完成，推进=' + r.advanced + '，新提交=' + r.submitted) : (r.reason || '未执行') })
          return refresh()
        }).catch(function (err) {
          setMsg({ err: true, text: err.message })
        }).finally(function () { setBusy(false) })
      }

      function saveSettings() {
        if (!draft) return
        setBusy(true); setMsg(null)
        apiPost('/config', draft).then(function (res) {
          setDraft(res.config)
          setMsg({ err: false, text: '插件配置已保存' })
        }).catch(function (err) {
          setMsg({ err: true, text: err.message })
        }).finally(function () { setBusy(false) })
      }

      var summary = state && state.lastPreflight
      return h('div', { className: 'acp-root' },
        h('div', { className: 'acp-top' },
          h('div', null,
            h('div', { className: 'acp-title' }, '追番云媒体流水线 V5 ', h('span', { className: 'acp-sub' }, 'v' + ((state && state.version) || '5.4.10'))),
            h('div', { className: 'acp-sub' }, 'MoviePilot → 115 Season 平铺 → LitePan STRM → TMDB 剧照 → Emby')
          ),
          h('div', { className: 'acp-tabs' },
            ['overview', 'logs', 'settings'].map(function (key) {
              var labels = { overview: '运行概览', logs: '每日日志', settings: '配置与关于' }
              return h('button', {
                key: key,
                className: 'acp-tab' + (tab === key ? ' active' : ''),
                onClick: function () { setTab(key) },
              }, labels[key])
            })
          )
        ),
        msg ? h('div', { className: 'acp-note' + (msg.err ? ' err' : '') }, msg.text) : null,

        tab === 'overview' ? h('div', { className: 'acp-card' },
          h('div', { className: 'acp-actions' },
            h('button', { className: 'acp-btn primary', disabled: busy, onClick: runCheck }, busy ? '检查中…' : '运行只读门禁巡检'),
            h('button', { className: 'acp-btn', disabled: busy, onClick: function () { triggerRun(false) } }, '受控推进一轮'),
            h('button', { className: 'acp-btn', disabled: busy, onClick: function () { triggerRun(true) } }, '仅续推已有任务')
          ),
          summary ? h('div', null,
            h('div', { className: 'acp-sub', style: { marginBottom: '8px' } },
              '当前季度：' + (summary.quarter || '-') + ' ｜ 已播 ' + summary.backfill.airedEpisodes + ' 集 ｜ 新候选 ' + summary.newCandidates + ' ｜ 范围外复核 ' + summary.outsideReviewsCount + ' 条（保留未动）'
            ),
            h('div', { className: 'acp-grid', style: { marginBottom: '10px' } },
              (summary.works || []).map(function (w) {
                return h('div', { key: w.mediaId + '-' + w.season, className: 'acp-card' },
                  h('strong', null, '《' + w.work + '》S0' + w.season),
                  h('span', { className: 'acp-badge ' + (w.blocked ? 'err' : 'ok') }, w.blocked ? '复核阻断' : ('已入库 ' + w.readyCount + ' 集')),
                  w.pendingEpisodes && w.pendingEpisodes.length
                    ? h('div', { className: 'acp-sub' }, '待推进：' + w.pendingEpisodes.map(function (p) { return 'E' + p.episode + '(' + p.statusLabel + ')' }).join('、'))
                    : null
                )
              })
            ),
            h('div', { className: 'acp-grid' },
              (summary.resources || []).map(function (r) {
                var cls = r.state === 'reuse' ? 'ok' : (r.state === 'create' ? 'warn' : 'err')
                return h('div', { key: r.key, className: 'acp-sub' },
                  r.label + '：', h('span', { className: 'acp-badge ' + cls }, r.state)
                )
              })
            )
          ) : h('div', { className: 'acp-sub' }, '点击上方「运行只读门禁巡检」即可查看当前季度番剧进度与 8 项资源状态。')
        ) : null,

        tab === 'logs' ? h('div', { className: 'acp-card' },
          h('div', { className: 'acp-actions' },
            ((state && state.logDates) || []).slice(0, 10).map(function (d) {
              return h('button', {
                key: d,
                className: 'acp-btn' + (logData.date === d ? ' primary' : ''),
                onClick: function () { apiGet('/logs?date=' + encodeURIComponent(d)).then(setLogData) },
              }, d)
            })
          ),
          h('pre', { className: 'acp-pre' }, logData.content || '暂无每日运行报告')
        ) : null,

        tab === 'settings' && draft ? h('div', { className: 'acp-card' },
          h('div', { className: 'acp-field' },
            h('label', null, '运行模式（wrapper 走 .dsh/nas-pipeline，local 直接调用 Python V5）'),
            h('select', {
              className: 'acp-input',
              value: draft.mode,
              onChange: function (e) { setDraft(Object.assign({}, draft, { mode: e.target.value })) },
            }, h('option', { value: 'wrapper' }, 'wrapper'), h('option', { value: 'local' }, 'local'))
          ),
          h('div', { className: 'acp-field' },
            h('label', null, 'Wrapper 脚本路径'),
            h('input', { className: 'acp-input', value: draft.wrapperPath, onChange: function (e) { setDraft(Object.assign({}, draft, { wrapperPath: e.target.value })) } })
          ),
          h('div', { className: 'acp-field' },
            h('label', null, 'V5 根目录（local 模式）'),
            h('input', { className: 'acp-input', value: draft.v5Root, onChange: function (e) { setDraft(Object.assign({}, draft, { v5Root: e.target.value })) } })
          ),
          h('div', { className: 'acp-field' },
            h('label', null, 'V5 配置文件路径（local 模式）'),
            h('input', { className: 'acp-input', value: draft.configPath, onChange: function (e) { setDraft(Object.assign({}, draft, { configPath: e.target.value })) } })
          ),
          h('div', { className: 'acp-field' },
            h('label', null, '每日报告目录'),
            h('input', { className: 'acp-input', value: draft.dailyLogDir, onChange: function (e) { setDraft(Object.assign({}, draft, { dailyLogDir: e.target.value })) } })
          ),
          h('label', null,
            h('input', { type: 'checkbox', checked: !!draft.autoContext, onChange: function (e) { setDraft(Object.assign({}, draft, { autoContext: e.target.checked })) } }),
            ' 聊到追番/Emby 时自动注入自然语言表格汇报规范'
          ),
          h('div', { className: 'acp-actions' },
            h('button', { className: 'acp-btn primary', disabled: busy, onClick: saveSettings }, '保存设置')
          )
        ) : null
      )
    }

    class ErrorBoundary extends Component {
      constructor(props) { super(props); this.state = { error: null } }
      static getDerivedStateFromError(error) { return { error: error } }
      render() {
        if (this.state.error) {
          return h('div', { className: 'acp-root' }, h('div', { className: 'acp-note err' }, '面板加载失败：' + (this.state.error.message || this.state.error)))
        }
        return this.props.children
      }
    }

    function apply(ctx) {
      ctx.effect(function () {
        var id = 'dsh-anime-cloud-pipeline-styles'
        if (!document.getElementById(id)) {
          var s = document.createElement('style')
          s.id = id
          s.textContent = CSS
          document.head.appendChild(s)
        }
        return function () {
          var el = document.getElementById(id)
          if (el && el.parentNode) el.parentNode.removeChild(el)
        }
      }, 'dsh-anime-cloud-pipeline: styles')

      ctx.slots.inject('settings.section', function () {
        return ctx.slots.register({
          name: 'settings.section',
          id: 'anime-cloud-pipeline',
          order: 48,
          label: function () { return '追番流水线' },
        }, function (props) {
          return h(ErrorBoundary, null, h(Section, props || {}))
        })
      })
    }

    module.exports = {
      name: 'anime-cloud-pipeline',
      inject: ['slots'],
      apply: apply,
    }
    return module.exports
  },
})
