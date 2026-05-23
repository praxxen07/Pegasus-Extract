'use client'

import { useEffect, useRef, useState, useCallback } from 'react'

/* ─────────────────────────────────────────
   TYPES
───────────────────────────────────────── */
type Format = 'CSV' | 'JSON' | 'EXCEL'

interface Tag {
  id: string
  value: string
}

const LOADER_LABELS = [
  'LOADING TIER-1 DETERMINISTIC ENGINE...',
  'INITIALIZING TIER-2 LIVE DOM EXTRACTOR...',
  'MOUNTING TIER-3 XHR INTERCEPTOR...',
  'ACTIVATING TIER-4 TLS FINGERPRINT MODULE...',
  'CONNECTING AI CHAIN: GEMINI → GROQ → DEEPSEEK...',
  'PEGASUS ONLINE',
]

const EXTRACT_STEPS = [
  { p: 8,   log: 'Resolving DNS & detecting site fingerprint...',    tier: 'TIER 1 · DETERMINISTIC DETECTION' },
  { p: 18,  log: 'Checking cached extraction blueprints...',          tier: 'TIER 1 · DETERMINISTIC DETECTION' },
  { p: 30,  log: 'Blueprint miss — switching to LiveDOMExtractor.',   tier: 'TIER 2 · LIVE DOM EXTRACTOR' },
  { p: 42,  log: 'Mounting XHR Interceptor for dynamic content...',   tier: 'TIER 3 · XHR INTERCEPTOR' },
  { p: 52,  log: 'Compressing DOM tokens for AI parsing pass...',     tier: 'TIER 3 · XHR INTERCEPTOR' },
  { p: 63,  log: 'Dispatching AI chain: Gemini → Groq...',           tier: 'AI CHAIN · GEMINI → GROQ → DEEPSEEK' },
  { p: 74,  log: 'AI extracted schema — running MultiLevelCrawler.',  tier: 'MULTI-LEVEL CRAWLER' },
  { p: 83,  log: 'Traversing nested page structures...',              tier: 'MULTI-LEVEL CRAWLER' },
  { p: 91,  log: 'Normalizing & deduplicating records...',            tier: 'TIER 5 · UNIVERSAL ADAPTER' },
  { p: 100, log: 'Extraction complete. Data ready for export.',       tier: 'COMPLETE ✓' },
]

/* ─────────────────────────────────────────
   NEURAL NETWORK CANVAS
───────────────────────────────────────── */
function NeuralCanvas() {
  const canvasRef = useRef<HTMLCanvasElement>(null)

  useEffect(() => {
    const cvs = canvasRef.current
    if (!cvs) return
    const ctx = cvs.getContext('2d')!
    let W = 0, H = 0
    let animId: number
    const mouse = { x: -999, y: -999 }

    type Node = {
      x: number; y: number; vx: number; vy: number
      r: number; cyan: boolean; op: number
    }
    let nodes: Node[] = []

    function resize() {
      W = cvs.width = window.innerWidth
      H = cvs.height = window.innerHeight
    }

    function init() {
      nodes = []
      const N = Math.min(Math.floor(W * H / 16000), 100)
      for (let i = 0; i < N; i++) {
        nodes.push({
          x: Math.random() * W, y: Math.random() * H,
          vx: (Math.random() - 0.5) * 0.22,
          vy: (Math.random() - 0.5) * 0.22,
          r: Math.random() * 1.4 + 0.4,
          cyan: Math.random() > 0.72,
          op: Math.random() * 0.55 + 0.18,
        })
      }
    }

    function frame() {
      ctx.clearRect(0, 0, W, H)

      // grid overlay
      ctx.save()
      ctx.strokeStyle = 'rgba(139,92,246,0.025)'
      ctx.lineWidth = 0.5
      for (let x = 0; x < W; x += 80) { ctx.beginPath(); ctx.moveTo(x, 0); ctx.lineTo(x, H); ctx.stroke() }
      for (let y = 0; y < H; y += 80) { ctx.beginPath(); ctx.moveTo(0, y); ctx.lineTo(W, y); ctx.stroke() }
      ctx.restore()

      // connections
      for (let i = 0; i < nodes.length; i++) {
        for (let j = i + 1; j < nodes.length; j++) {
          const dx = nodes[i].x - nodes[j].x
          const dy = nodes[i].y - nodes[j].y
          const d2 = dx * dx + dy * dy
          if (d2 < 13225) {
            ctx.beginPath()
            ctx.moveTo(nodes[i].x, nodes[i].y)
            ctx.lineTo(nodes[j].x, nodes[j].y)
            ctx.strokeStyle = `rgba(139,92,246,${(1 - d2 / 13225) * 0.1})`
            ctx.lineWidth = 0.5
            ctx.stroke()
          }
        }
      }

      // nodes
      nodes.forEach(n => {
        ctx.beginPath()
        ctx.arc(n.x, n.y, n.r, 0, Math.PI * 2)
        const c = n.cyan ? '6,255,200' : '139,92,246'
        ctx.fillStyle = `rgba(${c},${n.op})`
        ctx.fill()
      })

      // update
      nodes.forEach(n => {
        n.x += n.vx; n.y += n.vy
        const dx = n.x - mouse.x; const dy = n.y - mouse.y
        const d = Math.sqrt(dx * dx + dy * dy)
        if (d < 120) { n.vx += (dx / d) * 0.018; n.vy += (dy / d) * 0.018 }
        const s = Math.sqrt(n.vx * n.vx + n.vy * n.vy)
        if (s > 0.55) { n.vx = (n.vx / s) * 0.55; n.vy = (n.vy / s) * 0.55 }
        if (n.x < -8) n.x = W + 8; if (n.x > W + 8) n.x = -8
        if (n.y < -8) n.y = H + 8; if (n.y > H + 8) n.y = -8
      })

      animId = requestAnimationFrame(frame)
    }

    const onResize = () => { resize(); init() }
    const onMouse = (e: MouseEvent) => { mouse.x = e.clientX; mouse.y = e.clientY }

    window.addEventListener('resize', onResize)
    window.addEventListener('mousemove', onMouse)
    resize(); init(); frame()

    return () => {
      cancelAnimationFrame(animId)
      window.removeEventListener('resize', onResize)
      window.removeEventListener('mousemove', onMouse)
    }
  }, [])

  return <canvas id="bg-canvas" ref={canvasRef} />
}

/* ─────────────────────────────────────────
   MAIN PAGE
───────────────────────────────────────── */
export default function PegasusExtract() {
  // ── Intro state
  const [introOut, setIntroOut] = useState(false)
  const [introGone, setIntroGone] = useState(false)
  const [appVisible, setAppVisible] = useState(false)
  const [loaderLabel, setLoaderLabel] = useState(LOADER_LABELS[0])

  // ── Form state
  const [url, setUrl] = useState('')
  const [prompt, setPrompt] = useState('')
  const [maxPages, setMaxPages] = useState(100)
  const [format, setFormat] = useState<Format>('CSV')
  const [tags, setTags] = useState<Tag[]>([
    { id: '1', value: 'name' },
    { id: '2', value: 'price' },
    { id: '3', value: 'rating' },
  ])
  const [tagInput, setTagInput] = useState('')
  const [activeStep, setActiveStep] = useState(1)

  // ── Progress state
  const [extracting, setExtracting] = useState(false)
  const [progVisible, setProgVisible] = useState(false)
  const [progPct, setProgPct] = useState(0)
  const [progLog, setProgLog] = useState('Initializing neural parser...')
  const [tierBadge, setTierBadge] = useState('TIER 1 · DETERMINISTIC DETECTION')

  // ── 3D tilt state
  const panelRef = useRef<HTMLDivElement>(null)
  const tiltRaf = useRef<number>(0)

  /* ── INTRO LIFECYCLE ── */
  useEffect(() => {
    let idx = 0
    const cycle = setInterval(() => {
      idx++
      if (idx < LOADER_LABELS.length) setLoaderLabel(LOADER_LABELS[idx])
      else clearInterval(cycle)
    }, 480)

    const t1 = setTimeout(() => setIntroOut(true), 3400)
    const t2 = setTimeout(() => { setIntroGone(true); setAppVisible(true) }, 4300)

    return () => { clearInterval(cycle); clearTimeout(t1); clearTimeout(t2) }
  }, [])

  /* ── TAG SYSTEM ── */
  const addTag = useCallback(() => {
    const val = tagInput.trim().replace(/\s+/g, '_')
    if (!val) return
    if (tags.some(t => t.value === val)) { setTagInput(''); return }
    setTags(prev => [...prev, { id: Date.now().toString(), value: val }])
    setTagInput('')
  }, [tagInput, tags])

  const removeTag = useCallback((id: string) => {
    setTags(prev => prev.filter(t => t.id !== id))
  }, [])

  const handleTagKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') { e.preventDefault(); addTag() }
    if (e.key === 'Backspace' && !tagInput && tags.length > 0) {
      setTags(prev => prev.slice(0, -1))
    }
  }

  /* ── EXTRACTION ── */
  const handleExtract = useCallback(() => {
    if (extracting) return
    setExtracting(true)
    setProgVisible(true)
    setProgPct(0)

    let i = 0
    const iv = setInterval(() => {
      if (i >= EXTRACT_STEPS.length) { clearInterval(iv); return }
      const s = EXTRACT_STEPS[i]
      setProgPct(s.p)
      setProgLog(s.log)
      setTierBadge(s.tier)
      i++
      if (s.p >= 100) {
        clearInterval(iv)
        setTimeout(() => {
          setProgVisible(false)
          setProgPct(0)
          setExtracting(false)
        }, 2200)
      }
    }, 420)
  }, [extracting])

  /* ── RESET ── */
  const handleReset = () => {
    setUrl('')
    setPrompt('')
    setMaxPages(100)
    setFormat('CSV')
    setTags([])
    setTagInput('')
  }

  /* ── 3D TILT ── */
  const handlePanelMouseMove = (e: React.MouseEvent<HTMLDivElement>) => {
    cancelAnimationFrame(tiltRaf.current)
    const panel = panelRef.current
    if (!panel) return
    tiltRaf.current = requestAnimationFrame(() => {
      const r = panel.getBoundingClientRect()
      const dx = (e.clientX - (r.left + r.width / 2)) / r.width
      const dy = (e.clientY - (r.top + r.height / 2)) / r.height
      panel.style.transform = `perspective(1100px) rotateX(${-dy * 2.5}deg) rotateY(${dx * 2.5}deg)`
    })
  }
  const handlePanelMouseLeave = () => {
    cancelAnimationFrame(tiltRaf.current)
    if (panelRef.current)
      panelRef.current.style.transform = 'perspective(1100px) rotateX(0) rotateY(0)'
  }

  /* ─────────────────────────────────────────
     RENDER
  ───────────────────────────────────────── */
  return (
    <>
      <NeuralCanvas />

      {/* ── INTRO ── */}
      {!introGone && (
        <div id="intro" className={introOut ? 'out' : ''}>
          <div className="intro-scanline" />
          <div className="intro-grid" />
          <div className="intro-logo" data-text="PEGASUS">PEGASUS</div>
          <div className="intro-sub">NEURAL EXTRACTION ENGINE</div>
          <div className="intro-tagline">5-TIER AGENTIC WEB INTELLIGENCE · INITIALIZING</div>
          <div className="intro-loader">
            <div className="loader-track">
              <div className="loader-fill" />
            </div>
            <div className="loader-label">{loaderLabel}</div>
          </div>
        </div>
      )}

      {/* ── APP ── */}
      <div id="app" className={appVisible ? 'visible' : ''}>

        {/* SIDEBAR */}
        <nav className="sidebar">
          <div className="sidebar-brand">
            <div className="brand-name">PEGASUS</div>
            <div className="brand-status">
              <div className="s-dot" />
              AI NODE: ACTIVE
            </div>
          </div>
          <div className="sidebar-nav">
            {[
              { icon: 'memory',        label: 'Extractor',  active: true  },
              { icon: 'database',      label: 'History',    active: false },
              { icon: 'terminal',      label: 'Connectors', active: false },
              { icon: 'key',           label: 'Tokens',     active: false },
              { icon: 'receipt_long',  label: 'Logs',       active: false },
            ].map(item => (
              <a key={item.label} className={`nav-item${item.active ? ' active' : ''}`} href="#">
                <span className="material-symbols-outlined">{item.icon}</span>
                <span>{item.label}</span>
              </a>
            ))}
          </div>
          <div className="sidebar-footer">
            <button className="btn-deploy">⊕ DEPLOY NEW NODE</button>
          </div>
        </nav>

        {/* MAIN */}
        <div className="main">
          {/* TOPBAR */}
          <header className="topbar">
            <div className="topbar-title">PEGASUS EXTRACT</div>
            <div className="topbar-right">
              <span className="topbar-status">
                <span className="status-blink">●</span> SYSTEM: OPTIMAL
              </span>
              <button className="topbar-btn">
                <span className="material-symbols-outlined">notifications</span>
              </button>
              <button className="topbar-btn">
                <span className="material-symbols-outlined">account_tree</span>
              </button>
            </div>
          </header>

          {/* CONTENT */}
          <div className="content">
            <div
              className="panel"
              id="main-panel"
              ref={panelRef}
              onMouseMove={handlePanelMouseMove}
              onMouseLeave={handlePanelMouseLeave}
            >
              <div className="amb1" />
              <div className="amb2" />

              <div className="panel-header">
                <div className="panel-title">Extraction Command Center</div>
                <div className="panel-sub">Configure neural parsing parameters for your target · Analysis: 15–30s</div>
              </div>

              <div className="timeline">
                <div className="tl-line" />

                {/* Step 1: URL */}
                <div className={`tl-step${activeStep === 1 ? ' active' : ''}`}>
                  <div className={`tl-node${activeStep === 1 ? ' active' : ''}`} />
                  <div className="step-lbl">
                    <span className="material-symbols-outlined">public</span>
                    Target Vector
                  </div>
                  <div className="step-card">
                    <div className="step-sublbl">Primary URL</div>
                    <input
                      type="url"
                      placeholder="https://"
                      value={url}
                      onChange={e => setUrl(e.target.value)}
                      onFocus={() => setActiveStep(1)}
                    />
                  </div>
                </div>

                {/* Step 2: Prompt */}
                <div className={`tl-step${activeStep === 2 ? ' active' : ''}`}>
                  <div className={`tl-node${activeStep === 2 ? ' active' : ''}`} />
                  <div className="step-lbl">
                    <span className="material-symbols-outlined">psychology</span>
                    Extraction Prompt
                  </div>
                  <div className="step-card">
                    <div className="step-sublbl">Natural Language Request</div>
                    <textarea
                      rows={3}
                      placeholder="Describe the data to extract... e.g. 'Product names, prices, and ratings from each listing'"
                      value={prompt}
                      onChange={e => setPrompt(e.target.value)}
                      onFocus={() => setActiveStep(2)}
                    />
                  </div>
                </div>

                {/* Step 3: Schema */}
                <div className={`tl-step${activeStep === 3 ? ' active' : ''}`}>
                  <div className={`tl-node${activeStep === 3 ? ' active' : ''}`} />
                  <div className="step-lbl">
                    <span className="material-symbols-outlined">data_object</span>
                    Schema Definition
                  </div>
                  <div className="step-card">
                    <div className="step-sublbl">Field Keys · optional, auto-detected if empty</div>
                    <div className="tags-wrap">
                      {tags.map(tag => (
                        <span key={tag.id} className="tag" onClick={() => removeTag(tag.id)}>
                          {tag.value} <span className="tag-rm">×</span>
                        </span>
                      ))}
                    </div>
                    <div className="tag-row">
                      <input
                        type="text"
                        placeholder="Add field key..."
                        value={tagInput}
                        onChange={e => setTagInput(e.target.value)}
                        onKeyDown={handleTagKeyDown}
                        onFocus={() => setActiveStep(3)}
                      />
                      <button className="btn-add" onClick={addTag}>
                        <span className="material-symbols-outlined">add</span>
                      </button>
                    </div>
                  </div>
                </div>

                {/* Step 4: Options */}
                <div className={`tl-step${activeStep === 4 ? ' active' : ''}`}>
                  <div className={`tl-node${activeStep === 4 ? ' active' : ''}`} />
                  <div className="step-lbl">
                    <span className="material-symbols-outlined">settings</span>
                    Output Parameters
                  </div>
                  <div className="step-card">
                    <div className="opts-row">
                      <div className="opt-grp">
                        <div className="step-sublbl">Max Pages</div>
                        <input
                          type="number"
                          value={maxPages}
                          min={1}
                          max={9999}
                          onChange={e => setMaxPages(Number(e.target.value))}
                          onFocus={() => setActiveStep(4)}
                        />
                      </div>
                      <div className="opt-grp">
                        <div className="step-sublbl">Output Format</div>
                        <div className="fmt-toggle">
                          {(['CSV', 'JSON', 'EXCEL'] as Format[]).map(f => (
                            <button
                              key={f}
                              className={`fmt-btn${format === f ? ' active' : ''}`}
                              onClick={() => setFormat(f)}
                            >
                              {f}
                            </button>
                          ))}
                        </div>
                      </div>
                    </div>
                  </div>
                </div>
              </div>{/* /timeline */}

              {/* ACTION BAR */}
              <div className="action-bar">
                <button className="btn-reset" onClick={handleReset}>CLEAR</button>
                <button
                  className={`btn-extract${extracting ? ' loading' : ''}`}
                  onClick={handleExtract}
                >
                  <span className="shimmer" />
                  <span className="material-symbols-outlined">bolt</span>
                  <span>{extracting ? 'EXTRACTING...' : 'INITIATE EXTRACTION'}</span>
                </button>
              </div>

              {/* PROGRESS PANEL */}
              {progVisible && (
                <div className="prog-panel vis">
                  <div className="prog-head">
                    <span className="prog-lbl">⬡ EXTRACTING</span>
                    <span className="prog-pct">{progPct}%</span>
                  </div>
                  <div className="prog-track">
                    <div className="prog-fill" style={{ width: `${progPct}%` }} />
                  </div>
                  <div className="prog-log">{progLog}</div>
                  <div className="tier-badge">{tierBadge}</div>
                </div>
              )}

            </div>{/* /panel */}
          </div>{/* /content */}

          {/* FOOTER */}
          <footer>
            <div className="foot-brand">Computational Clarity Secured.</div>
            <div className="foot-links">
              <a href="#">API Protocol</a>
              <a href="#">Neural Logs</a>
              <a href="#">Support</a>
            </div>
          </footer>
        </div>{/* /main */}

        {/* MOBILE NAV */}
        <nav className="mob-nav">
          {[
            { icon: 'memory',    label: 'Extractor', active: true  },
            { icon: 'database',  label: 'History',   active: false },
            { icon: 'terminal',  label: 'Connect',   active: false },
            { icon: 'settings',  label: 'Settings',  active: false },
          ].map(item => (
            <a key={item.label} className={`mob-item${item.active ? ' active' : ''}`} href="#">
              <span className="material-symbols-outlined">{item.icon}</span>
              <span>{item.label}</span>
            </a>
          ))}
        </nav>

      </div>{/* /app */}
    </>
  )
}