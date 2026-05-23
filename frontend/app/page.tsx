'use client'

import { useEffect, useRef, useState, useCallback } from 'react'

/* ─────────────────────────────────────────
   TYPES
───────────────────────────────────────── */
type Format = 'CSV' | 'JSON' | 'EXCEL'
type AppStage = 'idle' | 'analyzing' | 'analyzed' | 'extracting' | 'done' | 'error'

interface Tag { id: string; value: string }

const API = 'https://pegasus-extract.onrender.com'

const LOADER_LABELS = [
  'LOADING TIER-1 DETERMINISTIC ENGINE...',
  'INITIALIZING TIER-2 LIVE DOM EXTRACTOR...',
  'MOUNTING TIER-3 XHR INTERCEPTOR...',
  'ACTIVATING TIER-4 TLS FINGERPRINT MODULE...',
  'CONNECTING AI CHAIN: GEMINI → GROQ → DEEPSEEK...',
  'PEGASUS ONLINE',
]

/* ─────────────────────────────────────────
   NEURAL NETWORK CANVAS
───────────────────────────────────────── */
function NeuralCanvas() {
  const canvasRef = useRef<HTMLCanvasElement>(null)
  useEffect(() => {
    const cvs = canvasRef.current; if (!cvs) return
    const ctx = cvs.getContext('2d')!
    let W = 0, H = 0, animId: number
    const mouse = { x: -999, y: -999 }
    type Node = { x:number;y:number;vx:number;vy:number;r:number;cyan:boolean;op:number }
    let nodes: Node[] = []

    const resize = () => { W = cvs.width = window.innerWidth; H = cvs.height = window.innerHeight }
    const init = () => {
      nodes = []
      const N = Math.min(Math.floor(W * H / 16000), 100)
      for (let i = 0; i < N; i++) nodes.push({
        x: Math.random()*W, y: Math.random()*H,
        vx: (Math.random()-.5)*.22, vy: (Math.random()-.5)*.22,
        r: Math.random()*1.4+.4, cyan: Math.random()>.72, op: Math.random()*.55+.18
      })
    }
    const frame = () => {
      ctx.clearRect(0,0,W,H)
      ctx.save(); ctx.strokeStyle='rgba(139,92,246,0.025)'; ctx.lineWidth=.5
      for(let x=0;x<W;x+=80){ctx.beginPath();ctx.moveTo(x,0);ctx.lineTo(x,H);ctx.stroke()}
      for(let y=0;y<H;y+=80){ctx.beginPath();ctx.moveTo(0,y);ctx.lineTo(W,y);ctx.stroke()}
      ctx.restore()
      for(let i=0;i<nodes.length;i++) for(let j=i+1;j<nodes.length;j++){
        const dx=nodes[i].x-nodes[j].x,dy=nodes[i].y-nodes[j].y,d2=dx*dx+dy*dy
        if(d2<13225){ctx.beginPath();ctx.moveTo(nodes[i].x,nodes[i].y);ctx.lineTo(nodes[j].x,nodes[j].y);ctx.strokeStyle=`rgba(139,92,246,${(1-d2/13225)*.1})`;ctx.lineWidth=.5;ctx.stroke()}
      }
      nodes.forEach(n=>{
        ctx.beginPath();ctx.arc(n.x,n.y,n.r,0,Math.PI*2)
        ctx.fillStyle=`rgba(${n.cyan?'6,255,200':'139,92,246'},${n.op})`;ctx.fill()
        n.x+=n.vx;n.y+=n.vy
        const dx=n.x-mouse.x,dy=n.y-mouse.y,d=Math.sqrt(dx*dx+dy*dy)
        if(d<120){n.vx+=(dx/d)*.018;n.vy+=(dy/d)*.018}
        const s=Math.sqrt(n.vx*n.vx+n.vy*n.vy)
        if(s>.55){n.vx=(n.vx/s)*.55;n.vy=(n.vy/s)*.55}
        if(n.x<-8)n.x=W+8;if(n.x>W+8)n.x=-8;if(n.y<-8)n.y=H+8;if(n.y>H+8)n.y=-8
      })
      animId = requestAnimationFrame(frame)
    }
    const onResize = () => { resize(); init() }
    const onMouse = (e:MouseEvent) => { mouse.x=e.clientX; mouse.y=e.clientY }
    window.addEventListener('resize',onResize); window.addEventListener('mousemove',onMouse)
    resize(); init(); frame()
    return () => { cancelAnimationFrame(animId); window.removeEventListener('resize',onResize); window.removeEventListener('mousemove',onMouse) }
  }, [])
  return <canvas id="bg-canvas" ref={canvasRef} />
}

/* ─────────────────────────────────────────
   MAIN PAGE
───────────────────────────────────────── */
export default function PegasusExtract() {
  // Intro
  const [introOut, setIntroOut]   = useState(false)
  const [introGone, setIntroGone] = useState(false)
  const [appVisible, setAppVisible] = useState(false)
  const [loaderLabel, setLoaderLabel] = useState(LOADER_LABELS[0])

  // Form
  const [url, setUrl]         = useState('')
  const [prompt, setPrompt]   = useState('')
  const [maxPages, setMaxPages] = useState(100)
  const [format, setFormat]   = useState<Format>('CSV')
  const [tags, setTags]       = useState<Tag[]>([
    {id:'1',value:'name'},{id:'2',value:'price'},{id:'3',value:'rating'}
  ])
  const [tagInput, setTagInput] = useState('')
  const [activeStep, setActiveStep] = useState(1)

  // API state
  const [stage, setStage]         = useState<AppStage>('idle')
  const [jobId, setJobId]         = useState<string|null>(null)
  const [extractionId, setExtractionId] = useState<string|null>(null)
  const [progPct, setProgPct]     = useState(0)
  const [progLog, setProgLog]     = useState('')
  const [tierBadge, setTierBadge] = useState('')
  const [errorMsg, setErrorMsg]   = useState('')
  const [progVisible, setProgVisible] = useState(false)

  // Refs
  const panelRef  = useRef<HTMLDivElement>(null)
  const tiltRaf   = useRef<number>(0)
  const pollRef   = useRef<ReturnType<typeof setInterval>|null>(null)

  /* ── INTRO ── */
  useEffect(() => {
    let idx = 0
    const cycle = setInterval(() => { idx++; if(idx<LOADER_LABELS.length) setLoaderLabel(LOADER_LABELS[idx]); else clearInterval(cycle) }, 480)
    const t1 = setTimeout(() => setIntroOut(true), 3400)
    const t2 = setTimeout(() => { setIntroGone(true); setAppVisible(true) }, 4300)
    return () => { clearInterval(cycle); clearTimeout(t1); clearTimeout(t2) }
  }, [])

  /* ── CLEANUP POLL ON UNMOUNT ── */
  useEffect(() => () => { if(pollRef.current) clearInterval(pollRef.current) }, [])

  /* ── TAGS ── */
  const addTag = useCallback(() => {
    const val = tagInput.trim().replace(/\s+/g,'_'); if(!val) return
    if(tags.some(t=>t.value===val)){setTagInput('');return}
    setTags(p=>[...p,{id:Date.now().toString(),value:val}]); setTagInput('')
  }, [tagInput, tags])

  const removeTag = (id:string) => setTags(p=>p.filter(t=>t.id!==id))

  const handleTagKeyDown = (e:React.KeyboardEvent<HTMLInputElement>) => {
    if(e.key==='Enter'){e.preventDefault();addTag()}
    if(e.key==='Backspace'&&!tagInput&&tags.length) setTags(p=>p.slice(0,-1))
  }

  /* ── STEP 1: ANALYZE ── */
  const handleExtract = useCallback(async () => {
    if(stage!=='idle') return
    if(!url.trim()){setErrorMsg('Please enter a target URL first.');return}

    setErrorMsg('')
    setStage('analyzing')
    setProgVisible(true)
    setProgPct(5)
    setProgLog('Sending analysis request to Pegasus backend...')
    setTierBadge('TIER 1 · DETERMINISTIC DETECTION')

    try {
      const res = await fetch(`${API}/analyze`, {
        method:'POST',
        headers:{'Content-Type':'application/json'},
        body: JSON.stringify({
          url: url.trim(),
          description: prompt.trim() || 'Extract all relevant data from this page',
          schema_fields: tags.map(t=>t.value),
          max_pages: maxPages,
        })
      })
      if(!res.ok) throw new Error(`Analyze failed: ${res.status} ${res.statusText}`)
      const data = await res.json()

      // backend returns job_id as string directly or inside object
      const id = typeof data === 'string' ? data : (data.job_id || data.id || data)
      setJobId(id)
      setProgPct(20)
      setProgLog(`Analysis job created (${id}) — polling status...`)
      setTierBadge('TIER 2 · LIVE DOM EXTRACTOR')
      setStage('analyzed')

      // Auto-confirm and start extraction
      await startExtraction(id)

    } catch(err:unknown) {
      const msg = err instanceof Error ? err.message : String(err)
      setStage('error')
      setErrorMsg(`Analysis error: ${msg}`)
      setProgVisible(false)
    }
  }, [stage, url, prompt, tags, maxPages])

  /* ── STEP 2: EXTRACT ── */
  const startExtraction = async (id: string) => {
    setProgPct(35)
    setProgLog('Confirming extraction job...')
    setTierBadge('TIER 3 · XHR INTERCEPTOR')

    try {
      const res = await fetch(`${API}/extract`, {
        method:'POST',
        headers:{'Content-Type':'application/json'},
        body: JSON.stringify({ job_id: id, confirm: true })
      })
      if(!res.ok) throw new Error(`Extract start failed: ${res.status} ${res.statusText}`)
      const data = await res.json()

      const eid = typeof data === 'string' ? data : (data.extraction_job_id || data.job_id || data.id || data)
      setExtractionId(eid)
      setStage('extracting')
      setProgPct(45)
      setProgLog(`Extraction started (${eid}) — monitoring progress...`)
      setTierBadge('AI CHAIN · GEMINI → GROQ → DEEPSEEK')

      pollExtraction(eid)

    } catch(err:unknown) {
      const msg = err instanceof Error ? err.message : String(err)
      setStage('error')
      setErrorMsg(`Extraction start error: ${msg}`)
      setProgVisible(false)
    }
  }

  /* ── STEP 3: POLL ── */
  const pollExtraction = (eid: string) => {
    let ticks = 0
    const POLL_INTERVAL = 3000

    const POLL_STEPS = [
      {at:2,  pct:55, log:'Running multi-level crawler on target...',        tier:'MULTI-LEVEL CRAWLER'},
      {at:5,  pct:65, log:'Traversing nested page structures...',             tier:'MULTI-LEVEL CRAWLER'},
      {at:8,  pct:75, log:'AI normalizing extracted records...',              tier:'AI CHAIN · GEMINI → GROQ → DEEPSEEK'},
      {at:11, pct:82, log:'Deduplicating and validating schema...',           tier:'TIER 5 · UNIVERSAL ADAPTER'},
      {at:14, pct:90, log:'Formatting output data...',                        tier:'TIER 5 · UNIVERSAL ADAPTER'},
    ]

    pollRef.current = setInterval(async () => {
      ticks++

      // Update visual progress based on elapsed ticks
      const step = POLL_STEPS.find(s=>s.at===ticks)
      if(step){ setProgPct(step.pct); setProgLog(step.log); setTierBadge(step.tier) }

      try {
        const res = await fetch(`${API}/extract/${eid}`)
        if(!res.ok) throw new Error(`Poll failed: ${res.status}`)
        const data = await res.json()

        // Handle various status field names backends use
        const status = (data.status || data.state || '').toLowerCase()

        if(status === 'completed' || status === 'done' || status === 'success' || status === 'finished') {
          clearInterval(pollRef.current!)
          setProgPct(100)
          setProgLog('Extraction complete! Your data is ready to download.')
          setTierBadge('COMPLETE ✓')
          setStage('done')
          setExtractionId(eid)
        } else if(status === 'failed' || status === 'error') {
          clearInterval(pollRef.current!)
          throw new Error(data.error || data.message || 'Extraction failed on backend')
        }
        // else still running — keep polling

        // Safety timeout: stop after 5 minutes
        if(ticks > 100) {
          clearInterval(pollRef.current!)
          throw new Error('Extraction timed out after 5 minutes')
        }

      } catch(err:unknown) {
        clearInterval(pollRef.current!)
        const msg = err instanceof Error ? err.message : String(err)
        setStage('error')
        setErrorMsg(msg)
        setProgVisible(false)
      }
    }, POLL_INTERVAL)
  }

  /* ── STEP 4: DOWNLOAD ── */
  const handleDownload = async (fmt: Format) => {
    if(!extractionId) return
    const fmtMap: Record<Format,string> = {CSV:'csv', JSON:'json', EXCEL:'excel'}
    const dlUrl = `${API}/extract/${extractionId}/download/${fmtMap[fmt]}`
    window.open(dlUrl, '_blank')
  }

  /* ── RESET ── */
  const handleReset = () => {
    if(pollRef.current) clearInterval(pollRef.current)
    setUrl(''); setPrompt(''); setMaxPages(100); setFormat('CSV')
    setTags([]); setTagInput(''); setStage('idle')
    setJobId(null); setExtractionId(null)
    setProgPct(0); setProgLog(''); setTierBadge('')
    setErrorMsg(''); setProgVisible(false)
  }

  /* ── 3D TILT ── */
  const handlePanelMouseMove = (e:React.MouseEvent<HTMLDivElement>) => {
    cancelAnimationFrame(tiltRaf.current)
    const panel = panelRef.current; if(!panel) return
    tiltRaf.current = requestAnimationFrame(() => {
      const r = panel.getBoundingClientRect()
      const dx = (e.clientX-(r.left+r.width/2))/r.width
      const dy = (e.clientY-(r.top+r.height/2))/r.height
      panel.style.transform = `perspective(1100px) rotateX(${-dy*2.5}deg) rotateY(${dx*2.5}deg)`
    })
  }
  const handlePanelMouseLeave = () => {
    cancelAnimationFrame(tiltRaf.current)
    if(panelRef.current) panelRef.current.style.transform = 'perspective(1100px) rotateX(0) rotateY(0)'
  }

  const isRunning = stage === 'analyzing' || stage === 'analyzed' || stage === 'extracting'

  /* ─────────────────────────────────────────
     RENDER
  ───────────────────────────────────────── */
  return (
    <>
      <NeuralCanvas />

      {/* INTRO */}
      {!introGone && (
        <div id="intro" className={introOut ? 'out' : ''}>
          <div className="intro-scanline" />
          <div className="intro-grid" />
          <div className="intro-logo" data-text="PEGASUS">PEGASUS</div>
          <div className="intro-sub">NEURAL EXTRACTION ENGINE</div>
          <div className="intro-tagline">5-TIER AGENTIC WEB INTELLIGENCE · INITIALIZING</div>
          <div className="intro-loader">
            <div className="loader-track"><div className="loader-fill" /></div>
            <div className="loader-label">{loaderLabel}</div>
          </div>
        </div>
      )}

      {/* APP */}
      <div id="app" className={appVisible ? 'visible' : ''}>

        {/* SIDEBAR */}
        <nav className="sidebar">
          <div className="sidebar-brand">
            <div className="brand-name">PEGASUS</div>
            <div className="brand-status"><div className="s-dot" />AI NODE: ACTIVE</div>
          </div>
          <div className="sidebar-nav">
            {[
              {icon:'memory',label:'Extractor',active:true},
              {icon:'database',label:'History',active:false},
              {icon:'terminal',label:'Connectors',active:false},
              {icon:'key',label:'Tokens',active:false},
              {icon:'receipt_long',label:'Logs',active:false},
            ].map(item=>(
              <a key={item.label} className={`nav-item${item.active?' active':''}`} href="#">
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
          <header className="topbar">
            <div className="topbar-title">PEGASUS EXTRACT</div>
            <div className="topbar-right">
              <span className="topbar-status">
                <span className="status-blink">●</span>
                {stage==='idle'&&' READY'}
                {isRunning&&' PROCESSING'}
                {stage==='done'&&' EXTRACTION COMPLETE'}
                {stage==='error'&&' ERROR DETECTED'}
              </span>
              <button className="topbar-btn"><span className="material-symbols-outlined">notifications</span></button>
              <button className="topbar-btn"><span className="material-symbols-outlined">account_tree</span></button>
            </div>
          </header>

          <div className="content">
            <div className="panel" id="main-panel" ref={panelRef}
              onMouseMove={handlePanelMouseMove} onMouseLeave={handlePanelMouseLeave}>
              <div className="amb1" /><div className="amb2" />

              <div className="panel-header">
                <div className="panel-title">Extraction Command Center</div>
                <div className="panel-sub">Configure neural parsing parameters · Analysis: 15–30s · Backend: pegasus-extract.onrender.com</div>
              </div>

              <div className="timeline">
                <div className="tl-line" />

                {/* Step 1 */}
                <div className={`tl-step${activeStep===1?' active':''}`}>
                  <div className={`tl-node${activeStep===1?' active':''}`} />
                  <div className="step-lbl"><span className="material-symbols-outlined">public</span>Target Vector</div>
                  <div className="step-card">
                    <div className="step-sublbl">Primary URL</div>
                    <input type="url" placeholder="https://" value={url}
                      onChange={e=>setUrl(e.target.value)} onFocus={()=>setActiveStep(1)}
                      disabled={isRunning} />
                  </div>
                </div>

                {/* Step 2 */}
                <div className={`tl-step${activeStep===2?' active':''}`}>
                  <div className={`tl-node${activeStep===2?' active':''}`} />
                  <div className="step-lbl"><span className="material-symbols-outlined">psychology</span>Extraction Prompt</div>
                  <div className="step-card">
                    <div className="step-sublbl">Natural Language Request</div>
                    <textarea rows={3} placeholder="Describe the data to extract... e.g. 'Product names, prices, and ratings'"
                      value={prompt} onChange={e=>setPrompt(e.target.value)}
                      onFocus={()=>setActiveStep(2)} disabled={isRunning} />
                  </div>
                </div>

                {/* Step 3 */}
                <div className={`tl-step${activeStep===3?' active':''}`}>
                  <div className={`tl-node${activeStep===3?' active':''}`} />
                  <div className="step-lbl"><span className="material-symbols-outlined">data_object</span>Schema Definition</div>
                  <div className="step-card">
                    <div className="step-sublbl">Field Keys · optional, auto-detected if empty</div>
                    <div className="tags-wrap">
                      {tags.map(tag=>(
                        <span key={tag.id} className="tag" onClick={()=>!isRunning&&removeTag(tag.id)}>
                          {tag.value} <span className="tag-rm">×</span>
                        </span>
                      ))}
                    </div>
                    <div className="tag-row">
                      <input type="text" placeholder="Add field key..." value={tagInput}
                        onChange={e=>setTagInput(e.target.value)} onKeyDown={handleTagKeyDown}
                        onFocus={()=>setActiveStep(3)} disabled={isRunning} />
                      <button className="btn-add" onClick={addTag} disabled={isRunning}>
                        <span className="material-symbols-outlined">add</span>
                      </button>
                    </div>
                  </div>
                </div>

                {/* Step 4 */}
                <div className={`tl-step${activeStep===4?' active':''}`}>
                  <div className={`tl-node${activeStep===4?' active':''}`} />
                  <div className="step-lbl"><span className="material-symbols-outlined">settings</span>Output Parameters</div>
                  <div className="step-card">
                    <div className="opts-row">
                      <div className="opt-grp">
                        <div className="step-sublbl">Max Pages</div>
                        <input type="number" value={maxPages} min={1} max={9999}
                          onChange={e=>setMaxPages(Number(e.target.value))}
                          onFocus={()=>setActiveStep(4)} disabled={isRunning} />
                      </div>
                      <div className="opt-grp">
                        <div className="step-sublbl">Output Format</div>
                        <div className="fmt-toggle">
                          {(['CSV','JSON','EXCEL'] as Format[]).map(f=>(
                            <button key={f} className={`fmt-btn${format===f?' active':''}`}
                              onClick={()=>setFormat(f)} disabled={isRunning}>{f}</button>
                          ))}
                        </div>
                      </div>
                    </div>
                  </div>
                </div>
              </div>

              {/* ERROR MESSAGE */}
              {errorMsg && (
                <div style={{marginTop:'1rem',padding:'0.75rem 1rem',background:'rgba(239,68,68,0.08)',border:'1px solid rgba(239,68,68,0.25)',borderRadius:'8px',fontFamily:'var(--font-mono)',fontSize:'0.68rem',color:'#f87171',letterSpacing:'0.04em'}}>
                  ⚠ {errorMsg}
                </div>
              )}

              {/* ACTION BAR */}
              <div className="action-bar">
                <button className="btn-reset" onClick={handleReset}>CLEAR</button>

                {/* DOWNLOAD button — shown when done */}
                {stage==='done' && (
                  <button className="btn-extract" style={{background:'linear-gradient(135deg,rgba(4,191,150,0.9),rgba(6,255,200,0.6))'}}
                    onClick={()=>handleDownload(format)}>
                    <span className="shimmer" />
                    <span className="material-symbols-outlined">download</span>
                    <span>DOWNLOAD {format}</span>
                  </button>
                )}

                {/* EXTRACT button — shown when idle or error */}
                {(stage==='idle'||stage==='error') && (
                  <button className="btn-extract" onClick={handleExtract}>
                    <span className="shimmer" />
                    <span className="material-symbols-outlined">bolt</span>
                    <span>INITIATE EXTRACTION</span>
                  </button>
                )}

                {/* PROCESSING button — shown while running */}
                {isRunning && (
                  <button className="btn-extract loading" disabled>
                    <span className="shimmer" />
                    <span className="material-symbols-outlined">sync</span>
                    <span>
                      {stage==='analyzing'&&'ANALYZING...'}
                      {stage==='analyzed'&&'STARTING EXTRACTION...'}
                      {stage==='extracting'&&'EXTRACTING...'}
                    </span>
                  </button>
                )}
              </div>

              {/* PROGRESS PANEL */}
              {progVisible && (
                <div className="prog-panel vis">
                  <div className="prog-head">
                    <span className="prog-lbl">
                      {stage==='done' ? '✓ COMPLETE' : '⬡ PROCESSING'}
                    </span>
                    <span className="prog-pct">{progPct}%</span>
                  </div>
                  <div className="prog-track">
                    <div className="prog-fill" style={{width:`${progPct}%`}} />
                  </div>
                  <div className="prog-log">{progLog}</div>
                  <div className="tier-badge">{tierBadge}</div>

                  {/* Download options when done */}
                  {stage==='done' && (
                    <div style={{marginTop:'0.75rem',display:'flex',gap:'0.5rem',flexWrap:'wrap'}}>
                      {(['CSV','JSON','EXCEL'] as Format[]).map(f=>(
                        <button key={f}
                          onClick={()=>handleDownload(f)}
                          style={{padding:'0.3rem 0.8rem',background:'rgba(6,255,200,0.08)',border:'1px solid rgba(6,255,200,0.25)',borderRadius:'6px',color:'var(--cyan)',fontFamily:'var(--font-mono)',fontSize:'0.62rem',letterSpacing:'0.12em',cursor:'pointer',transition:'all 0.2s ease'}}>
                          ↓ {f}
                        </button>
                      ))}
                    </div>
                  )}
                </div>
              )}

            </div>
          </div>

          <footer>
            <div className="foot-brand">Computational Clarity Secured.</div>
            <div className="foot-links">
              <a href={`${API}/docs`} target="_blank" rel="noreferrer">API Protocol</a>
              <a href="#">Neural Logs</a>
              <a href="#">Support</a>
            </div>
          </footer>
        </div>

        {/* MOBILE NAV */}
        <nav className="mob-nav">
          {[{icon:'memory',label:'Extractor',active:true},{icon:'database',label:'History',active:false},{icon:'terminal',label:'Connect',active:false},{icon:'settings',label:'Settings',active:false}].map(item=>(
            <a key={item.label} className={`mob-item${item.active?' active':''}`} href="#">
              <span className="material-symbols-outlined">{item.icon}</span>
              <span>{item.label}</span>
            </a>
          ))}
        </nav>
      </div>
    </>
  )
}