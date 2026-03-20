"use client";

import { useEffect, useMemo, useState } from "react";

const API_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8001";

type LoadingStep = {
  label: string;
  status: "pending" | "active" | "done";
};

type Preview = {
  estimated_records?: number | string;
  estimated_time?: string;
  fields_found?: string[];
  confidence?: string;
  warnings?: string[];
};

type ClarificationQuestion = {
  field: string;
  question: string;
  type: "text" | "number" | "boolean" | "choice";
  choices: string[] | null;
  default: any;
};

type AnalysisResponse = {
  status: string;
  url: string;
  site_summary?: string;
  analysis?: any;
  extraction_plan?: any;
  preview?: Preview;
  ready_to_extract?: boolean;
  provider_used?: string;
  clarifications_needed?: boolean;
  questions?: ClarificationQuestion[];
  error?: string;
};

type ExtractionStatus = {
  status: "running" | "success" | "failed" | "pending";
  progress: number;
  current_step: string;
  records_extracted: number;
  output_files: { csv?: string; json?: string; report?: string } | null;
  error?: string | null;
};

export default function Page() {
  const [url, setUrl] = useState("");
  const [description, setDescription] = useState("");
  const [fieldInput, setFieldInput] = useState("");
  const [fields, setFields] = useState<string[]>(["name", "price", "rating"]);
  const [maxPages, setMaxPages] = useState(100);
  const [outputFormats, setOutputFormats] = useState<string[]>([
    "csv",
    "json",
  ]);

  const [jobId, setJobId] = useState<string | null>(null);
  const [analysisJobId, setAnalysisJobId] = useState<string | null>(null);
  const [analysis, setAnalysis] = useState<AnalysisResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [extractionJobId, setExtractionJobId] = useState<string | null>(null);
  const [extraction, setExtraction] = useState<ExtractionStatus | null>(null);

  const [clarificationAnswers, setClarificationAnswers] = useState<
    Record<string, any>
  >({});
  const [clarificationsSubmitted, setClarificationsSubmitted] =
    useState(false);

  const [loadingSteps, setLoadingSteps] = useState<LoadingStep[]>([
    { label: "Connecting to website...", status: "pending" },
    { label: "Loading page content...", status: "pending" },
    { label: "Analyzing structure with AI...", status: "pending" },
    { label: "Creating extraction plan...", status: "pending" },
  ]);

  const confidenceLabel = useMemo(() => {
    const c = analysis?.preview?.confidence || "";
    if (!c) return "";
    return c.toUpperCase();
  }, [analysis]);

  useEffect(() => {
    let interval: NodeJS.Timeout;

    if (jobId) {
      interval = setInterval(async () => {
        try {
          const res = await fetch(`${API_URL}/analyze/${jobId}`);
          if (!res.ok) return;
          const data: AnalysisResponse = await res.json();
          setAnalysis(data);

          if (data.status === "success" || data.status === "error") {
            setLoading(false);
            setJobId(null);
          }

          // Update loading steps progression
          if (data.status === "pending") {
            setLoadingSteps((prev) => {
              const next = [...prev];
              if (next[0].status === "pending") next[0].status = "done";
              if (next[1].status === "pending") next[1].status = "active";
              return next;
            });
          } else if (data.status === "success") {
            setLoadingSteps([
              { label: "Connecting to website...", status: "done" },
              { label: "Loading page content...", status: "done" },
              { label: "Analyzing structure with AI...", status: "done" },
              { label: "Creating extraction plan...", status: "done" },
            ]);
          }
        } catch {
          // silence errors to user; just stop polling
          setLoading(false);
          setJobId(null);
        }
      }, 2000);
    }

    return () => {
      if (interval) clearInterval(interval);
    };
  }, [jobId]);

  // Poll extraction job
  useEffect(() => {
    let interval: NodeJS.Timeout;
    if (extractionJobId) {
      interval = setInterval(async () => {
        try {
          const res = await fetch(`${API_URL}/extract/${extractionJobId}`);
          if (!res.ok) return;
          const data: ExtractionStatus = await res.json();
          setExtraction(data);
          if (data.status === "success" || data.status === "failed") {
            clearInterval(interval);
            if (data.status === "failed") {
              setExtractionJobId(null);
            }
          }
        } catch {
          setExtractionJobId(null);
        }
      }, 2000);
    }
    return () => {
      if (interval) clearInterval(interval);
    };
  }, [extractionJobId]);

  const toggleOutput = (format: string) => {
    setOutputFormats((prev) =>
      prev.includes(format)
        ? prev.filter((f) => f !== format)
        : [...prev, format]
    );
  };

  const addFieldChip = () => {
    const value = fieldInput.trim();
    if (!value) return;
    if (!fields.includes(value)) {
      setFields((prev) => [...prev, value]);
    }
    setFieldInput("");
  };

  const removeFieldChip = (name: string) => {
    setFields((prev) => prev.filter((f) => f !== name));
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);
    setAnalysis(null);
    setClarificationAnswers({});
    setClarificationsSubmitted(false);
    setLoading(true);
    setLoadingSteps([
      { label: "Connecting to website...", status: "active" },
      { label: "Loading page content...", status: "pending" },
      { label: "Analyzing structure with AI...", status: "pending" },
      { label: "Creating extraction plan...", status: "pending" },
    ]);

    try {
      const res = await fetch(`${API_URL}/analyze`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          url,
          description,
          schema_fields: fields,
          max_pages: maxPages,
        }),
      });

      if (!res.ok) {
        setLoading(false);
        return;
      }

      const data = await res.json();
      setJobId(data.job_id);
      setAnalysisJobId(data.job_id);
    } catch {
      setLoading(false);
    }
  };

  const startExtraction = async () => {
    if (!analysisJobId) return;
    if (analysis?.clarifications_needed && !clarificationsSubmitted) return;
    setExtraction(null);
    try {
      const res = await fetch(`${API_URL}/extract`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ job_id: analysisJobId, confirm: true }),
      });
      if (!res.ok) return;
      const data = await res.json();
      setExtractionJobId(data.extraction_job_id);
      setExtraction({
        status: "running",
        progress: 0,
        current_step: "Starting extraction...",
        records_extracted: 0,
        output_files: null,
        error: null,
      });
    } catch {
      // silent failure
    }
  };

  const openDownload = async (fmt: "csv" | "json") => {
    if (!extractionJobId) return;
    try {
      const url = `${API_URL}/extract/${extractionJobId}/download/${fmt}`;
      const response = await fetch(url);
      if (!response.ok) {
        throw new Error(`Download failed: ${response.status}`);
      }
      const blob = await response.blob();
      const filename = `pegasus_extract_${extractionJobId}.${fmt}`;
      const link = document.createElement("a");
      link.href = window.URL.createObjectURL(blob);
      link.download = filename;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      window.URL.revokeObjectURL(link.href);
    } catch (error) {
      console.error("Download error:", error);
      alert(`Download failed. Please try again.`);
    }
  };

  const handleClarificationChange = (
    field: string,
    value: any
  ) => {
    setClarificationAnswers((prev) => ({
      ...prev,
      [field]: value,
    }));
  };

  const submitClarifications = async () => {
    if (!analysisJobId || !analysis?.questions?.length) return;
    try {
      const res = await fetch(`${API_URL}/analyze/${analysisJobId}/clarify`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ answers: clarificationAnswers }),
      });
      if (!res.ok) return;
      const data = await res.json();
      setClarificationsSubmitted(true);
      setAnalysis((prev) =>
        prev
          ? {
              ...prev,
              extraction_plan: data.updated_plan,
              clarifications_needed: false,
              questions: [],
              ready_to_extract: true,
            }
          : prev
      );
    } catch {
      // ignore for now
    }
  };

  return (
    <main className="min-h-screen flex flex-col items-center px-4 py-10">
      <header className="w-full max-w-3xl mb-10 flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="h-10 w-10 rounded-full bg-gradient-to-tr from-pegasus-violet to-pegasus-cyan flex items-center justify-center text-xl font-bold">
            P
          </div>
          <div>
            <h1 className="text-xl font-semibold tracking-wide">
              PEGASUS EXTRACT
            </h1>
            <p className="text-sm text-slate-400">
              AI-Powered Universal Web Data Extraction
            </p>
          </div>
        </div>
        <span className="text-xs text-slate-500">
          Phase 2 · Analyzer, Planner & Extractor
        </span>
      </header>

      <section className="w-full max-w-3xl space-y-8">
        <form
          onSubmit={handleSubmit}
          className="rounded-2xl border border-slate-800 bg-gradient-to-b from-slate-900/60 to-slate-950/80 p-6 shadow-xl shadow-black/50"
        >
          <div className="space-y-6">
            <div>
              <h2 className="text-sm font-semibold text-slate-300 mb-2">
                Step 1 — Target URL
              </h2>
              <input
                type="url"
                required
                placeholder="https://"
                value={url}
                onChange={(e) => setUrl(e.target.value)}
                className="w-full rounded-lg bg-slate-900/70 border border-slate-700 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-pegasus-violet"
              />
            </div>

            <div>
              <h2 className="text-sm font-semibold text-slate-300 mb-2">
                Step 2 — What data do you want?
              </h2>
              <textarea
                required
                placeholder='Describe in plain English... e.g. "Product names, prices, ratings, and review counts"'
                value={description}
                onChange={(e) => setDescription(e.target.value)}
                rows={3}
                className="w-full rounded-lg bg-slate-900/70 border border-slate-700 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-pegasus-violet resize-none"
              />
            </div>

            <div>
              <h2 className="text-sm font-semibold text-slate-300 mb-2">
                Step 3 — Fields (optional, auto-detected if empty)
              </h2>
              <div className="flex gap-2 mb-2">
                <input
                  type="text"
                  placeholder="Add field"
                  value={fieldInput}
                  onChange={(e) => setFieldInput(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === "Enter") {
                      e.preventDefault();
                      addFieldChip();
                    }
                  }}
                  className="flex-1 rounded-lg bg-slate-900/70 border border-slate-700 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-pegasus-violet"
                />
                <button
                  type="button"
                  onClick={addFieldChip}
                  className="px-3 py-2 rounded-lg border border-pegasus-violet text-xs font-medium text-pegasus-violet hover:bg-pegasus-violet/10"
                >
                  + Add field
                </button>
              </div>
              <div className="flex flex-wrap gap-2">
                {fields.map((f) => (
                  <span
                    key={f}
                    className="inline-flex items-center gap-1 rounded-full border border-slate-700 bg-slate-900/70 px-3 py-1 text-xs"
                  >
                    {f}
                    <button
                      type="button"
                      onClick={() => removeFieldChip(f)}
                      className="text-slate-500 hover:text-slate-300"
                    >
                      ×
                    </button>
                  </span>
                ))}
              </div>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div>
                <h2 className="text-sm font-semibold text-slate-300 mb-2">
                  Step 4 — Options
                </h2>
                <label className="block text-xs text-slate-400 mb-1">
                  Max pages
                </label>
                <input
                  type="number"
                  min={1}
                  max={10000}
                  value={maxPages}
                  onChange={(e) => setMaxPages(Number(e.target.value))}
                  className="w-28 rounded-lg bg-slate-900/70 border border-slate-700 px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-pegasus-violet"
                />
              </div>

              <div>
                <h2 className="text-sm font-semibold text-slate-300 mb-2">
                  Output
                </h2>
                <div className="flex flex-wrap gap-2">
                  {["csv", "json", "excel"].map((format) => (
                    <button
                      key={format}
                      type="button"
                      onClick={() => toggleOutput(format)}
                      className={`px-3 py-1.5 rounded-full text-xs border ${
                        outputFormats.includes(format)
                          ? "border-pegasus-cyan bg-pegasus-cyan/10 text-pegasus-cyan"
                          : "border-slate-700 text-slate-400"
                      }`}
                    >
                      {format.toUpperCase()}
                    </button>
                  ))}
                </div>
              </div>
            </div>
          </div>

          <div className="mt-6 flex items-center justify-between">
            <p className="text-xs text-slate-500">
              Analysis takes 15–30 seconds depending on the site.
            </p>
            <button
              type="submit"
              disabled={loading}
              className="inline-flex items-center gap-2 rounded-lg bg-pegasus-violet px-4 py-2 text-sm font-semibold shadow-lg shadow-pegasus-violet/40 hover:bg-pegasus-violet/90 disabled:opacity-60"
            >
              <span>🔍</span>
              <span>{loading ? "Analyzing..." : "Analyze Site"}</span>
            </button>
          </div>
        </form>

        {loading && (
          <div className="rounded-2xl border border-slate-800 bg-slate-950/70 p-4 text-sm text-slate-200 space-y-3">
            <div className="font-medium flex items-center gap-2">
              <span className="h-2 w-2 rounded-full bg-pegasus-cyan animate-pulse" />
              Running analysis pipeline...
            </div>
            <p className="text-xs text-slate-400">
              This typically takes 15–30 seconds. You can keep this tab open
              while PEGASUS EXTRACT inspects the site and designs an extraction
              plan.
            </p>
            <ul className="space-y-1.5 text-xs">
              {loadingSteps.map((step, idx) => (
                <li key={idx} className="flex items-center gap-2">
                  <span className="w-4 text-center">
                    {step.status === "done" ? "✓" : step.status === "active" ? "●" : "○"}
                  </span>
                  <span
                    className={
                      step.status === "active"
                        ? "text-pegasus-cyan"
                        : step.status === "done"
                        ? "text-slate-300"
                        : "text-slate-500"
                    }
                  >
                    {step.label}
                  </span>
                </li>
              ))}
            </ul>
          </div>
        )}

        {error && (
          <div className="rounded-xl border border-red-700 bg-red-950/40 px-4 py-3 text-sm text-red-100">
            {error}
          </div>
        )}

        {analysis && analysis.status === "success" && (
          <div className="rounded-2xl border border-emerald-700 bg-emerald-950/40 p-5 text-sm text-slate-100 space-y-3">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2">
                <span className="h-6 w-6 rounded-full bg-emerald-500 flex items-center justify-center text-xs">
                  ✓
                </span>
                <div>
                  <div className="text-sm font-semibold">
                    Site Analysis Complete
                  </div>
                  <div className="text-xs text-slate-400">
                    Ready to send to extraction engine.
                  </div>
                </div>
              </div>
              {confidenceLabel && (
                <div className="text-xs text-right">
                  <div className="text-slate-400">Confidence</div>
                  <div className="font-semibold text-emerald-300">
                    {confidenceLabel} ●
                  </div>
                </div>
              )}
            </div>

            <div className="mt-2 text-xs text-slate-400">
              <div className="font-mono text-pegasus-cyan">
                {new URL(analysis.url).host}
              </div>
              {analysis.site_summary && (
                <p className="mt-1">&quot;{analysis.site_summary}&quot;</p>
              )}
            </div>

            <div className="mt-3 grid grid-cols-1 md:grid-cols-2 gap-4 text-xs">
              <div>
                <div className="font-semibold text-slate-300 mb-1">
                  Fields found
                </div>
                <div className="flex flex-wrap gap-1.5">
                  {analysis.preview?.fields_found?.map((f) => (
                    <span
                      key={f}
                      className="rounded-full border border-slate-700 bg-slate-900/60 px-2 py-1"
                    >
                      {f}
                    </span>
                  ))}
                </div>
              </div>
              <div className="space-y-1">
                <div>
                  <span className="text-slate-400">Pagination: </span>
                  <span className="text-slate-100">
                    {analysis.analysis?.pagination?.type || "unknown"}
                  </span>
                </div>
                <div>
                  <span className="text-slate-400">JS Required: </span>
                  <span className="text-slate-100">
                    {analysis.analysis?.js_rendering_required ? "Yes" : "No"}
                  </span>
                </div>
                <div>
                  <span className="text-slate-400">Est. Records: </span>
                  <span className="text-slate-100">
                    {analysis.preview?.estimated_records ?? "unknown"}
                  </span>
                </div>
                <div>
                  <span className="text-slate-400">Est. Time: </span>
                  <span className="text-slate-100">
                    {analysis.preview?.estimated_time ?? "unknown"}
                  </span>
                </div>
              </div>
            </div>

            {analysis.clarifications_needed && analysis.questions && (
              <div className="mt-4 rounded-xl border border-amber-700 bg-amber-950/40 p-4 space-y-3">
                <div className="text-xs font-semibold text-amber-200">
                  This site needs a bit more info before extraction.
                </div>
                <div className="space-y-3">
                  {analysis.questions.map((q) => (
                    <div key={q.field} className="space-y-1">
                      <label className="block text-xs text-slate-200">
                        {q.question}
                      </label>
                      {q.type === "choice" && q.choices ? (
                        <select
                          className="w-full rounded-md bg-slate-900/70 border border-slate-700 px-2 py-1 text-xs"
                          value={
                            clarificationAnswers[q.field] ??
                            q.default ??
                            ""
                          }
                          onChange={(e) =>
                            handleClarificationChange(
                              q.field,
                              e.target.value
                            )
                          }
                        >
                          {q.choices.map((c) => (
                            <option key={c} value={c}>
                              {c}
                            </option>
                          ))}
                        </select>
                      ) : q.type === "boolean" ? (
                        <select
                          className="w-full rounded-md bg-slate-900/70 border border-slate-700 px-2 py-1 text-xs"
                          value={
                            clarificationAnswers[q.field] ??
                            q.default ??
                            "false"
                          }
                          onChange={(e) =>
                            handleClarificationChange(
                              q.field,
                              e.target.value === "true"
                            )
                          }
                        >
                          <option value="true">Yes</option>
                          <option value="false">No</option>
                        </select>
                      ) : (
                        <input
                          className="w-full rounded-md bg-slate-900/70 border border-slate-700 px-2 py-1 text-xs"
                          type={q.type === "number" ? "number" : "text"}
                          defaultValue={
                            clarificationAnswers[q.field] ?? q.default ?? ""
                          }
                          onChange={(e) =>
                            handleClarificationChange(
                              q.field,
                              q.type === "number"
                                ? Number(e.target.value)
                                : e.target.value
                            )
                          }
                        />
                      )}
                    </div>
                  ))}
                </div>
                <button
                  type="button"
                  onClick={submitClarifications}
                  className="mt-2 inline-flex items-center gap-2 rounded-lg bg-amber-400/90 px-3 py-1.5 text-xs font-medium text-slate-950 hover:bg-amber-300"
                >
                  <span>✓</span>
                  <span>Save answers</span>
                </button>
              </div>
            )}

            <div className="mt-3 flex flex-wrap gap-3 text-xs">
              <button className="inline-flex items-center gap-2 rounded-lg border border-slate-700 bg-slate-900/50 px-3 py-1.5 hover:bg-slate-800/80">
                <span>📋</span>
                <span>View Full Plan</span>
              </button>
              <button
                type="button"
                onClick={startExtraction}
                disabled={
                  analysis.clarifications_needed && !clarificationsSubmitted
                }
                className="inline-flex items-center gap-2 rounded-lg bg-pegasus-cyan/90 px-3 py-1.5 font-medium text-slate-900 hover:bg-pegasus-cyan disabled:opacity-60"
              >
                <span>🚀</span>
                <span>Extract</span>
              </button>
            </div>
          </div>
        )}

        {extraction && extraction.status !== "success" && (
          <div className="rounded-2xl border border-amber-700 bg-amber-950/40 p-5 text-sm text-slate-100 space-y-3">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2">
                <span className="h-6 w-6 rounded-full bg-amber-400 flex items-center justify-center text-xs">
                  ⚡
                </span>
                <div>
                  <div className="text-sm font-semibold">
                    EXTRACTION IN PROGRESS
                  </div>
                  <div className="text-xs text-slate-200">
                    {extraction.current_step}
                  </div>
                </div>
              </div>
              <div className="text-xs text-right">
                <div className="text-slate-400 mb-1">Progress</div>
                <div className="w-40 h-2 rounded-full bg-slate-800 overflow-hidden">
                  <div
                    className="h-full bg-gradient-to-r from-pegasus-violet to-pegasus-cyan"
                    style={{ width: `${extraction.progress}%` }}
                  />
                </div>
                <div className="mt-1 text-slate-200">
                  {extraction.progress}%
                </div>
              </div>
            </div>
            <div className="text-xs text-slate-300 mt-2">
              Records extracted:{" "}
              <span className="font-semibold">
                {extraction.records_extracted}
              </span>
            </div>
          </div>
        )}

        {extraction && extraction.status === "success" && (
          <div className="rounded-2xl border border-emerald-700 bg-emerald-950/60 p-5 text-sm text-slate-100 space-y-3">
            <div className="flex items-center gap-2">
              <span className="h-7 w-7 rounded-full bg-emerald-500 flex items-center justify-center text-sm">
                ✓
              </span>
              <div>
                <div className="text-sm font-semibold">
                  EXTRACTION COMPLETE!
                </div>
                <div className="text-xs text-slate-300">
                  {extraction.records_extracted} records extracted.
                </div>
              </div>
            </div>
            <div className="mt-2 text-xs text-slate-300 space-y-1">
              <div>
                Success rate:{" "}
                <span className="font-semibold">
                  {/* success rate is in backend report; keep simple here */}
                  —
                </span>
              </div>
            </div>
            <div className="mt-3 flex flex-wrap gap-3 text-xs">
              <button
                type="button"
                onClick={() => openDownload("csv")}
                className="inline-flex items-center gap-2 rounded-lg bg-slate-100 px-3 py-1.5 font-medium text-slate-900 hover:bg-white"
              >
                <span>📥</span>
                <span>Download CSV</span>
              </button>
              <button
                type="button"
                onClick={() => openDownload("json")}
                className="inline-flex items-center gap-2 rounded-lg bg-slate-800 px-3 py-1.5 font-medium text-slate-100 hover:bg-slate-700"
              >
                <span>📥</span>
                <span>Download JSON</span>
              </button>
            </div>
          </div>
        )}
      </section>
    </main>
  );
}

