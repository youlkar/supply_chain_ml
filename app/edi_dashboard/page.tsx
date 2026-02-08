"use client";
import React, { useEffect, useState, useRef } from "react";
import { Badge, Button, Card, Progress, Table } from "tabler-react";
import { createClient } from "@supabase/supabase-js";
import {
  Upload,
  AlertCircle,
  Layers,
  Activity,
  Zap,
  ChevronRight,
  BarChart2,
  ShieldCheck,
  Target,
  Users,
  DollarSign,
  Clock,
  Download,
} from "lucide-react";

// Types aligned with the new Enterprise Schema
type IntelligenceRow = {
  id: string;
  poNumber: string;
  supplier: string;
  anomalyType: string; // label_what
  targetWho: string; // label_who
  mitigation: string; // label_mitigation
  confidence: number;
  exposureUsd: number;
  status: "NEW" | "IN_PROGRESS" | "RESOLVED";
  priority: "P1" | "P2" | "P3";
};

type DashboardKpis = {
  openCount: number;
  atRisk: number;
  exceptionRate: number;
  modelHealth: string;
  macroF1: number;
};

// Job status shape (assumed). Backend should return something like this.
type IngestJobStatus = {
  job_id: string;
  status: "QUEUED" | "RUNNING" | "VALIDATING" | "PROCESSING" | "DONE" | "FAILED";
  error?: unknown; // ✅ allow object errors too
  data?: any[];
};

export default function EdiIntelligenceDashboard() {
  const fileInputRef = useRef<HTMLInputElement>(null);

  // ✅ prevents repeated “Processing complete” / “Processing failed” toasts
  const lastTerminalJobRef = useRef<string | null>(null);

  const [data, setData] = useState<IntelligenceRow[]>([]);
  const [isUploading, setIsUploading] = useState(false);
  const [kpis, setKpis] = useState<DashboardKpis>({
    openCount: 0,
    atRisk: 0,
    exceptionRate: 0,
    modelHealth: "Stable",
    macroF1: 0.992,
  });
  const [lastUpdated, setLastUpdated] = useState<string>("");
  const [modalOpen, setModalOpen] = useState(false);
  const [attachedFiles, setAttachedFiles] = useState<File[]>([]);
  const [pendingFiles, setPendingFiles] = useState<File[]>([]);
  const [uploadStatus, setUploadStatus] = useState<{
    state: "idle" | "uploading" | "success" | "error";
    message?: string;
  }>({ state: "idle" });

  // Track ingest job
  const [activeJobId, setActiveJobId] = useState<string | null>(null);
  const [activeJobStatus, setActiveJobStatus] = useState<IngestJobStatus["status"] | null>(null);

  // Toast-style notice for background processing
  const [notice, setNotice] = useState<{
    open: boolean;
    type: "info" | "success" | "error";
    title: string;
    message: string;
  }>({
    open: false,
    type: "info",
    title: "",
    message: "",
  });

  const API_BASE = process.env.NEXT_PUBLIC_EDI_BACKEND_URL || "http://localhost:8000";

  const SUPABASE_URL = process.env.NEXT_PUBLIC_SUPABASE_URL || "";
  const SUPABASE_ANON_KEY = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY || "";
  const SUPABASE_BUCKET = process.env.NEXT_PUBLIC_SUPABASE_EDI_BUCKET || "edi-uploads";

  const supabase =
    SUPABASE_URL && SUPABASE_ANON_KEY ? createClient(SUPABASE_URL, SUPABASE_ANON_KEY) : null;

  // Demo default buyer id
  const BUYER_ID = "6880007b-9993-4e6a-9355-685b86300062";

  const makeBatchId = () => {
    // @ts-ignore
    if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") return crypto.randomUUID();
    return `batch_${Date.now()}_${Math.random().toString(16).slice(2)}`;
  };

  const mapBackendResult = (payload: any): IntelligenceRow => ({
    id: payload.id,
    poNumber: payload.poNumber,
    supplier: payload.supplier,
    anomalyType: payload.anomalyType,
    targetWho: payload.targetWho,
    mitigation: payload.mitigation,
    confidence: payload.confidence,
    exposureUsd: payload.estImpactUsd,
    status: "NEW",
    priority: payload.confidence < 0.8 || payload.estImpactUsd > 1000 ? "P1" : "P2",
  });

  const classifyFile = (filename: string) => {
    const lowerName = filename.toLowerCase();
    if (lowerName.includes("__asn") || lowerName.includes("-asn") || lowerName.includes("_asn")) return "asn";
    if (lowerName.includes("__inv") || lowerName.includes("-inv") || lowerName.includes("_inv")) return "invoice";
    if (lowerName.startsWith("po") && lowerName.endsWith(".edi")) return "po";
    return null;
  };

  // IMPORTANT: Must match DB check constraint: PO | ASN | INV
  const inferDocumentType = (filename: string): "PO" | "ASN" | "INV" => {
    const k = classifyFile(filename);
    if (k === "po") return "PO";
    if (k === "asn") return "ASN";
    if (k === "invoice") return "INV";
    return "INV";
  };

  // ✅ Unified “read response body” helper (gives you exact backend/edge errors)
  const readResponseBody = async (resp: Response): Promise<{ text: string; json: any | null }> => {
    const text = await resp.text().catch(() => "");
    let json: any | null = null;
    try {
      json = text ? JSON.parse(text) : null;
    } catch {
      json = null;
    }
    return { text, json };
  };

  // ✅ Robust error formatter (prevents [object Object])
  const formatBackendDetail = (detail: unknown): string => {
    if (!detail) return "Backend failed to process request.";
    if (typeof detail === "string") return detail;

    if (typeof detail === "object") {
      const d: any = detail;

      // FastAPI typical
      if (typeof d.detail === "string") return d.detail;
      if (Array.isArray(d.detail)) {
        return d.detail
          .map((x: any) => x?.msg || x?.message || JSON.stringify(x))
          .join(" | ");
      }

      // Preferred: { error: { message, code, details } } or { error: "..." }
      if (typeof d.error === "string") return d.error;
      if (d.error && typeof d.error === "object") {
        if (typeof d.error.message === "string") return d.error.message;
        if (typeof d.error.details === "string") return d.error.details;
        try {
          return JSON.stringify(d.error);
        } catch {
          return "Backend returned an unreadable error object.";
        }
      }

      if (typeof d.message === "string") return d.message;

      try {
        return JSON.stringify(detail);
      } catch {
        return "Backend returned an unreadable error object.";
      }
    }

    return String(detail);
  };

  const stringifyErrorPayload = (value: unknown): string => {
    if (!value) return "Unknown error";
    if (typeof value === "string") return value;
    if (typeof value === "object") {
      try {
        return JSON.stringify(value);
      } catch {
        return String(value);
      }
    }
    return String(value);
  };

  // ✅ Safer: always turns any caught value into a readable string
  const formatUnknownError = (err: unknown): string => {
    if (!err) return "Unknown error";
    if (typeof err === "string") return err;
    if (err instanceof Error) return err.message || "Error";
    try {
      return formatBackendDetail(err);
    } catch {
      try {
        return JSON.stringify(err);
      } catch {
        return String(err);
      }
    }
  };

  const fetchDashboard = async () => {
    try {
      const resp = await fetch(`${API_BASE}/api/edi/dashboard`);
      if (!resp.ok) throw new Error("Backend offline");
      const payload = await resp.json();
      setKpis(payload.kpis);
      setLastUpdated(payload.lastUpdatedAt);
    } catch (err) {
      console.error("Dashboard Sync Error:", err);
    }
  };

  const handleFiles = (fileList: FileList | null) => {
    const files = fileList ? Array.from(fileList) : [];
    if (!files.length) return;

    const deduped = files.filter(
      (file) =>
        !attachedFiles.some(
          (attached) => attached.name === file.name && attached.lastModified === file.lastModified
        )
    );

    if (!deduped.length) {
      setUploadStatus({ state: "error", message: "Document with that name already added." });
      return;
    }

    setAttachedFiles((prev) => [...prev, ...deduped]);
    setPendingFiles((prev) => [...prev, ...deduped]);
  };

  const extractPoNumber = (filename: string) => {
    const match = filename.match(/(PO\d+)/i);
    return match ? match[1].toUpperCase() : null;
  };

  const uploadFileToSupabase = async (
    file: File,
    buyerId: string,
    _batchId: string,
    docType: "PO" | "ASN" | "INV",
    poNumber: string
  ) => {
    if (!supabase) {
      throw new Error(
        "Supabase not configured. Set NEXT_PUBLIC_SUPABASE_URL and NEXT_PUBLIC_SUPABASE_ANON_KEY in .env.local"
      );
    }

    const safeName = file.name.replace(/[^a-zA-Z0-9._-]/g, "_");

    const objectPath = `staging/${buyerId || "unknown_buyer"}/${poNumber || "unknown_po"}/${docType}/${safeName}`;

    const { error } = await supabase.storage.from(SUPABASE_BUCKET).upload(objectPath, file, {
      cacheControl: "3600",
      upsert: true,
      contentType: file.type || "application/octet-stream",
    });

    if (error) throw new Error(`Supabase upload failed for ${file.name}: ${error.message}`);
    return objectPath;
  };

  const handleConfirmUploads = async () => {
    const poFiles = pendingFiles.filter((file) => classifyFile(file.name) === "po");
    const poNumber = poFiles.length ? extractPoNumber(poFiles[0].name) : null;
    const asnFiles = pendingFiles.filter((file) => classifyFile(file.name) === "asn");
    const invoiceFiles = pendingFiles.filter((file) => classifyFile(file.name) === "invoice");

    if (poFiles.length !== 1 || asnFiles.length === 0 || invoiceFiles.length === 0) {
      setUploadStatus({ state: "error", message: "Select exactly 1 PO plus at least 1 ASN and 1 invoice." });
      return;
    }

    if (!poNumber) {
      setUploadStatus({ state: "error", message: "Unable to read PO number from PO file." });
      return;
    }

    setUploadStatus({ state: "uploading", message: "Uploading to storage..." });
    setIsUploading(true);

    // ✅ reset terminal toast guard for new submission
    lastTerminalJobRef.current = null;

    // ✅ reset current job UI
    setActiveJobId(null);
    setActiveJobStatus(null);

    try {
      const allFiles = [...poFiles, ...asnFiles, ...invoiceFiles];
      const batchId = makeBatchId();

      const uploaded: Array<{ filename: string; doc_type: "PO" | "ASN" | "INV"; storage_path: string }> = [];

      for (const f of allFiles) {
        const docType = inferDocumentType(f.name);
        const storagePath = await uploadFileToSupabase(f, BUYER_ID, batchId, docType, poNumber);
        uploaded.push({ filename: f.name, doc_type: docType, storage_path: storagePath });
      }

      setUploadStatus({ state: "uploading", message: "Submitting batch for validation..." });

      const kickoffResp = await fetch(`${API_BASE}/api/edi/ingest`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          buyer_id: BUYER_ID,
          expected_po: poNumber,
          batch_id: batchId,
          storage: {
            provider: "supabase",
            bucket: SUPABASE_BUCKET,
            prefix: `staging/${BUYER_ID}/${poNumber}`,
          },
          files: uploaded,
        }),
      });

      if (!kickoffResp.ok) {
        const { text, json } = await readResponseBody(kickoffResp);
        console.error("Kickoff failed:", kickoffResp.status, text || json);
        throw new Error(formatBackendDetail(json ?? text) || `Kickoff failed (${kickoffResp.status})`);
      }

      const kickoffPayload: any = await kickoffResp.json().catch(async () => {
        const { text } = await readResponseBody(kickoffResp);
        throw new Error(`Kickoff returned invalid JSON: ${text || "(empty body)"}`);
      });

      const jobId: string | undefined = kickoffPayload?.job_id || kickoffPayload?.jobId || kickoffPayload?.id;

      if (!jobId) {
        if (Array.isArray(kickoffPayload?.data) && kickoffPayload.data.length) {
          setData(kickoffPayload.data.map(mapBackendResult));
          await fetchDashboard();
          setUploadStatus({ state: "success", message: "Files processed successfully." });
          setPendingFiles([]);
          setModalOpen(false);

          setNotice({
            open: true,
            type: "success",
            title: "Processing complete",
            message: "This batch finished processing and has been written to the database.",
          });
          return;
        }
        throw new Error("Backend did not return job_id. Implement async ingest response: { job_id, status }.");
      }

      setActiveJobId(jobId);
      setActiveJobStatus("QUEUED");

      setUploadStatus({ state: "success", message: "Ingest accepted. Processing in background..." });
      setPendingFiles([]);
      setModalOpen(false);

      setNotice({
        open: true,
        type: "info",
        title: "Upload complete",
        message:
          "Your files were uploaded successfully. Processing has started in the background — you can keep using the dashboard while we validate and reconcile this batch.",
      });
    } catch (err: unknown) {
      const msg = formatUnknownError(err);
      console.error("Ingest error (raw):", err);

      setUploadStatus({ state: "error", message: msg });

      setNotice({
        open: true,
        type: "error",
        title: "Upload failed",
        message: msg || "We couldn't submit this batch. Please try again.",
      });
    } finally {
      setIsUploading(false);
      if (fileInputRef.current) fileInputRef.current.value = "";
    }
  };

  const handleFileInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    handleFiles(e.target.files);
  };

  const handleDrop = (e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    handleFiles(e.dataTransfer.files);
  };

  useEffect(() => {
    fetchDashboard();
    const interval = setInterval(fetchDashboard, 30000);
    return () => clearInterval(interval);
  }, []);

  // Auto-dismiss non-error notices
  useEffect(() => {
    if (!notice.open) return;
    if (notice.type === "error") return;

    const t = setTimeout(() => {
      setNotice((n) => ({ ...n, open: false }));
    }, 7000);

    return () => clearTimeout(t);
  }, [notice.open, notice.type]);

  // Background job polling (non-blocking) + ✅ improved error body logging + ✅ terminal toast dedupe
  useEffect(() => {
    if (!activeJobId) return;

    let cancelled = false;
    const pollEveryMs = 2000;

    const tick = async () => {
      try {
        const resp = await fetch(`${API_BASE}/api/edi/jobs/${activeJobId}`);

        if (!resp.ok) {
          const { text, json } = await readResponseBody(resp);
          console.error("Job status fetch failed:", resp.status, text || json);

          // best-effort UI
          setUploadStatus({
            state: "uploading",
            message: "Processing... (status check temporarily unavailable)",
          });
          return;
        }

        const statusPayload = (await resp.json()) as IngestJobStatus;
        if (cancelled) return;

        setActiveJobStatus(statusPayload.status);

        if (statusPayload.status === "FAILED") {
          // ✅ normalize to readable string (prevents [object Object])
          const rawErr =
            (statusPayload as any).error ??
            (statusPayload as any).detail ??
            (statusPayload as any).message ??
            statusPayload;

          const msg =
            formatBackendDetail(rawErr) ||
            stringifyErrorPayload(rawErr) ||
            "Ingest job failed (no error provided).";

          console.error("Job FAILED payload:", statusPayload);

          setUploadStatus({ state: "error", message: msg });

          if (lastTerminalJobRef.current !== activeJobId) {
            lastTerminalJobRef.current = activeJobId;
            setNotice({
              open: true,
              type: "error",
              title: "Processing failed",
              message: msg,
            });
          }

          setIsUploading(false);
          setActiveJobId(null);
          setActiveJobStatus(null);
          return;
        }

        if (statusPayload.status === "DONE") {
          if (Array.isArray((statusPayload as any).data) && (statusPayload as any).data.length) {
            setData((statusPayload as any).data.map(mapBackendResult));
          } else {
            await fetchDashboard();
          }

          setUploadStatus({ state: "success", message: "Ingest completed." });

          if (lastTerminalJobRef.current !== activeJobId) {
            lastTerminalJobRef.current = activeJobId;
            setNotice({
              open: true,
              type: "success",
              title: "Processing complete",
              message:
                "This batch finished processing and has been written to the database. Dashboard metrics will reflect the latest results.",
            });
          }

          setIsUploading(false);
          setActiveJobId(null);
          setActiveJobStatus(null);
          return;
        }

        const msg =
          statusPayload.status === "QUEUED"
            ? "Queued..."
            : statusPayload.status === "RUNNING"
            ? "Starting..."
            : statusPayload.status === "VALIDATING"
            ? "Validating documents..."
            : "Processing & writing to DB...";
        setUploadStatus({ state: "uploading", message: msg });
      } catch (err) {
        console.error("Polling tick error:", err);
      }
    };

    tick();
    const t = setInterval(tick, pollEveryMs);

    return () => {
      cancelled = true;
      clearInterval(t);
    };
  }, [activeJobId, API_BASE]);

  return (
    <div className="min-h-screen bg-[#F8FAFD] font-sans antialiased text-[#1A237E]">
      <input
        type="file"
        ref={fileInputRef}
        className="hidden"
        onChange={handleFileInputChange}
        multiple
        accept=".csv,.edi,.txt"
      />

      <div className="flex h-screen overflow-hidden">
        {/* Sidebar Nav with Model Health integration */}
        <aside className="hidden w-72 bg-white border-r border-slate-200 lg:flex flex-col p-8 shadow-sm">
          <div className="flex items-center gap-3 mb-12">
            <div className="h-10 w-10 rounded-2xl bg-gradient-to-br from-indigo-600 to-blue-500 shadow-blue-200 shadow-lg flex items-center justify-center text-white">
              <Layers size={22} />
            </div>
            <span className="text-xl font-black tracking-tighter">SupplyLens</span>
          </div>

          <div className="space-y-2 flex-1">
            <SidebarItem icon={<Activity size={18} />} label="Intelligence Hub" active />
            <SidebarItem icon={<Users size={18} />} label="Vendor Rankings" />
            <SidebarItem icon={<ShieldCheck size={18} />} label="Audit Integrity" />
            <SidebarItem icon={<Target size={18} />} label="ML Model Lab" />
          </div>

          <div className="p-5 rounded-3xl bg-indigo-50 border border-indigo-100 shadow-inner">
            <div className="flex items-center gap-2 mb-3">
              <div className="h-2 w-2 rounded-full bg-green-500 animate-pulse"></div>
              <span className="text-[10px] font-extrabold text-indigo-900 uppercase tracking-widest">
                Model: {kpis.modelHealth}
              </span>
            </div>
            <p className="text-[11px] text-indigo-700 leading-relaxed font-medium">
              Last Eval:{" "}
              <span className="font-black text-indigo-900">{(kpis.macroF1 * 100).toFixed(1)}% F1</span>
            </p>
          </div>
        </aside>

        {/* Main Content Area */}
        <main className="flex-1 overflow-y-auto p-10">
          <header className="mb-10 flex items-center justify-between">
            <div>
              <h1 className="text-3xl font-black text-slate-900 tracking-tight">Strategy & Intelligence</h1>
              <p className="text-sm font-semibold text-slate-400 mt-1">
                Prescriptive analytics for supply chain automation.
              </p>

              {activeJobId && (
                <p className="mt-2 text-[11px] font-bold text-slate-400">
                  Active job: <span className="font-black text-slate-700">{activeJobId}</span>
                  {activeJobStatus && (
                    <span className="ml-2">
                      • <span className="font-black text-slate-700">{activeJobStatus}</span>
                    </span>
                  )}
                </p>
              )}
            </div>

            <div className="flex gap-4">
              <Button
                onClick={() => setModalOpen(true)}
                disabled={isUploading}
                className="rounded-2xl bg-indigo-600 px-7 py-3 text-xs font-black text-white shadow-xl shadow-indigo-100 hover:bg-indigo-700 transition-all hover:-translate-y-0.5"
              >
                {isUploading ? "Processing..." : "Ingest EDI Documents"}
              </Button>
            </div>
          </header>

          {/* Metric Bar */}
          <div className="mb-10 grid grid-cols-1 gap-6 sm:grid-cols-2 lg:grid-cols-4">
            <IntelligenceCard
              title="Exposure at Risk"
              value={`$${kpis.atRisk.toLocaleString()}`}
              trend="-3%"
              sub="Active Discrepancies"
              icon={<DollarSign />}
              color="red"
            />
            <IntelligenceCard
              title="Touchless Rate"
              value={`${(100 - kpis.exceptionRate * 100).toFixed(1)}%`}
              trend="+4%"
              sub="AI Auto-Approval"
              icon={<Zap />}
              color="blue"
            />
            <IntelligenceCard
              title="Open Exceptions"
              value={kpis.openCount}
              trend="Stable"
              sub="In Queue"
              icon={<AlertCircle />}
              color="indigo"
            />
            <IntelligenceCard
              title="Mean Recon Time"
              value="1.4h"
              trend="-22m"
              sub="Model Response"
              icon={<Clock size={20} />}
              color="indigo"
            />
          </div>

          <div className="mb-10 grid grid-cols-1 gap-8 lg:grid-cols-3">
            <Card className="lg:col-span-2 border-none shadow-sm rounded-[32px] bg-white p-8">
              <div className="flex items-center justify-between mb-8">
                <div>
                  <h3 className="text-xl font-black text-slate-900">Financial Leakage Trends</h3>
                  <p className="text-xs font-bold text-slate-400">Model accuracy vs volume trends.</p>
                </div>
              </div>
              <div className="flex h-[300px] w-full items-center justify-center rounded-3xl bg-slate-50 border-2 border-dashed border-slate-200/60">
                <div className="text-center">
                  <BarChart2 size={40} className="mx-auto text-slate-200 mb-3" />
                  <p className="text-sm font-black text-slate-300 uppercase tracking-widest">
                    MLflow Time-Series Pipeline
                  </p>
                </div>
              </div>
            </Card>

            <Card className="border-none shadow-sm rounded-[32px] bg-white p-8 flex flex-col">
              <h3 className="text-xl font-black text-slate-900 mb-6 tracking-tight">Systemic Root Causes</h3>
              <div className="space-y-6 flex-1">
                <StatProgress label="UOM Master Data Sync" value={78} color="indigo" />
                <StatProgress label="Tax Rate Compliance" value={98} color="green" />
                <StatProgress label="Vendor Pricing Drift" value={14} color="red" />
                <StatProgress label="Duplicate Entry Filter" value={92} color="blue" />
              </div>
            </Card>
          </div>

          {/* Live Action Queue */}
          <Card className="border-none shadow-sm rounded-[32px] bg-white overflow-hidden">
            <div className="px-10 py-8 border-b border-slate-100 flex justify-between items-center bg-white">
              <div>
                <h3 className="text-xl font-black text-slate-900">Prescriptive Action Queue</h3>
                <span className="text-xs text-slate-400">Refreshed: {lastUpdated || "Syncing..."}</span>
              </div>
              <div className="flex gap-3">
                <Button
                  size="sm"
                  className="bg-indigo-50 text-indigo-600 border-none font-black text-[10px] rounded-xl px-4 py-2 hover:bg-indigo-100 transition-colors"
                >
                  Batch Resolve
                </Button>
                <button className="p-2 bg-slate-50 rounded-xl text-slate-400 hover:text-slate-600 transition-colors">
                  <Download size={16} />
                </button>
              </div>
            </div>

            <div className="overflow-x-auto">
              <Table className="w-full">
                <thead>
                  <tr className="bg-slate-50/50 text-left text-[11px] font-black text-slate-400 uppercase tracking-widest">
                    <th className="px-10 py-5">Anomaly (WHAT)</th>
                    <th className="px-10 py-5">Exposure</th>
                    <th className="px-10 py-5">Owner (WHO)</th>
                    <th className="px-10 py-5">Mitigation Plan</th>
                    <th className="px-10 py-5 text-right">Certainty</th>
                  </tr>
                </thead>

                <tbody className="divide-y divide-slate-100 bg-white">
                  {data.map((row) => (
                    <tr key={row.id} className="group hover:bg-slate-50/70 transition-all cursor-pointer">
                      <td className="px-10 py-7">
                        <div className="flex items-center gap-4">
                          <div
                            className={`h-2.5 w-2.5 rounded-full ${
                              row.priority === "P1" ? "bg-red-500" : "bg-indigo-400"
                            } shadow-lg`}
                          ></div>
                          <div>
                            <div className="text-sm font-black text-slate-900">{row.anomalyType}</div>
                            <div className="text-[11px] font-bold text-slate-400 mt-0.5">
                              {row.poNumber} • {row.supplier}
                            </div>
                          </div>
                        </div>
                      </td>

                      <td className="px-10 py-7 text-sm font-black text-slate-900">
                        ${row.exposureUsd.toLocaleString()}
                      </td>

                      <td className="px-10 py-7">
                        <Badge
                          color={row.targetWho === "Vendor" ? "red" : "indigo"}
                          className="rounded-xl px-3 py-1 font-black uppercase text-[10px] tracking-widest"
                        >
                          {row.targetWho}
                        </Badge>
                      </td>

                      <td className="px-10 py-7 text-xs font-bold text-slate-600">{row.mitigation}</td>

                      <td className="px-10 py-7 text-right">
                        <div className="flex items-center justify-end gap-3">
                          <span className="text-xs font-black text-slate-900">
                            {Math.round(row.confidence * 100)}%
                          </span>
                          <div className="w-16">
                            <Progress value={row.confidence * 100} size="xs" color="indigo" />
                          </div>
                          <ChevronRight
                            size={16}
                            className="text-slate-300 group-hover:text-indigo-600 transition-colors"
                          />
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </Table>
            </div>
          </Card>
        </main>
      </div>

      {/* Toast / Notice */}
      {notice.open && (
        <div className="fixed bottom-6 right-6 z-[60] w-[360px] max-w-[calc(100vw-3rem)]">
          <div
            className={`rounded-[24px] border bg-white p-4 shadow-2xl ${
              notice.type === "error"
                ? "border-red-200"
                : notice.type === "success"
                ? "border-emerald-200"
                : "border-indigo-200"
            }`}
            role="status"
            aria-live="polite"
          >
            <div className="flex items-start justify-between gap-3">
              <div className="flex items-start gap-3">
                <div
                  className={`mt-0.5 h-10 w-10 shrink-0 rounded-2xl flex items-center justify-center shadow-inner ${
                    notice.type === "error"
                      ? "bg-red-50 text-red-600"
                      : notice.type === "success"
                      ? "bg-emerald-50 text-emerald-600"
                      : "bg-indigo-50 text-indigo-700"
                  }`}
                >
                  {notice.type === "error" ? (
                    <AlertCircle size={18} />
                  ) : notice.type === "success" ? (
                    <ShieldCheck size={18} />
                  ) : (
                    <Zap size={18} />
                  )}
                </div>

                <div>
                  <p className="text-xs font-black tracking-widest uppercase text-slate-500">{notice.title}</p>
                  <p className="mt-1 text-[12px] font-semibold text-slate-600 leading-relaxed">
                    {notice.message}
                  </p>

                  {activeJobId &&
                    activeJobStatus &&
                    !["DONE", "FAILED"].includes(activeJobStatus) && (
                      <div className="mt-3">
                        <div className="flex items-center justify-between text-[10px] font-black text-slate-400 uppercase tracking-widest">
                          <span>Job</span>
                          <span className="text-slate-500">{activeJobStatus}</span>
                        </div>
                        <div className="mt-2 h-1.5 w-full rounded-full bg-slate-100 overflow-hidden">
                          <div className="h-full w-1/3 rounded-full bg-indigo-600 animate-pulse" />
                        </div>
                      </div>
                    )}
                </div>
              </div>

              <button
                onClick={() => setNotice((n) => ({ ...n, open: false }))}
                className="text-[10px] font-black uppercase tracking-widest text-slate-400 hover:text-slate-600"
                aria-label="Dismiss notification"
              >
                Dismiss
              </button>
            </div>
          </div>
        </div>
      )}

      {modalOpen && (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center px-4 py-6 bg-black/40"
          onClick={() => setModalOpen(false)}
        >
          <div
            className="w-full max-w-3xl rounded-[32px] bg-white p-8 shadow-2xl"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="flex items-start justify-between gap-6">
              <div>
                <p className="text-[10px] font-black tracking-widest text-indigo-600 uppercase">
                  Upload PO, ASN & Invoice
                </p>
                <h3 className="text-2xl font-black text-slate-900">Bring your EDI batches together</h3>
                <p className="text-sm text-slate-400 mt-2">
                  Drop PO, ASN, and invoice files (CSV / EDI / TXT) to let the AI engine reconcile them.
                </p>
              </div>

              <button
                onClick={() => setModalOpen(false)}
                className="text-xs font-black tracking-widest uppercase text-slate-400 hover:text-slate-600 transition-colors"
              >
                Close
              </button>
            </div>

            <div
              onClick={(e) => {
                e.stopPropagation();
                fileInputRef.current?.click();
              }}
              onDragOver={(e) => e.preventDefault()}
              onDrop={handleDrop}
              className="mt-6 cursor-pointer rounded-3xl border-2 border-dashed border-slate-200 bg-slate-50/70 px-10 py-14 text-center text-slate-500 transition hover:border-indigo-500"
            >
              <Upload size={40} className="mx-auto text-indigo-400" />
              <p className="mt-3 text-sm font-black text-slate-700">Click or drag files here</p>
              <p className="text-xs text-slate-400">We support the EDI PO / ASN / Invoice exports.</p>
            </div>

            <div className="mt-6">
              {pendingFiles.length ? (
                <div className="space-y-3">
                  {pendingFiles.map((file, idx) => (
                    <div
                      key={`${file.name}-${idx}`}
                      className="flex items-center justify-between gap-4 rounded-2xl border border-slate-100 bg-white px-4 py-3 text-sm font-black text-slate-700 shadow-sm"
                    >
                      <div>
                        <p className="text-xs uppercase tracking-widest text-slate-400">
                          {classifyFile(file.name) || "pending"}
                        </p>
                        <p className="text-sm font-black text-slate-900">{file.name}</p>
                      </div>

                      <div className="flex items-center gap-3">
                        <span className="text-[11px] font-black text-slate-400">
                          {(file.size / 1024).toFixed(1)} KB
                        </span>
                        <button
                          className="text-xs uppercase text-red-500 hover:underline"
                          onClick={() => {
                            setPendingFiles((prev) => prev.filter((_, i) => i !== idx));
                            setAttachedFiles((prev) => {
                              const matchIndex = prev.findIndex(
                                (af) => af.name === file.name && af.lastModified === file.lastModified
                              );
                              if (matchIndex === -1) return prev;
                              return [...prev.slice(0, matchIndex), ...prev.slice(matchIndex + 1)];
                            });
                          }}
                        >
                          Remove
                        </button>
                      </div>
                    </div>
                  ))}
                </div>
              ) : (
                <p className="text-xs font-semibold uppercase tracking-widest text-slate-400">
                  No files attached yet. Select at least one document.
                </p>
              )}

              <div className="mt-3 flex items-center justify-between gap-3">
                <button
                  className="flex-1 rounded-2xl border border-slate-200 bg-white px-4 py-2 text-xs font-black uppercase tracking-widest text-slate-700 hover:border-slate-300"
                  onClick={() => {
                    setPendingFiles([]);
                    setAttachedFiles([]);
                    setUploadStatus({ state: "idle" });
                    setActiveJobId(null);
                    setActiveJobStatus(null);
                    setNotice((n) => ({ ...n, open: false }));
                    lastTerminalJobRef.current = null;
                  }}
                >
                  Clear files
                </button>

                <button
                  className="flex-1 rounded-2xl bg-indigo-600 px-4 py-2 text-xs font-black uppercase tracking-widest text-white shadow-xl shadow-indigo-100 hover:bg-indigo-700 disabled:opacity-70"
                  disabled={
                    uploadStatus.state === "uploading" ||
                    pendingFiles.filter((file) => classifyFile(file.name) === "po").length !== 1 ||
                    pendingFiles.filter((file) => classifyFile(file.name) === "asn").length === 0 ||
                    pendingFiles.filter((file) => classifyFile(file.name) === "invoice").length === 0
                  }
                  onClick={handleConfirmUploads}
                >
                  {uploadStatus.state === "uploading" ? "Processing..." : "Confirm upload"}
                </button>
              </div>

              {(uploadStatus.message || uploadStatus.state === "success") && (
                <p
                  className={`mt-3 text-[12px] ${
                    uploadStatus.state === "error" ? "text-red-600" : "text-emerald-600"
                  }`}
                >
                  {uploadStatus.message}
                </p>
              )}

              {!uploadStatus.message && uploadStatus.state !== "uploading" && (
                <p className="mt-3 text-[12px] text-slate-500">
                  {pendingFiles.length === 0
                    ? "At least 1 PO, 1 ASN, and 1 invoice required."
                    : pendingFiles.filter((file) => classifyFile(file.name) === "po").length !== 1
                    ? "Please attach exactly 1 PO document."
                    : pendingFiles.filter((file) => classifyFile(file.name) === "asn").length === 0
                    ? "Add at least 1 ASN document."
                    : pendingFiles.filter((file) => classifyFile(file.name) === "invoice").length === 0
                    ? "Add at least 1 invoice document."
                    : ""}
                </p>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

// -----------------------------
// HELPER COMPONENTS
// -----------------------------

function SidebarItem({ icon, label, active = false }: any) {
  return (
    <button
      className={`flex w-full items-center gap-4 rounded-2xl px-5 py-3.5 text-sm font-black transition-all duration-200 ${
        active
          ? "bg-indigo-600 text-white shadow-xl shadow-indigo-100"
          : "text-slate-400 hover:bg-indigo-50 hover:text-indigo-600"
      }`}
    >
      <div className={`${active ? "text-white" : "text-slate-400"}`}>{icon}</div>
      <span className="tracking-tight">{label}</span>
    </button>
  );
}

function IntelligenceCard({ title, value, trend, sub, icon, color }: any) {
  const colors = {
    red: "bg-red-50 text-red-600 shadow-red-50",
    blue: "bg-blue-50 text-blue-600 shadow-blue-50",
    green: "bg-green-50 text-green-600 shadow-green-50",
    indigo: "bg-indigo-50 text-indigo-600 shadow-indigo-50",
  };

  return (
    <Card className="border-none shadow-sm rounded-[32px] bg-white p-7 transition-all hover:-translate-y-1">
      <div className="flex flex-col">
        <div
          className={`h-14 w-14 rounded-3xl flex items-center justify-center mb-6 shadow-xl ${
            colors[color as keyof typeof colors]
          }`}
        >
          {React.cloneElement(icon as React.ReactElement, { size: 24 })}
        </div>
        <p className="text-[10px] font-black text-slate-400 uppercase tracking-widest">{title}</p>
        <div className="flex items-baseline gap-3 mt-1.5">
          <h2 className="text-3xl font-black text-slate-900 tracking-tighter">{value}</h2>
          <span
            className={`text-xs font-black ${String(trend).startsWith("+") ? "text-green-500" : "text-red-500"}`}
          >
            {trend}
          </span>
        </div>
        <p className="text-[11px] font-bold text-slate-400 mt-1">{sub}</p>
      </div>
    </Card>
  );
}

function StatProgress({ label, value, color }: any) {
  const barColors: { [key: string]: string } = {
    indigo: "bg-indigo-600 shadow-indigo-100",
    green: "bg-green-500 shadow-green-100",
    red: "bg-red-500 shadow-red-100",
    blue: "bg-blue-600 shadow-blue-100",
  };

  return (
    <div className="group transition-all">
      <div className="flex justify-between items-center mb-2.5">
        <span className="text-[11px] font-black text-slate-500 uppercase tracking-wider group-hover:text-indigo-600 transition-colors">
          {label}
        </span>
        <span className="text-xs font-black text-slate-900 bg-slate-50 px-2 py-0.5 rounded-md">{value}%</span>
      </div>
      <div className="h-1.5 w-full bg-slate-100 rounded-full overflow-hidden shadow-inner">
        <div
          className={`h-full rounded-full transition-all duration-700 ease-out shadow-sm ${
            barColors[color] || barColors.indigo
          }`}
          style={{ width: `${value}%` }}
        />
      </div>
    </div>
  );
}