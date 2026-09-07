import api, { API_ORIGIN } from "./api";

export interface DatasetInfo { name: string; rows: number; size_mb: number; status?: string; [key: string]: unknown; }
export interface DataOverview { total_dataset: number; total_rows: number; total_size_mb: number; datasets: DatasetInfo[]; }
export interface JobHistory { job_id?: string; status?: string; progress?: number; current_step?: string; created_at?: string; uploaded_at?: string; started_at?: string; finished_at?: string; total_files?: number; processed_files?: number; files?: unknown[]; [key: string]: unknown; }
export type ExportFormat = "csv" | "xlsx";
export interface DataManagementExportParams { dataset?: string; month?: string; unitupi?: string; unitap?: string; unitup?: string; tariff?: string; segment?: string; suspect_name?: string; classification?: string; location_code?: string; idpel?: string; columns?: string; }
export interface DlpdExportParams { customer_type?: string; month?: string; unitupi?: string; unitap?: string; unitup?: string; tariff?: string; segment?: string; status?: string; inspection_status?: string; dlpd_repeat?: string; kendala?: string; search?: string; idpel?: string; }
export interface SuspectExportParams { month?: string; unitupi?: string; unitap?: string; classification?: string; tariff?: string; segment?: string; search?: string; idpel?: string; }
export interface SuspectSummaryExportParams { month?: string; unitupi?: string; unitap?: string; classification?: string; tariff?: string; segment?: string; }

function unwrap<T>(value: unknown): T { if (value && typeof value === "object" && !Array.isArray(value)) { const record = value as Record<string, unknown>; if (record.data !== undefined) return record.data as T; if (record.result !== undefined) return record.result as T; } return value as T; }
function cleanParams(params: object): Record<string, unknown> { return Object.fromEntries(Object.entries(params).filter(([, value]) => value !== undefined && value !== null && !(typeof value === "string" && value.trim() === ""))); }
function downloadBlob(blob: Blob, filename: string): void { const url = window.URL.createObjectURL(blob); const anchor = document.createElement("a"); anchor.href = url; anchor.download = filename; document.body.appendChild(anchor); anchor.click(); anchor.remove(); window.URL.revokeObjectURL(url); }
function safeFilename(value: unknown): string { return String(value ?? "").trim().replace(/[^a-zA-Z0-9._-]+/g, "_").replace(/^_+|_+$/g, ""); }
function filenameMonth(month?: string): string { if (!month) return ""; const value = safeFilename(month); return value ? `_${value}` : ""; }

export async function getDataOverview(): Promise<DataOverview> { const response = await api.get("/data-management/overview"); const data = unwrap<Partial<DataOverview>>(response.data) || {}; return { total_dataset: Number(data.total_dataset ?? 0), total_rows: Number(data.total_rows ?? 0), total_size_mb: Number(data.total_size_mb ?? 0), datasets: Array.isArray(data.datasets) ? data.datasets as DatasetInfo[] : [] }; }
export async function getJobHistory(): Promise<JobHistory[]> { const response = await api.get("/history"); const data = unwrap<unknown>(response.data); return Array.isArray(data) ? data as JobHistory[] : []; }
export async function getJobStatus(jobId: string): Promise<JobHistory> { const response = await api.get(`/jobs/${encodeURIComponent(jobId)}`); return unwrap<JobHistory>(response.data); }
// The backend now runs ETL synchronously inside this request (no more
// fire-and-forget background task -- see backend upload.py) so this call
// blocks until ETL actually finishes. Give it a generous ceiling instead of
// the api instance's 120s default; large Drive workbooks can take a while
// on a small/free-tier host.
const ETL_PROCESS_TIMEOUT_MS = Number(import.meta.env.VITE_ETL_PROCESS_TIMEOUT || 1800000);
export async function startETL(jobId: string): Promise<{ success: boolean; job_id: string; status: string; message?: string }> { const response = await api.post(`/upload/process/${encodeURIComponent(jobId)}`, undefined, { timeout: ETL_PROCESS_TIMEOUT_MS }); return unwrap(response.data); }
export async function retryETL(jobId: string): Promise<{ success: boolean; job_id: string; status: string; message?: string }> { const response = await api.post(`/upload/process/${encodeURIComponent(jobId)}`, undefined, { timeout: ETL_PROCESS_TIMEOUT_MS }); return unwrap(response.data); }
// The backend runs one bounded chunk of Drive sync/retry inside this single
// HTTP call, up to DRIVE_SYNC_TIME_BUDGET_SECONDS (150s server-side -- see
// drive.py) before pausing and writing "PAUSED (TIME BUDGET) ... call
// /drive/retry to continue". Both calls used to fall back to the shared
// `api` instance's 120s default timeout, which is BELOW that 150s budget --
// so any chunk that legitimately ran 120-150s (routine with the ~50-750MB
// workbooks this project syncs) got killed client-side with a raw
// "timeout of 120000ms exceeded" alert, which also threw inside
// UploadPage's auto-retry while-loop and aborted the whole sync instead of
// just continuing to the next chunk. Confirmed live 2026-09-04. Give both
// calls real headroom above the server's own budget, same pattern already
// used for ETL_PROCESS_TIMEOUT_MS above.
const DRIVE_SYNC_TIMEOUT_MS = Number(import.meta.env.VITE_DRIVE_SYNC_TIMEOUT || 200000);
export async function syncGoogleDrive(): Promise<{ success: boolean; job_id: string; status: string; message?: string }> { const response = await api.post("/drive/sync", undefined, { timeout: DRIVE_SYNC_TIMEOUT_MS }); return unwrap(response.data); }
export async function retryDriveSync(jobId: string): Promise<{ success: boolean; job_id: string; status: string; message?: string; retry_of?: string }> { const response = await api.post(`/drive/retry/${encodeURIComponent(jobId)}`, undefined, { timeout: DRIVE_SYNC_TIMEOUT_MS }); return unwrap(response.data); }
export async function retryFailedBatch(batchId: string): Promise<{ success: boolean; batch_id: string; status: string; retried: number }> { const response = await api.post(`/upload/batch/retry-failed/${encodeURIComponent(batchId)}`); return unwrap(response.data); }

export async function getSystemHealth() { const response = await fetch(`${API_ORIGIN}/health`, { method: "GET", headers: { Accept: "application/json" }, cache: "no-store" }); if (!response.ok) throw new Error(`Health ${response.status}`); return response.json() as Promise<{ status?: string; application?: string; environment?: string }>; }
export async function refreshWarehouse() { const response = await api.post("/warehouse/refresh"); return response.data; }
export async function downloadDataManagementExport(params: DataManagementExportParams): Promise<Blob> { const response = await api.get("/data-management/export", { params: cleanParams(params), responseType: "blob" }); return response.data as Blob; }
export async function exportDataManagement(params: DataManagementExportParams, filename = "data-management-export"): Promise<void> { const blob = await downloadDataManagementExport(params); downloadBlob(blob, `${safeFilename(filename)}_${safeFilename(params.dataset || "dataset")}${filenameMonth(params.month)}.xlsx`); }
export async function downloadDlpdExport(format: ExportFormat, params: DlpdExportParams): Promise<Blob> { const response = await api.get(`/dlpd/customers/export/${format}`, { params: cleanParams(params), responseType: "blob" }); return response.data as Blob; }
export async function exportDlpd(format: ExportFormat, params: DlpdExportParams): Promise<Blob> { return downloadDlpdExport(format, params); }
export async function downloadAndExportDlpd(format: ExportFormat, params: DlpdExportParams, filename = "dlpd"): Promise<void> { const blob = await downloadDlpdExport(format, params); downloadBlob(blob, `${safeFilename(filename)}_${safeFilename(params.customer_type || "all")}${filenameMonth(params.month)}.${format}`); }
export async function downloadSuspectExport(format: ExportFormat, params: SuspectExportParams): Promise<Blob> { const response = await api.get(`/suspect/export/${format}`, { params: cleanParams(params), responseType: "blob" }); return response.data as Blob; }
export async function exportSuspect(format: ExportFormat, params: SuspectExportParams): Promise<Blob> { return downloadSuspectExport(format, params); }
export async function downloadAndExportSuspect(format: ExportFormat, params: SuspectExportParams, filename = "suspect"): Promise<void> { const blob = await downloadSuspectExport(format, params); downloadBlob(blob, `${safeFilename(filename)}${filenameMonth(params.month)}.${format}`); }
export async function downloadSuspectSummaryExport(format: ExportFormat, params: SuspectSummaryExportParams): Promise<Blob> { const response = await api.get(`/suspect/summary/export/${format}`, { params: cleanParams(params), responseType: "blob" }); return response.data as Blob; }
export async function exportSuspectSummary(format: ExportFormat, params: SuspectSummaryExportParams): Promise<Blob> { return downloadSuspectSummaryExport(format, params); }
export async function downloadAndExportSuspectSummary(format: ExportFormat, params: SuspectSummaryExportParams, filename = "suspect-summary"): Promise<void> { const blob = await downloadSuspectSummaryExport(format, params); downloadBlob(blob, `${safeFilename(filename)}${filenameMonth(params.month)}.${format}`); }
export async function downloadDlpdCsv(params: DlpdExportParams): Promise<void> { await downloadAndExportDlpd("csv", params, "dlpd"); }
export async function downloadDlpdXlsx(params: DlpdExportParams): Promise<void> { await downloadAndExportDlpd("xlsx", params, "dlpd"); }
export async function downloadSuspectCsv(params: SuspectExportParams): Promise<void> { await downloadAndExportSuspect("csv", params, "suspect"); }
export async function downloadSuspectXlsx(params: SuspectExportParams): Promise<void> { await downloadAndExportSuspect("xlsx", params, "suspect"); }
export async function downloadSuspectSummaryCsv(params: SuspectSummaryExportParams): Promise<void> { await downloadAndExportSuspectSummary("csv", params, "suspect-summary"); }
export async function downloadSuspectSummaryXlsx(params: SuspectSummaryExportParams): Promise<void> { await downloadAndExportSuspectSummary("xlsx", params, "suspect-summary"); }