import { useCallback, useEffect, useRef, useState } from "react";
import UploadDropzone from "../components/upload/UploadDropzone";
import UploadFileTable from "../components/upload/UploadFileTable";
import { uploadBatchFiles, type BatchUploadResponse } from "../api/upload_batch";
import { getJobHistory, getJobStatus, startETL, retryETL, syncGoogleDrive, retryDriveSync, type JobHistory } from "../api/system";

const POLL_INTERVAL = 3000;
type JobMap = Record<string, JobHistory>;

type UploadedFileRecord = {
    key: string; jobId: string; filename: string; originalFilename: string; size: number;
    dataset: string; month: string; status: string; progress: number; currentStep: string;
    uploadedAt: string; startedAt: string; finishedAt: string; storage: string; error: string;
};

const asString = (value: unknown) => value == null ? "" : String(value);
const asNumber = (value: unknown, fallback = 0) => { const n = Number(value); return Number.isFinite(n) ? n : fallback; };
const statusOf = (job?: JobHistory) => String(job?.status || "UPLOADED").toUpperCase();
const isTerminal = (job?: JobHistory) => ["FINISHED", "FAILED", "ERROR"].includes(statusOf(job));
const isPollingComplete = (job?: JobHistory) => isTerminal(job) || (statusOf(job) === "UPLOADED" && Number(job?.progress ?? 0) >= 100 && String(job?.current_step ?? "").toUpperCase().includes("READY FOR ETL"));
// A Drive sync chunk pausing on its time budget ("PAUSED (TIME BUDGET)") is
// healthy and expected -- only a real backend exception sets last_error, or
// flips status to FAILED.
const isDriveSyncFailed = (job?: JobHistory) => statusOf(job) === "FAILED" || Boolean((job as any)?.last_error);
const sleep = (ms: number) => new Promise<void>((resolve) => window.setTimeout(resolve, ms));
function formatBytes(value: number) { if (!value || value < 0) return "-"; if (value < 1024) return `${value} B`; if (value < 1048576) return `${(value / 1024).toFixed(1)} KB`; if (value < 1073741824) return `${(value / 1048576).toFixed(1)} MB`; return `${(value / 1073741824).toFixed(2)} GB`; }
function formatDate(value: string) { if (!value) return "-"; const d = new Date(value); return Number.isNaN(d.getTime()) ? value : d.toLocaleString("id-ID"); }
function flattenHistory(history: JobHistory[]): UploadedFileRecord[] {
    const records: UploadedFileRecord[] = [];
    history.forEach((job, jobIndex) => {
        const jobId = asString(job.job_id).trim() || `job-${jobIndex}`;
        const status = statusOf(job); const progress = Math.min(Math.max(asNumber(job.progress), 0), 100);
        const files = Array.isArray(job.files) ? job.files : [];
        if (!files.length) {
            const filename = asString(job.filename) || asString(job.original_filename) || "Nama file tidak tersedia";
            records.push({ key: `${jobId}-0`, jobId, filename, originalFilename: filename, size: asNumber(job.size), dataset: asString(job.dataset), month: asString(job.month), status, progress, currentStep: asString(job.current_step), uploadedAt: asString(job.uploaded_at || job.created_at), startedAt: asString(job.started_at), finishedAt: asString(job.finished_at), storage: asString(job.storage), error: asString(job.error || job.last_error) });
            return;
        }
        files.forEach((raw, index) => {
            const item = raw && typeof raw === "object" ? raw as Record<string, unknown> : {};
            const filename = asString(item.original_filename) || asString(item.filename) || asString(job.filename) || `File ${index + 1}`;
            records.push({ key: `${jobId}-${index}-${filename}`, jobId, filename: asString(item.filename) || filename, originalFilename: filename, size: asNumber(item.size), dataset: asString(item.dataset), month: asString(item.month), status, progress, currentStep: asString(job.current_step), uploadedAt: asString(job.uploaded_at || job.created_at), startedAt: asString(job.started_at), finishedAt: asString(job.finished_at), storage: asString(item.storage || job.storage), error: asString(item.error || job.error || job.last_error) });
        });
    });
    return records;
}

export default function UploadPage() {
    const [files, setFiles] = useState<File[]>([]);
    const [uploadResult, setUploadResult] = useState<BatchUploadResponse | null>(null);
    const [jobs, setJobs] = useState<JobMap>({});
    const [history, setHistory] = useState<JobHistory[]>([]);
    const [historyLoading, setHistoryLoading] = useState(true);
    const [historyError, setHistoryError] = useState("");
    const [loading, setLoading] = useState(false);
    const [syncingDrive, setSyncingDrive] = useState(false);
    const [retrying, setRetrying] = useState<Record<string, boolean>>({});
    const pollingRef = useRef(false);

    const refreshHistory = useCallback(async () => {
        try {
            setHistoryError(""); const durable = await getJobHistory(); setHistory(durable);
            setJobs(prev => { const merged = { ...prev }; durable.forEach(job => { const id = asString(job.job_id); if (id) merged[id] = job; }); return merged; }); return durable;
        } catch (error: any) { console.error(error); setHistoryError(error?.message || "Riwayat file tidak dapat dimuat."); return []; }
        finally { setHistoryLoading(false); }
    }, []);

    useEffect(() => { void refreshHistory(); return () => { pollingRef.current = false; }; }, [refreshHistory]);

    async function pollJobs(jobIds: string[]) {
        const pending = new Set(jobIds); const latest: JobMap = {};
        while (pollingRef.current && pending.size) {
            const ids = Array.from(pending);
            const responses = await Promise.allSettled(ids.map(async jobId => ({ jobId, value: await getJobStatus(jobId) })));
            responses.forEach((response, index) => {
                const jobId = ids[index];
                if (response.status === "fulfilled") { latest[jobId] = response.value.value; if (isPollingComplete(response.value.value)) pending.delete(jobId); }
                else if (Number((response.reason as any)?.response?.status || 0) === 404) { latest[jobId] = { job_id: jobId, status: "FAILED", progress: 100, current_step: "JOB TIDAK DITEMUKAN", error: "Job tidak ditemukan pada backend." }; pending.delete(jobId); }
            });
            setJobs(prev => ({ ...prev, ...latest }));
            setHistory(prev => { const map = new Map(prev.map(job => [asString(job.job_id), job])); Object.values(latest).forEach(job => { const id = asString(job.job_id); if (id) map.set(id, job); }); return Array.from(map.values()); });
            if (pending.size) await sleep(POLL_INTERVAL);
        }
        return latest;
    }

    async function handleUpload() {
        if (!files.length) { alert("Pilih file terlebih dahulu."); return; }
        pollingRef.current = true; setLoading(true); setUploadResult(null); setJobs({});
        try {
            const result = await uploadBatchFiles(files); setUploadResult(result);
            const ids = result.jobs.map(item => String(item.job_id || "").trim()).filter(Boolean);
            if (!ids.length) throw new Error("Tidak ada job yang berhasil dibuat.");
            const ready: JobMap = {};
            ids.forEach(jobId => { ready[jobId] = { job_id: jobId, status: "UPLOADED", progress: 0, current_step: "READY FOR ETL" }; });
            setJobs(ready);
            await refreshHistory();
        } catch (error: any) { console.error(error); alert(error?.message || "Upload atau proses ETL gagal."); await refreshHistory(); }
        finally { pollingRef.current = false; setLoading(false); }
    }

    async function handleSyncDrive() {
        if (loading || syncingDrive) return;
        setSyncingDrive(true);
        pollingRef.current = true;
        try {
            const result = await syncGoogleDrive();
            const jobId = String(result.job_id || "").trim();
            if (jobId) {
                setJobs(prev => ({ ...prev, [jobId]: { job_id: jobId, status: "UPLOADED", progress: 0, current_step: "GOOGLE DRIVE DOWNLOADING" } }));
                // The backend only performs one bounded chunk of work per HTTP
                // call (FastAPI Cloud's free tier does not keep a detached
                // background task alive between requests), so keep calling
                // retry here until the job is actually done or genuinely fails.
                let status = await getJobStatus(jobId);
                setJobs(prev => ({ ...prev, [jobId]: status }));
                while (pollingRef.current && !isPollingComplete(status) && !isDriveSyncFailed(status)) {
                    await retryDriveSync(jobId);
                    status = await getJobStatus(jobId);
                    setJobs(prev => ({ ...prev, [jobId]: status }));
                    setHistory(prev => { const map = new Map(prev.map(job => [asString(job.job_id), job])); map.set(jobId, status); return Array.from(map.values()); });
                }
            }
            await refreshHistory();
        } catch (error: any) {
            alert(error?.response?.data?.detail || error?.message || "Google Drive sync gagal dimulai.");
        } finally {
            pollingRef.current = false;
            setSyncingDrive(false);
        }
    }

    async function handleStartETL(jobId: string) {
        if (loading || retrying[jobId]) return;
        setRetrying(prev => ({ ...prev, [jobId]: true }));
        try {
            await startETL(jobId);
            const queued: JobHistory = { job_id: jobId, status: "DETECTING", progress: 1, current_step: "ETL STARTING" };
            setJobs(prev => ({ ...prev, [jobId]: { ...(prev[jobId] || {}), ...queued } }));
            pollingRef.current = true;
            await pollJobs([jobId]);
            await refreshHistory();
        } catch (error: any) {
            alert(error?.response?.data?.detail || error?.message || "ETL gagal dimulai.");
        } finally {
            setRetrying(prev => ({ ...prev, [jobId]: false }));
            pollingRef.current = false;
        }
    }

    async function handleRetry(jobId: string) {
        if (retrying[jobId]) return;
        setRetrying(prev => ({ ...prev, [jobId]: true }));
        try {
            const result = await retryETL(jobId);
            const effectiveJobId = asString((result as any)?.job_id).trim() || jobId;
            const refreshed = await getJobStatus(effectiveJobId);
            setJobs(prev => ({ ...prev, [effectiveJobId]: refreshed }));
            pollingRef.current = true;
            await pollJobs([effectiveJobId]);
            await refreshHistory();
        } catch (error: any) { alert(error?.response?.data?.detail || error?.message || "Retry ETL gagal dijadwalkan."); }
        finally { setRetrying(prev => ({ ...prev, [jobId]: false })); pollingRef.current = false; }
    }

    const jobList = Object.entries(jobs);
    const totalJobs = uploadResult?.jobs.length || 0;
    const finishedJobs = jobList.filter(([, job]) => statusOf(job) === "FINISHED").length;
    const failedJobs = jobList.filter(([, job]) => ["FAILED", "ERROR"].includes(statusOf(job))).length;
    const averageProgress = totalJobs ? Math.round(uploadResult!.jobs.reduce((sum, item) => sum + Math.min(Math.max(Number(jobs[String(item.job_id)]?.progress ?? 0), 0), 100), 0) / totalJobs) : 0;
    const uploadedFiles = flattenHistory(history);
    const failedHistory = uploadedFiles.filter(file => ["FAILED", "ERROR"].includes(file.status));

    return <div>
        <h1>Upload Center</h1>
        <UploadDropzone files={files} setFiles={setFiles} />
        <UploadFileTable files={files} />
        <div style={{ display: "flex", gap: 12, flexWrap: "wrap", marginTop: 20 }}>
            <button onClick={handleUpload} disabled={loading || syncingDrive} style={{ padding: "12px 24px", cursor: loading ? "not-allowed" : "pointer" }}>{loading ? "Uploading..." : "Upload Files"}</button>
            <button type="button" onClick={() => void handleSyncDrive()} disabled={loading || syncingDrive} style={{ padding: "12px 24px", cursor: syncingDrive ? "not-allowed" : "pointer" }}>{syncingDrive ? "Syncing Google Drive..." : "☁ Sync Google Drive"}</button>
        </div>
        <div style={{ marginTop: 8, fontSize: 13, opacity: 0.8 }}>Sync Drive hanya mengunduh file. ETL tidak berjalan sampai tombol Start ETL ditekan.</div>
        {uploadResult && <div style={{ marginTop: 20, padding: 20, border: "1px solid #2d4f70", borderRadius: 10, background: "#111827" }}>
            <h3>Batch Status</h3><p>Upload: <strong>{uploadResult.uploaded_files}/{uploadResult.total_files}</strong></p><p>ETL: <strong>{finishedJobs}/{totalJobs} selesai</strong>{failedJobs ? ` • ${failedJobs} gagal` : " • belum berjalan sampai tombol Start ETL ditekan"}</p>
            <div style={{ width: "100%", height: 10, background: "#374151", borderRadius: 999, overflow: "hidden" }}><div style={{ width: `${averageProgress}%`, height: "100%", background: "#22c55e" }} /></div><div style={{ marginTop: 8 }}>{averageProgress}% overall</div>
            {uploadResult.failures.length > 0 && <div style={{ marginTop: 16 }}><strong>File gagal upload:</strong>{uploadResult.failures.map(f => <div key={f.filename}>{String(f.filename)}: {String(f.error)}</div>)}</div>}
            {uploadResult.jobs.map(item => {
                const jobId = String(item.job_id || "").trim();
                return jobId ? <button key={jobId} type="button" onClick={() => void handleStartETL(jobId)} disabled={loading || !!retrying[jobId] || statusOf(jobs[jobId]) === "FINISHED"} style={{ marginTop: 14, marginRight: 8, padding: "10px 16px" }}>
                    {retrying[jobId] ? "Starting ETL..." : "▶ Start ETL"}
                </button> : null;
            })}
        </div>}
        <div style={{ marginTop: 20, padding: 20, border: "1px solid #2d4f70", borderRadius: 10, background: "#111827" }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: 12, flexWrap: "wrap" }}><div><h3 style={{ marginBottom: 4 }}>Semua File Terupload</h3><div>{historyLoading ? "Memuat daftar file..." : `${uploadedFiles.length} file terdeteksi dari durable storage`}</div></div><button type="button" onClick={() => void refreshHistory()} disabled={historyLoading} style={{ padding: "8px 14px" }}>Refresh Daftar</button></div>
            {historyError && <div style={{ marginTop: 12, color: "#f59e0b" }}>{historyError}</div>}
            {uploadedFiles.length > 0 && <div style={{ overflowX: "auto", marginTop: 16 }}><table style={{ width: "100%", borderCollapse: "collapse", minWidth: 1100 }}><thead><tr>{["No.","Nama File","Ukuran","Dataset","Bulan","Status","Progress","Upload","Job ID","Aksi"].map(label => <th key={label} style={{ textAlign: "left", padding: "10px 8px", borderBottom: "1px solid #263244" }}>{label}</th>)}</tr></thead><tbody>{uploadedFiles.map((file,index) => { const status=file.status; const progress=Math.min(Math.max(file.progress,0),100); const color=status==="FAILED"||status==="ERROR"?"#ef4444":status==="FINISHED"?"#22c55e":"#f59e0b"; const errorText=String(file.error||""); return <tr key={file.key}>
                <td style={{padding:"10px 8px",borderBottom:"1px solid #1f2937"}}>{index+1}</td><td style={{padding:"10px 8px",borderBottom:"1px solid #1f2937"}}><strong>{String(file.originalFilename)}</strong></td><td style={{padding:"10px 8px",borderBottom:"1px solid #1f2937"}}>{formatBytes(file.size)}</td><td style={{padding:"10px 8px",borderBottom:"1px solid #1f2937"}}>{String(file.dataset||"-")}</td><td style={{padding:"10px 8px",borderBottom:"1px solid #1f2937"}}>{String(file.month||"-")}</td><td style={{padding:"10px 8px",borderBottom:"1px solid #1f2937"}}><span style={{color,fontWeight:700}}>{String(status)}</span>{errorText&&<div style={{marginTop:4,color:"#fca5a5",fontSize:12}}>{errorText}</div>}</td><td style={{padding:"10px 8px",borderBottom:"1px solid #1f2937",minWidth:140}}><div style={{fontSize:12,marginBottom:4}}>{progress}%</div><div style={{width:"100%",height:8,background:"#374151",borderRadius:999,overflow:"hidden"}}><div style={{width:`${progress}%`,height:"100%",background:color}}/></div></td><td style={{padding:"10px 8px",borderBottom:"1px solid #1f2937"}}>{formatDate(file.uploadedAt)}</td><td style={{padding:"10px 8px",borderBottom:"1px solid #1f2937",whiteSpace:"nowrap"}}>{String(file.jobId)}</td><td style={{padding:"10px 8px",borderBottom:"1px solid #1f2937"}}>{["FAILED","ERROR"].includes(status) ? <button type="button" onClick={() => void handleRetry(file.jobId)} disabled={!!retrying[file.jobId]} style={{padding:"7px 12px",whiteSpace:"nowrap"}}>{retrying[file.jobId] ? "Retrying..." : file.storage === "google_drive" ? "🔄 Sync Ulang Drive" : "🔄 Retry ETL"}</button> : status === "RETRYING" || status === "PROCESSING" ? <span style={{fontSize:12}}>Sedang diproses...</span> : status === "UPLOADED" || status === "READY_FOR_ETL" ? <button type="button" onClick={() => void handleStartETL(file.jobId)} disabled={!!retrying[file.jobId]} style={{padding:"7px 12px",whiteSpace:"nowrap"}}>{retrying[file.jobId] ? "Starting..." : "▶ Start ETL"}</button> : status === "FINISHED" ? <span style={{color:"#22c55e",fontSize:12}}>✓ Selesai</span> : <span style={{fontSize:12}}>Menunggu...</span>}</td>
            </tr>; })}</tbody></table></div>}
            {failedHistory.length > 0 && <div style={{ marginTop: 14, color: "#fca5a5" }}>Ada {failedHistory.length} file gagal. Perbaiki ETL lalu klik <strong>Retry ETL</strong> pada file tersebut.</div>}
        </div>
        {jobList.length > 0 && <div style={{ marginTop:20,padding:20,border:"1px solid #2d4f70",borderRadius:10,background:"#111827" }}><h3>ETL Per File</h3>{jobList.map(([jobId,job])=>{const progress=Math.min(Math.max(Number(job.progress??0),0),100);const status=statusOf(job);const filename=asString(job.filename)||asString(job.original_filename)||jobId;const currentStep=asString(job.current_step);const errorText=asString(job.error);const retryingJob=!!retrying[jobId];return <div key={jobId} style={{marginBottom:16,paddingBottom:12,borderBottom:"1px solid #263244"}}><strong>{filename}</strong><div style={{marginTop:4}}>Status: <strong>{status}</strong>{currentStep ? ` • ${currentStep}` : ""}</div><div style={{marginTop:8,width:"100%",height:8,background:"#374151",borderRadius:999,overflow:"hidden"}}><div style={{width:`${progress}%`,height:"100%",background:status==="FAILED"||status==="ERROR"?"#ef4444":"#22c55e"}}/></div><div style={{marginTop:4}}>{progress}% • Job {String(jobId)}</div>{errorText && <div style={{marginTop:4}}>Error: {errorText}</div>}{(status === "UPLOADED" || status === "READY_FOR_ETL") && <button type="button" onClick={()=>void handleStartETL(jobId)} disabled={retryingJob} style={{marginTop:8,padding:"7px 12px"}}>{retryingJob ? "Starting..." : "▶ Start ETL"}</button>}{["FAILED","ERROR"].includes(status)&&<button type="button" onClick={()=>void handleRetry(jobId)} disabled={retryingJob} style={{marginTop:8,padding:"7px 12px"}}>{retryingJob ? "Starting..." : String(job.storage || "").toLowerCase() === "google_drive" ? "🔄 Sync Ulang Drive" : "▶ Start ETL Lagi"}</button>}</div>;})}</div>}
    </div>;
}