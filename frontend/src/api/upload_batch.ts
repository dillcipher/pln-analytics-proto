import api from "./api";

export interface BatchUploadFailure {
    filename: string;
    error: string;
}

export interface BatchUploadJob {
    filename: string;
    upload_id?: string;
    job_id: string;
    total_chunks?: number;
    status: string;
}

export interface BatchUploadResponse {
    success: boolean;
    total_files: number;
    uploaded_files: number;
    files: Array<{
        filename: string;
        upload_id: string;
        job_id: string;
        total_chunks: number;
        complete: {
            success: boolean;
            job_id: string;
            status?: string;
        };
    }>;
    jobs: BatchUploadJob[];
    failures: BatchUploadFailure[];
    batch_id?: string;
}

function errorMessage(error: unknown): string {
    const value = error as any;
    const detail = value?.response?.data?.detail;
    if (Array.isArray(detail)) {
        return detail
            .map((item: any) => typeof item === "string" ? item : item?.msg || JSON.stringify(item))
            .join(" | ");
    }
    if (detail !== undefined && detail !== null) {
        return typeof detail === "object" ? JSON.stringify(detail) : String(detail);
    }
    return value?.message ? String(value.message) : "Upload gagal.";
}

/**
 * Browser upload flow:
 *   Browser -> FastAPI -> Google Drive -> local ETL
 *
 * No Supabase Storage chunks are created. Google Drive is the durable raw
 * source, while the backend keeps a local copy for the active ETL job.
 */
export async function uploadBatchFiles(files: File[]): Promise<BatchUploadResponse> {
    if (!files.length) throw new Error("No files selected.");

    const form = new FormData();
    files.forEach((file) => form.append("files", file, file.name));

    try {
        const response = await api.post<{
            success: boolean;
            job_id: string;
            status: string;
            total_files: number;
            files: Array<Record<string, any>>;
            message?: string;
        }>("/upload/files", form, {
            timeout: 30 * 60 * 1000,
            headers: { Accept: "application/json" },
            maxContentLength: Infinity,
            maxBodyLength: Infinity,
        });

        const jobId = String(response.data.job_id || "").trim();
        if (!jobId) throw new Error("Backend tidak mengembalikan Job ID.");

        return {
            success: response.data.success,
            total_files: response.data.total_files || files.length,
            uploaded_files: response.data.files?.length || files.length,
            files: (response.data.files || []).map((item: any) => ({
                filename: String(item.original_filename || item.filename || "file"),
                upload_id: "",
                job_id: jobId,
                total_chunks: 0,
                complete: {
                    success: true,
                    job_id: jobId,
                    status: response.data.status || "READY_FOR_ETL",
                },
            })),
            jobs: [{
                filename: String(response.data.total_files || files.length) + " file(s)",
                job_id: jobId,
                status: response.data.status || "READY_FOR_ETL",
            }],
            failures: [],
            batch_id: jobId,
        };
    } catch (error) {
        throw new Error(errorMessage(error));
    }
}
