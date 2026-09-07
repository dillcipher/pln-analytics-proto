import api from "./api";

export interface DriveSyncResponse {
    success: boolean;
    job_id: string;
    status: string;
    folder_id: string;
    message: string;
}

export async function syncGoogleDrive(folderId?: string): Promise<DriveSyncResponse> {
    const response = await api.post<DriveSyncResponse>(
        "/drive/sync",
        undefined,
        {
            params: folderId ? { folder_id: folderId } : undefined,
            // The backend now runs one bounded chunk of the sync inside this
            // request (see DRIVE_SYNC_TIME_BUDGET_SECONDS on the backend) so
            // the hosting platform's per-request autoscaling keeps the
            // worker alive for it. Give it real headroom above that budget
            // instead of the old 30s, which was too tight.
            timeout: 60000,
            headers: { Accept: "application/json" },
        },
    );
    return response.data;
}
