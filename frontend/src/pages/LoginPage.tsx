import { useState, type FormEvent, type CSSProperties } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import api from "../api/api";

interface LocationState {
    from?: { pathname: string };
}

export default function LoginPage() {
    const navigate = useNavigate();
    const location = useLocation();

    const [username, setUsername] = useState("");
    const [password, setPassword] = useState("");
    const [error, setError] = useState<string | null>(null);
    const [loading, setLoading] = useState(false);

    async function handleSubmit(event: FormEvent) {
        event.preventDefault();
        setError(null);
        setLoading(true);

        try {
            const body = new URLSearchParams();
            body.set("username", username);
            body.set("password", password);

            const response = await api.post("/auth/login", body, {
                headers: { "Content-Type": "application/x-www-form-urlencoded" },
            });

            localStorage.setItem("access_token", response.data.access_token);

            const state = location.state as LocationState | null;
            const redirectTo = state?.from?.pathname || "/";
            navigate(redirectTo, { replace: true });
        } catch (err: any) {
            if (err?.response?.status === 401) {
                setError("Username atau password salah.");
            } else {
                setError("Gagal terhubung ke server. Coba lagi.");
            }
        } finally {
            setLoading(false);
        }
    }

    return (
        <div
            style={{
                minHeight: "100vh",
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                background: "var(--bg)",
            }}
        >
            <form
                onSubmit={handleSubmit}
                style={{
                    width: 340,
                    background: "var(--surface)",
                    border: "1px solid var(--border)",
                    borderRadius: 12,
                    padding: 32,
                    display: "flex",
                    flexDirection: "column",
                    gap: 16,
                }}
            >
                <div>
                    <h2 style={{ margin: 0, color: "var(--text)" }}>PLN Analytics</h2>
                    <p style={{ margin: "4px 0 0", color: "var(--text-soft)", fontSize: 13 }}>
                        Masuk untuk mengakses dashboard
                    </p>
                </div>

                <label style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                    <span style={{ fontSize: 13, color: "var(--text-soft)" }}>Username</span>
                    <input
                        value={username}
                        onChange={(e) => setUsername(e.target.value)}
                        autoFocus
                        required
                        style={inputStyle}
                    />
                </label>

                <label style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                    <span style={{ fontSize: 13, color: "var(--text-soft)" }}>Password</span>
                    <input
                        type="password"
                        value={password}
                        onChange={(e) => setPassword(e.target.value)}
                        required
                        style={inputStyle}
                    />
                </label>

                {error && (
                    <div style={{ color: "#ff6b6b", fontSize: 13 }}>{error}</div>
                )}

                <button
                    type="submit"
                    disabled={loading}
                    style={{
                        marginTop: 8,
                        padding: "10px 16px",
                        borderRadius: 8,
                        border: "none",
                        background: "var(--primary)",
                        color: "#fff",
                        fontWeight: 600,
                        cursor: loading ? "default" : "pointer",
                        opacity: loading ? 0.7 : 1,
                    }}
                >
                    {loading ? "Memproses..." : "Masuk"}
                </button>
            </form>
        </div>
    );
}

const inputStyle: CSSProperties = {
    padding: "10px 12px",
    borderRadius: 8,
    border: "1px solid var(--border)",
    background: "var(--bg-soft)",
    color: "var(--text)",
    fontSize: 14,
    outline: "none",
};
