import { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import api from "../api/api";

export default function Navbar() {
  const navigate = useNavigate();
  const [displayName, setDisplayName] = useState("Admin PLN");

  useEffect(() => {
    let cancelled = false;

    api
      .get("/auth/me")
      .then((response) => {
        if (!cancelled) {
          setDisplayName(response.data.full_name || response.data.username);
        }
      })
      .catch(() => {
        // Login guard already handles a missing/invalid session; this is
        // just the display name, so a failure here is not fatal.
      });

    return () => {
      cancelled = true;
    };
  }, []);

  function handleLogout() {
    localStorage.removeItem("access_token");
    navigate("/login", { replace: true });
  }

  return (
    <header
      style={{
        height: 70,
        display: "flex",
        justifyContent: "space-between",
        alignItems: "center",
        padding: "0 24px",
        borderBottom: "1px solid #1d293d",
      }}
    >
      <h3>Executive Dashboard</h3>

      <div style={{ display: "flex", alignItems: "center", gap: 16 }}>
        <span>{displayName}</span>
        <button
          onClick={handleLogout}
          style={{
            background: "transparent",
            border: "1px solid var(--border)",
            color: "var(--text-soft)",
            borderRadius: 6,
            padding: "6px 12px",
            cursor: "pointer",
            fontSize: 13,
          }}
        >
          Keluar
        </button>
      </div>
    </header>
  );
}
