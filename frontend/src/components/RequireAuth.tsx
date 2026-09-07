import { useEffect, useState } from "react";
import { Navigate, Outlet, useLocation, useNavigate } from "react-router-dom";

/**
 * Gate on a locally-present access_token. This is a UX guard, not the real
 * security boundary -- the backend rejects unauthenticated requests on its
 * own (see AUTH_REQUIRED in app/core/config.py), so a missing/expired/
 * tampered token here just means real API calls will 401 anyway. This only
 * exists so the app shows a login screen instead of a broken dashboard full
 * of failed requests.
 *
 * Also listens for "pln-auth-expired", dispatched by api.ts whenever any
 * request comes back 401 (expired token, or token from before a server
 * restart rotated JWT_SECRET_KEY) -- without this, an expired session would
 * otherwise just show empty/broken pages instead of returning to login.
 */
export default function RequireAuth() {
    const location = useLocation();
    const navigate = useNavigate();
    const [hasToken, setHasToken] = useState(
        () => !!localStorage.getItem("access_token"),
    );

    useEffect(() => {
        function handleExpired() {
            setHasToken(false);
            navigate("/login", { replace: true, state: { from: location } });
        }
        window.addEventListener("pln-auth-expired", handleExpired);
        return () => window.removeEventListener("pln-auth-expired", handleExpired);
    }, [location, navigate]);

    if (!hasToken) {
        return <Navigate to="/login" replace state={{ from: location }} />;
    }

    return <Outlet />;
}
