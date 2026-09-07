import { Outlet } from "react-router-dom";

import Sidebar from "../components/Sidebar";
import Navbar from "../components/Navbar";

export default function MainLayout() {
  return (
    <div
      style={{
        display: "grid",
        gridTemplateColumns: "260px 1fr",
        height: "100vh",
      }}
    >
      <Sidebar />

      {/*
        minWidth: 0 overrides the CSS Grid default of `min-width: auto` on
        this track. Without it, a grid item's automatic minimum width is
        its content's min-content size -- so any page rendering something
        intrinsically wide (e.g. SuspectPage's detail tables, which use
        `width: max-content` on <table> precisely so all columns render at
        their natural width inside their own overflow-x:auto wrapper) can
        force this whole `1fr` track wider than the viewport, pushing the
        wrapper's own horizontal scrollbar off-screen. Confirmed live
        2026-09-02: combined with `body { overflow-x: hidden }`
        (index.css), the pushed-off content became permanently
        unreachable -- no scrollbar at any level could reach it -- which
        is exactly the "kepotong" (cut off) symptom reported on the
        Suspect Analytics detail table. minWidth: 0 lets this track
        shrink to the viewport as normal, so each page's own
        overflow-x:auto wrapper (not the grid) handles wide content.
      */}
      <main style={{ minWidth: 0 }}>
        <Navbar />

        <div
          style={{
            padding: 24,
          }}
        >
          <Outlet />
        </div>
      </main>
    </div>
  );
}