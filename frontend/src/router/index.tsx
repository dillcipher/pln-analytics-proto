import { createBrowserRouter } from "react-router-dom";

import MainLayout from "../layouts/MainLayout";
import RequireAuth from "../components/RequireAuth";

import LoginPage from "../pages/LoginPage";
import ExecutivePage from "../pages/ExecutivePage";
import UploadPage from "../pages/UploadPage";
import DlpdPage from "../pages/DlpdPage";
import SuspectPage from "../pages/SuspectPage";
import DataManagementPage from "../pages/DataManagementPage";
import SettingsPage from "../pages/SettingsPage";
import "../pages/SystemPages.css";


export const router =
    createBrowserRouter([

        // ==================================================
        // LOGIN (outside the auth guard and the main layout)
        // ==================================================

        {
            path: "/login",

            element: (
                <LoginPage />
            ),
        },

        {
            path: "/",

            element: (
                <RequireAuth />
            ),

            children: [
                {
                    element: (
                        <MainLayout />
                    ),

                    children: [

                // ==================================================
                // EXECUTIVE
                // ==================================================

                {
                    index: true,

                    element: (
                        <ExecutivePage />
                    ),
                },

                {
                    path: "executive",

                    element: (
                        <ExecutivePage />
                    ),
                },


                // ==================================================
                // UPLOAD
                // ==================================================

                {
                    path: "upload",

                    element: (
                        <UploadPage />
                    ),
                },


                // ==================================================
                // DLPD
                // ==================================================

                {
                    path: "dlpd",

                    element: (
                        <DlpdPage />
                    ),
                },


                // ==================================================
                // SUSPECT
                // ==================================================

                {
                    path: "suspect",

                    element: (
                        <SuspectPage />
                    ),
                },


                // ==================================================
                // DATA MANAGEMENT
                // ==================================================

                {
                    path: "data-management",

                    element: <DataManagementPage />,
                },


                // ==================================================
                // SETTINGS
                // ==================================================

                {
                    path: "settings",

                    element: <SettingsPage />,
                },

                    ],
                },
            ],
        },
    ]);