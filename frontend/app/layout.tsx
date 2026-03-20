import "./globals.css";
import type { ReactNode } from "react";

export const metadata = {
  title: "PEGASUS EXTRACT",
  description: "AI-Powered Universal Web Data Extraction",
};

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en" className="h-full">
      <body className="min-h-screen bg-pegasus-bg text-slate-100 antialiased">
        {children}
      </body>
    </html>
  );
}

