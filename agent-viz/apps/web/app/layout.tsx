import "./globals.css";
import type { Metadata } from "next";
import Link from "next/link";

export const metadata: Metadata = {
  title: "Agent Trajectory Visual Analytics",
  description: "Trace sensemaking dashboard",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>
        <div className="page-glow page-glow-left" />
        <div className="page-glow page-glow-right" />
        <header className="app-header">
          <nav className="container app-nav">
            <div className="brand-wrap">
              <Link href="/" className="brand">
                Agent Viz
              </Link>
              <span className="brand-badge">Trajectory Analytics</span>
            </div>
            <div className="nav-cluster">
              <Link href="/" className="nav-link">Runs</Link>
              <Link href="/compare" className="nav-link">Compare</Link>
              <Link href="/analytics" className="nav-link">Analytics</Link>
              <Link href="/case-studies" className="nav-link">Case Studies</Link>
            </div>
          </nav>
        </header>
        <div className="app-shell">{children}</div>
      </body>
    </html>
  );
}
