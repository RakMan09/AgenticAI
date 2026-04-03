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
        <header className="app-header">
          <nav className="container app-nav">
            <Link href="/" className="brand">
              Agent Viz
            </Link>
            <Link href="/" className="nav-link">Runs</Link>
            <Link href="/compare" className="nav-link">Compare</Link>
            <Link href="/analytics" className="nav-link">Analytics</Link>
            <Link href="/case-studies" className="nav-link">Case Studies</Link>
          </nav>
        </header>
        {children}
      </body>
    </html>
  );
}
