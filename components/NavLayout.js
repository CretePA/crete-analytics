'use client';
import { useState, useEffect } from 'react';
import Link from 'next/link';
import { usePathname } from 'next/navigation';

const NAV_LINKS = [
  { label: 'Crete Data Ops', url: 'https://crete-hq-7405618890836566.6.azure.databricksapps.com' },
  { label: 'PerformYard', url: 'https://performyard-dashboard-7405618890836566.6.azure.databricksapps.com' },
];

const HomeIcon = () => (
  <svg width="16" height="16" viewBox="0 0 16 16" fill="none"><path d="M2 8l6-6 6 6M3 7v6a1 1 0 001 1h3V10h2v4h3a1 1 0 001-1V7" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"/></svg>
);
const FlashIcon = () => (
  <svg width="16" height="16" viewBox="0 0 16 16" fill="none"><path d="M9 1L3 9h4l-1 6 6-8H8l1-6z" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"/></svg>
);
const OpsIcon = () => (
  <svg width="16" height="16" viewBox="0 0 16 16" fill="none"><path d="M2 4h12M2 8h12M2 12h12" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round"/><circle cx="5" cy="4" r="1.5" fill="currentColor"/><circle cx="11" cy="8" r="1.5" fill="currentColor"/><circle cx="7" cy="12" r="1.5" fill="currentColor"/></svg>
);
const ExternalIcon = () => (
  <svg width="16" height="16" viewBox="0 0 16 16" fill="none"><path d="M6 2H3a1 1 0 00-1 1v3m0 4v3a1 1 0 001 1h3m4 0h3a1 1 0 001-1v-3m0-4V3a1 1 0 00-1-1h-3" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"/></svg>
);

const PAGE_TITLES = {
  '/': 'Crete Analytics',
  '/flash': 'Weekly Flash',
  '/ops': 'Data Ops HQ',
};

export default function NavLayout({ children }) {
  const [navOpen, setNavOpen] = useState(false);
  const [userName, setUserName] = useState('');
  const pathname = usePathname();

  useEffect(() => {
    fetch('/api/whoami').then(r => r.ok ? r.json() : null).then(d => { if (d?.name) setUserName(d.name); }).catch(() => {});
  }, []);

  const title = PAGE_TITLES[pathname] || 'Crete Analytics';

  return (
    <div className={`app-layout ${navOpen ? 'nav-open' : ''}`}>
      <nav className={`side-nav ${navOpen ? 'open' : ''}`}>
        <div className="nav-header"><span className="nav-brand">Dashboards</span></div>
        <ul className="nav-links">
          <li className={pathname === '/' ? 'nav-active' : ''}>
            <Link href="/" onClick={() => setNavOpen(false)}>
              <span className="nav-icon"><HomeIcon /></span>
              <span className="nav-label">Home</span>
            </Link>
          </li>
          <li className={pathname === '/flash' ? 'nav-active' : ''}>
            <Link href="/flash" onClick={() => setNavOpen(false)}>
              <span className="nav-icon"><FlashIcon /></span>
              <span className="nav-label">Weekly Flash</span>
            </Link>
          </li>
          <li className={pathname === '/ops' ? 'nav-active' : ''}>
            <Link href="/ops" onClick={() => setNavOpen(false)}>
              <span className="nav-icon"><OpsIcon /></span>
              <span className="nav-label">Data Ops</span>
            </Link>
          </li>
          <li className="nav-divider" />
          {NAV_LINKS.map((link, i) => (
            <li key={i}>
              <a href={link.url} target="_blank" rel="noopener noreferrer">
                <span className="nav-icon"><ExternalIcon /></span>
                <span className="nav-label">{link.label}</span>
              </a>
            </li>
          ))}
        </ul>
      </nav>
      {navOpen && <div className="nav-overlay" onClick={() => setNavOpen(false)} />}
      <div className="main-content">
        <header className="top-bar">
          <div className="top-bar-left">
            <button className="nav-toggle" onClick={() => setNavOpen(!navOpen)} aria-label="Toggle navigation"><span className="hamburger" /></button>
            <h1 className="app-title">{title}</h1>
          </div>
          <div className="top-bar-right">
            {userName && <span className="user-greeting">hello {userName.toLowerCase()}</span>}
          </div>
        </header>
        <main className="page-body">
          {children}
        </main>
      </div>
    </div>
  );
}
