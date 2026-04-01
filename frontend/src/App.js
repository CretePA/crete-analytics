import React, { useState, useEffect } from 'react';

const NAV_LINKS = [
  {
    label: 'Crete Data Ops',
    url: 'https://crete-hq-7405618890836566.6.azure.databricksapps.com',
    icon: '{}',
  },
  {
    label: 'PerformYard',
    url: 'https://performyard-dashboard-7405618890836566.6.azure.databricksapps.com',
    icon: '{}',
  },
];

function App() {
  const [userName, setUserName] = useState('');
  const [navOpen, setNavOpen] = useState(false);

  useEffect(() => {
    fetch('/api/whoami')
      .then(r => r.ok ? r.json() : null)
      .then(d => { if (d && d.name) setUserName(d.name); })
      .catch(() => {});
  }, []);

  return (
    <div className={`app-layout ${navOpen ? 'nav-open' : ''}`}>
      {/* Left nav */}
      <nav className={`side-nav ${navOpen ? 'open' : ''}`}>
        <div className="nav-header">
          <span className="nav-brand">Crete Apps</span>
        </div>
        <ul className="nav-links">
          {NAV_LINKS.map((link, i) => (
            <li key={i}>
              <a href={link.url} target="_blank" rel="noopener noreferrer">
                <span className="nav-icon">{link.icon}</span>
                <span className="nav-label">{link.label}</span>
              </a>
            </li>
          ))}
          <li className="nav-active">
            <a href="/">
              <span className="nav-icon">{'{}'}</span>
              <span className="nav-label">Crete Analytics</span>
            </a>
          </li>
        </ul>
      </nav>

      {/* Overlay for mobile */}
      {navOpen && <div className="nav-overlay" onClick={() => setNavOpen(false)} />}

      {/* Main content */}
      <div className="main-content">
        <header className="top-bar">
          <div className="top-bar-left">
            <button className="nav-toggle" onClick={() => setNavOpen(!navOpen)} aria-label="Toggle navigation">
              <span className="hamburger" />
            </button>
            <h1 className="app-title">Crete Analytics</h1>
          </div>
          <div className="top-bar-right">
            {userName && <span className="user-greeting">hello {userName.toLowerCase()}</span>}
          </div>
        </header>

        <main className="page-body">
          <div className="hello-card">
            <p>hello world</p>
          </div>
        </main>
      </div>
    </div>
  );
}

export default App;
