import React, { useState, useEffect, useRef, useCallback } from 'react';
import WeeklyFlash from './WeeklyFlash';
import DataOpsHQ from './DataOpsHQ';

const NAV_LINKS = [
  { label: 'PerformYard', url: 'https://performyard-dashboard-7405618890836566.6.azure.databricksapps.com' },
];

const HELLO_PHRASES = [
  { text: 'Hello World', lang: 'English' },
  { text: 'Hola Mundo', lang: 'Spanish' },
  { text: 'Bonjour le Monde', lang: 'French' },
  { text: 'Hallo Welt', lang: 'German' },
  { text: 'Ciao Mondo', lang: 'Italian' },
  { text: 'Olá Mundo', lang: 'Portuguese' },
  { text: 'こんにちは世界', lang: 'Japanese' },
  { text: '你好世界', lang: 'Chinese' },
  { text: '안녕하세요 세계', lang: 'Korean' },
  { text: 'مرحبا بالعالم', lang: 'Arabic' },
  { text: 'Привет мир', lang: 'Russian' },
  { text: 'Hej Världen', lang: 'Swedish' },
  { text: 'Merhaba Dünya', lang: 'Turkish' },
  { text: 'Γειά σου Κόσμε', lang: 'Greek' },
  { text: 'नमस्ते दुनिया', lang: 'Hindi' },
  { text: 'Witaj Świecie', lang: 'Polish' },
  { text: 'Xin chào Thế giới', lang: 'Vietnamese' },
  { text: 'Hei Maailma', lang: 'Finnish' },
];

function CyclingHello() {
  const [index, setIndex] = useState(0);
  const [fade, setFade] = useState('in');
  useEffect(() => {
    const timer = setInterval(() => {
      setFade('out');
      setTimeout(() => { setIndex(i => (i + 1) % HELLO_PHRASES.length); setFade('in'); }, 600);
    }, 10000);
    return () => clearInterval(timer);
  }, []);
  const phrase = HELLO_PHRASES[index];
  return (
    <div className="hello-hero">
      <div className={`hello-text ${fade}`}><span className="hello-phrase">{phrase.text}</span></div>
      <div className={`hello-lang ${fade}`}>{phrase.lang}</div>
    </div>
  );
}

function GenieChat() {
  const [messages, setMessages] = useState([]);
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const [conversationId, setConversationId] = useState(null);
  const messagesEnd = useRef(null);
  const scrollToBottom = useCallback(() => { messagesEnd.current?.scrollIntoView({ behavior: 'smooth' }); }, []);
  useEffect(scrollToBottom, [messages, scrollToBottom]);

  const handleSubmit = async (e) => {
    e.preventDefault();
    const q = input.trim();
    if (!q || loading) return;
    setMessages(prev => [...prev, { role: 'user', content: q }]);
    setInput('');
    setLoading(true);
    try {
      const res = await fetch('/api/genie/ask', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ question: q, conversation_id: conversationId }),
      });
      const data = await res.json();
      if (data.error) {
        setMessages(prev => [...prev, { role: 'error', content: data.error }]);
      } else {
        if (data.conversation_id) setConversationId(data.conversation_id);
        for (const att of (data.attachments || [])) {
          if (att.type === 'text') setMessages(prev => [...prev, { role: 'genie', content: att.content }]);
          if (att.type === 'query') setMessages(prev => [...prev, { role: 'genie-table', sql: att.sql, columns: att.columns || [], rows: att.rows || [] }]);
        }
        if (!data.attachments?.length) setMessages(prev => [...prev, { role: 'genie', content: 'No results returned.' }]);
      }
    } catch { setMessages(prev => [...prev, { role: 'error', content: 'Failed to reach Genie.' }]); }
    finally { setLoading(false); }
  };

  return (
    <div className="genie-panel">
      <div className="genie-header">
        <span className="genie-title">Genie</span>
        {conversationId && <button className="genie-new-btn" onClick={() => { setMessages([]); setConversationId(null); }}>New chat</button>}
      </div>
      <div className="genie-messages">
        {messages.length === 0 && <div className="genie-empty">Ask a question about your data</div>}
        {messages.map((msg, i) => {
          if (msg.role === 'user') return <div key={i} className="genie-msg genie-msg-user"><p>{msg.content}</p></div>;
          if (msg.role === 'error') return <div key={i} className="genie-msg genie-msg-error"><p>{msg.content}</p></div>;
          if (msg.role === 'genie') return <div key={i} className="genie-msg genie-msg-genie"><p>{msg.content}</p></div>;
          if (msg.role === 'genie-table') return (
            <div key={i} className="genie-msg genie-msg-genie">
              {msg.sql && <pre className="genie-sql">{msg.sql}</pre>}
              {msg.columns.length > 0 && (
                <div className="genie-table-wrap">
                  <table className="genie-table">
                    <thead><tr>{msg.columns.map((c, j) => <th key={j}>{c}</th>)}</tr></thead>
                    <tbody>{msg.rows.slice(0, 50).map((row, ri) => (
                      <tr key={ri}>{msg.columns.map((c, ci) => <td key={ci}>{row[c]}</td>)}</tr>
                    ))}</tbody>
                  </table>
                </div>
              )}
            </div>
          );
          return null;
        })}
        {loading && <div className="genie-msg genie-msg-genie"><div className="genie-typing"><span /><span /><span /></div></div>}
        <div ref={messagesEnd} />
      </div>
      <form className="genie-input-bar" onSubmit={handleSubmit}>
        <input type="text" value={input} onChange={e => setInput(e.target.value)} placeholder="Ask Genie a question..." disabled={loading} />
        <button type="submit" disabled={loading || !input.trim()}>
          <svg width="16" height="16" viewBox="0 0 16 16" fill="none"><path d="M2 8h12M9 3l5 5-5 5" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"/></svg>
        </button>
      </form>
    </div>
  );
}

function HomePage() {
  return (
    <div className="page-stack">
      <div className="hello-card"><CyclingHello /></div>
      <GenieChat />
    </div>
  );
}

function App() {
  const [userName, setUserName] = useState('');
  const [navOpen, setNavOpen] = useState(false);
  const [page, setPage] = useState(() => {
    const h = window.location.hash;
    if (h === '#flash') return 'flash';
    if (h === '#ops') return 'ops';
    return 'home';
  });

  useEffect(() => {
    fetch('/api/whoami').then(r => r.ok ? r.json() : null).then(d => { if (d?.name) setUserName(d.name); }).catch(() => {});
    const onHash = () => {
      const h = window.location.hash;
      if (h === '#flash') setPage('flash');
      else if (h === '#ops') setPage('ops');
      else setPage('home');
    };
    window.addEventListener('hashchange', onHash);
    return () => window.removeEventListener('hashchange', onHash);
  }, []);

  const navigate = (p) => {
    window.location.hash = p === 'home' ? '' : p;
    setPage(p);
    setNavOpen(false);
  };

  return (
    <div className={`app-layout ${navOpen ? 'nav-open' : ''}`}>
      <nav className={`side-nav ${navOpen ? 'open' : ''}`}>
        <div className="nav-header"><span className="nav-brand">Dashboards</span></div>
        <ul className="nav-links">
          <li className={page === 'home' ? 'nav-active' : ''}>
            <a href="#" onClick={e => { e.preventDefault(); navigate('home'); }}>
              <span className="nav-icon">
                <svg width="16" height="16" viewBox="0 0 16 16" fill="none"><path d="M2 8l6-6 6 6M3 7v6a1 1 0 001 1h3V10h2v4h3a1 1 0 001-1V7" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"/></svg>
              </span>
              <span className="nav-label">Home</span>
            </a>
          </li>
          <li className={page === 'flash' ? 'nav-active' : ''}>
            <a href="#flash" onClick={e => { e.preventDefault(); navigate('flash'); }}>
              <span className="nav-icon">
                <svg width="16" height="16" viewBox="0 0 16 16" fill="none"><path d="M9 1L3 9h4l-1 6 6-8H8l1-6z" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"/></svg>
              </span>
              <span className="nav-label">Weekly Flash</span>
            </a>
          </li>
          <li className={page === 'ops' ? 'nav-active' : ''}>
            <a href="#ops" onClick={e => { e.preventDefault(); navigate('ops'); }}>
              <span className="nav-icon">
                <svg width="16" height="16" viewBox="0 0 16 16" fill="none"><path d="M2 4h12M2 8h12M2 12h12" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round"/><circle cx="5" cy="4" r="1.5" fill="currentColor"/><circle cx="11" cy="8" r="1.5" fill="currentColor"/><circle cx="7" cy="12" r="1.5" fill="currentColor"/></svg>
              </span>
              <span className="nav-label">Data Ops</span>
            </a>
          </li>
          <li className="nav-divider" />
          {NAV_LINKS.map((link, i) => (
            <li key={i}>
              <a href={link.url} target="_blank" rel="noopener noreferrer">
                <span className="nav-icon">
                  <svg width="16" height="16" viewBox="0 0 16 16" fill="none"><path d="M6 2H3a1 1 0 00-1 1v3m0 4v3a1 1 0 001 1h3m4 0h3a1 1 0 001-1v-3m0-4V3a1 1 0 00-1-1h-3" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"/></svg>
                </span>
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
            <h1 className="app-title">{page === 'flash' ? 'Weekly Flash' : page === 'ops' ? 'Data Ops HQ' : 'Crete Analytics'}</h1>
          </div>
          <div className="top-bar-right">
            {userName && <span className="user-greeting">hello {userName.toLowerCase()}</span>}
          </div>
        </header>
        <main className="page-body">
          {page === 'flash' ? <WeeklyFlash /> : page === 'ops' ? <DataOpsHQ /> : <HomePage />}
        </main>
      </div>
    </div>
  );
}

export default App;
