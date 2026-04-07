'use client';
import { useState, useEffect, useRef, useCallback } from 'react';

export default function GenieChat() {
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
