import { NextResponse } from 'next/server';

const HOST = process.env.DATABRICKS_HOST || process.env.DATABRICKS_SERVER_HOSTNAME || '';
const GENIE_SPACE_ID = process.env.GENIE_SPACE_ID || '';

async function getAuthHeaders() {
  const token = process.env.DATABRICKS_TOKEN;
  if (token) return { Authorization: `Bearer ${token}` };

  const clientId = process.env.DATABRICKS_CLIENT_ID;
  const clientSecret = process.env.DATABRICKS_CLIENT_SECRET;
  if (clientId && clientSecret) {
    const res = await fetch(`https://${HOST}/oidc/v1/token`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({
        grant_type: 'client_credentials',
        client_id: clientId,
        client_secret: clientSecret,
        scope: 'all-apis',
      }),
    });
    const data = await res.json();
    return { Authorization: `Bearer ${data.access_token}` };
  }
  return {};
}

async function pollForResult(conversationId, messageId, auth) {
  const base = `https://${HOST}/api/2.0/genie/spaces/${GENIE_SPACE_ID}/conversations/${conversationId}/messages/${messageId}`;
  for (let i = 0; i < 60; i++) {
    const res = await fetch(base, { headers: { ...auth, 'Content-Type': 'application/json' } });
    const data = await res.json();
    if (data.status === 'COMPLETED' || data.status === 'FAILED') return data;
    await new Promise(r => setTimeout(r, 2000));
  }
  throw new Error('Genie timed out');
}

export async function POST(request) {
  if (!GENIE_SPACE_ID) {
    return NextResponse.json({ error: 'GENIE_SPACE_ID not configured' }, { status: 500 });
  }

  try {
    const { question, conversation_id } = await request.json();
    const auth = await getAuthHeaders();
    const apiBase = `https://${HOST}/api/2.0/genie/spaces/${GENIE_SPACE_ID}`;

    let res;
    if (conversation_id) {
      res = await fetch(`${apiBase}/conversations/${conversation_id}/messages`, {
        method: 'POST',
        headers: { ...auth, 'Content-Type': 'application/json' },
        body: JSON.stringify({ content: question }),
      });
    } else {
      res = await fetch(`${apiBase}/start-conversation`, {
        method: 'POST',
        headers: { ...auth, 'Content-Type': 'application/json' },
        body: JSON.stringify({ content: question }),
      });
    }

    const initial = await res.json();
    const convId = initial.conversation_id || conversation_id;
    const msgId = initial.message_id || initial.id;

    const result = await pollForResult(convId, msgId, auth);

    const attachments = [];
    for (const att of (result.attachments || [])) {
      if (att.text) {
        attachments.push({ type: 'text', content: att.text.content });
      }
      if (att.query) {
        const a = { type: 'query', sql: att.query.query || '' };
        const aid = att.attachment_id || att.query?.id;
        if (aid) {
          try {
            const qr = await fetch(
              `${apiBase}/conversations/${convId}/messages/${msgId}/query-result/${aid}`,
              { headers: { ...auth, 'Content-Type': 'application/json' } }
            );
            const qrData = await qr.json();
            const columns = (qrData.statement_response?.manifest?.schema?.columns || []).map(c => c.name);
            const rows = (qrData.statement_response?.result?.data_array || [])
              .slice(0, 200)
              .map(row => Object.fromEntries(columns.map((c, i) => [c, row[i]])));
            a.columns = columns;
            a.rows = rows;
          } catch (e) {
            console.warn('Could not fetch query result:', e.message);
          }
        }
        attachments.push(a);
      }
    }

    return NextResponse.json({ conversation_id: convId, message_id: msgId, attachments });
  } catch (e) {
    console.error('Genie error:', e);
    return NextResponse.json({ error: String(e.message || e) }, { status: 500 });
  }
}
