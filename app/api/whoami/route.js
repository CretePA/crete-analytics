import { headers } from 'next/headers';
import { NextResponse } from 'next/server';

export async function GET() {
  const h = headers();
  const email = h.get('x-forwarded-email') || h.get('x-databricks-user-email') || h.get('x-real-email') || '';
  const user = h.get('x-forwarded-user') || h.get('x-databricks-user') || h.get('x-real-user') || '';
  const name = email ? email.split('@')[0].split('.')[0].replace(/^\w/, c => c.toUpperCase()) : user || '';
  return NextResponse.json({ email, user, name });
}
