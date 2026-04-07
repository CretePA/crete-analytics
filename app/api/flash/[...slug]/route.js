import { NextResponse } from 'next/server';
const { runQuery, tbl } = require('@/lib/databricks');

function sanitizeDate(val) {
  if (!val) return null;
  if (!/^\d{4}-\d{2}-\d{2}$/.test(val)) throw new Error('Invalid date format');
  return val;
}

function sanitizeId(val) {
  if (!val) return null;
  if (!/^[\w\s\-\.]+$/.test(val)) throw new Error('Invalid firm ID');
  return val;
}

function flashQuery(asOfDate, memberFirm) {
  const safeDate = sanitizeDate(asOfDate);
  const safeFirm = sanitizeId(memberFirm);
  const dt = safeDate ? `DATE('${safeDate}')` : 'CURRENT_DATE()';
  const firmFilter = (col = 't.member_firm_id') =>
    safeFirm ? ` AND ${col} = '${safeFirm}'` : '';
  return { dt, ff: firmFilter };
}

const handlers = {
  firms: async () => {
    const sql = `
      SELECT DISTINCT member_firm_id, member_firm_id as member_firm_name
      FROM ${tbl('fct_time_entry')}
      WHERE member_firm_id IS NOT NULL AND member_firm_id != ''
      ORDER BY member_firm_id
    `;
    return runQuery(sql);
  },

  'time-gauges': async (dt, ff) => {
    const sql = `
      SELECT
        SUM(CASE WHEN YEAR(entry_date) = YEAR(${dt}) AND MONTH(entry_date) = MONTH(${dt}) AND DAY(entry_date) <= DAY(${dt}) THEN hours ELSE 0 END) as cy_mtd_hours,
        SUM(CASE WHEN YEAR(entry_date) = YEAR(${dt}) AND MONTH(entry_date) = MONTH(${dt}) AND DAY(entry_date) <= DAY(${dt}) THEN production_amount ELSE 0 END) as cy_mtd_tvp,
        SUM(CASE WHEN YEAR(entry_date) = YEAR(${dt}) - 1 AND MONTH(entry_date) = MONTH(${dt}) AND DAY(entry_date) <= DAY(${dt}) THEN hours ELSE 0 END) as py_mtd_hours,
        SUM(CASE WHEN YEAR(entry_date) = YEAR(${dt}) - 1 AND MONTH(entry_date) = MONTH(${dt}) AND DAY(entry_date) <= DAY(${dt}) THEN production_amount ELSE 0 END) as py_mtd_tvp,
        SUM(CASE WHEN YEAR(entry_date) = YEAR(${dt}) - 1 AND MONTH(entry_date) = MONTH(${dt}) THEN hours ELSE 0 END) as py_full_month_hours,
        SUM(CASE WHEN YEAR(entry_date) = YEAR(${dt}) - 1 AND MONTH(entry_date) = MONTH(${dt}) THEN production_amount ELSE 0 END) as py_full_month_tvp,
        SUM(CASE WHEN YEAR(entry_date) = YEAR(${dt}) AND entry_date <= ${dt} THEN hours ELSE 0 END) as cy_ytd_hours,
        SUM(CASE WHEN YEAR(entry_date) = YEAR(${dt}) AND entry_date <= ${dt} THEN production_amount ELSE 0 END) as cy_ytd_tvp,
        SUM(CASE WHEN YEAR(entry_date) = YEAR(${dt}) - 1 AND DAYOFYEAR(entry_date) <= DAYOFYEAR(${dt}) THEN hours ELSE 0 END) as py_ytd_hours,
        SUM(CASE WHEN YEAR(entry_date) = YEAR(${dt}) - 1 AND DAYOFYEAR(entry_date) <= DAYOFYEAR(${dt}) THEN production_amount ELSE 0 END) as py_ytd_tvp,
        SUM(CASE WHEN YEAR(entry_date) = YEAR(${dt}) - 1 THEN hours ELSE 0 END) as py_full_year_hours,
        SUM(CASE WHEN YEAR(entry_date) = YEAR(${dt}) - 1 THEN production_amount ELSE 0 END) as py_full_year_tvp,
        SUM(CASE WHEN YEAR(entry_date) = YEAR(${dt}) AND MONTH(entry_date) = MONTH(${dt}) AND DAY(entry_date) <= DAY(${dt}) THEN COALESCE(write_up_down, 0) ELSE 0 END) as cy_mtd_wuwd,
        SUM(CASE WHEN YEAR(entry_date) = YEAR(${dt}) - 1 AND MONTH(entry_date) = MONTH(${dt}) AND DAY(entry_date) <= DAY(${dt}) THEN COALESCE(write_up_down, 0) ELSE 0 END) as py_mtd_wuwd,
        SUM(CASE WHEN YEAR(entry_date) = YEAR(${dt}) AND entry_date <= ${dt} THEN COALESCE(write_up_down, 0) ELSE 0 END) as cy_ytd_wuwd,
        SUM(CASE WHEN YEAR(entry_date) = YEAR(${dt}) - 1 AND DAYOFYEAR(entry_date) <= DAYOFYEAR(${dt}) THEN COALESCE(write_up_down, 0) ELSE 0 END) as py_ytd_wuwd
      FROM ${tbl('fct_time_entry')} t
      WHERE t.is_billable = true ${ff()}
    `;
    return runQuery(sql);
  },

  'invoice-gauges': async (dt, ff) => {
    const sql = `
      SELECT
        SUM(CASE WHEN YEAR(invoice_date) = YEAR(${dt}) AND MONTH(invoice_date) = MONTH(${dt}) AND DAY(invoice_date) <= DAY(${dt}) THEN invoice_amount ELSE 0 END) as cy_mtd_billings,
        SUM(CASE WHEN YEAR(invoice_date) = YEAR(${dt}) - 1 AND MONTH(invoice_date) = MONTH(${dt}) AND DAY(invoice_date) <= DAY(${dt}) THEN invoice_amount ELSE 0 END) as py_mtd_billings,
        SUM(CASE WHEN YEAR(invoice_date) = YEAR(${dt}) - 1 AND MONTH(invoice_date) = MONTH(${dt}) THEN invoice_amount ELSE 0 END) as py_full_month_billings,
        SUM(CASE WHEN YEAR(invoice_date) = YEAR(${dt}) AND invoice_date <= ${dt} THEN invoice_amount ELSE 0 END) as cy_ytd_billings,
        SUM(CASE WHEN YEAR(invoice_date) = YEAR(${dt}) - 1 AND DAYOFYEAR(invoice_date) <= DAYOFYEAR(${dt}) THEN invoice_amount ELSE 0 END) as py_ytd_billings,
        SUM(CASE WHEN YEAR(invoice_date) = YEAR(${dt}) - 1 THEN invoice_amount ELSE 0 END) as py_full_year_billings
      FROM ${tbl('fct_invoice')} t
      WHERE 1=1 ${ff()}
    `;
    return runQuery(sql);
  },

  'cash-gauges': async (dt, ff) => {
    const sql = `
      SELECT
        SUM(CASE WHEN YEAR(receipt_date) = YEAR(${dt}) AND MONTH(receipt_date) = MONTH(${dt}) AND DAY(receipt_date) <= DAY(${dt}) THEN receipt_amount ELSE 0 END) as cy_mtd_cash,
        SUM(CASE WHEN YEAR(receipt_date) = YEAR(${dt}) - 1 AND MONTH(receipt_date) = MONTH(${dt}) AND DAY(receipt_date) <= DAY(${dt}) THEN receipt_amount ELSE 0 END) as py_mtd_cash,
        SUM(CASE WHEN YEAR(receipt_date) = YEAR(${dt}) - 1 AND MONTH(receipt_date) = MONTH(${dt}) THEN receipt_amount ELSE 0 END) as py_full_month_cash,
        SUM(CASE WHEN YEAR(receipt_date) = YEAR(${dt}) AND receipt_date <= ${dt} THEN receipt_amount ELSE 0 END) as cy_ytd_cash,
        SUM(CASE WHEN YEAR(receipt_date) = YEAR(${dt}) - 1 AND DAYOFYEAR(receipt_date) <= DAYOFYEAR(${dt}) THEN receipt_amount ELSE 0 END) as py_ytd_cash,
        SUM(CASE WHEN YEAR(receipt_date) = YEAR(${dt}) - 1 THEN receipt_amount ELSE 0 END) as py_full_year_cash
      FROM ${tbl('fct_cash_receipt')} t
      WHERE 1=1 ${ff()}
    `;
    return runQuery(sql);
  },

  'service-lines': async (dt, ff) => {
    const sql = `
      SELECT service_line,
        SUM(CASE WHEN YEAR(entry_date) = YEAR(${dt}) AND MONTH(entry_date) = MONTH(${dt}) AND DAY(entry_date) <= DAY(${dt}) THEN hours ELSE 0 END) as cy_mtd_hours,
        SUM(CASE WHEN YEAR(entry_date) = YEAR(${dt}) AND MONTH(entry_date) = MONTH(${dt}) AND DAY(entry_date) <= DAY(${dt}) THEN production_amount ELSE 0 END) as cy_mtd_tvp,
        SUM(CASE WHEN YEAR(entry_date) = YEAR(${dt}) - 1 AND MONTH(entry_date) = MONTH(${dt}) AND DAY(entry_date) <= DAY(${dt}) THEN hours ELSE 0 END) as py_mtd_hours,
        SUM(CASE WHEN YEAR(entry_date) = YEAR(${dt}) - 1 AND MONTH(entry_date) = MONTH(${dt}) AND DAY(entry_date) <= DAY(${dt}) THEN production_amount ELSE 0 END) as py_mtd_tvp,
        SUM(CASE WHEN YEAR(entry_date) = YEAR(${dt}) AND entry_date <= ${dt} THEN hours ELSE 0 END) as cy_ytd_hours,
        SUM(CASE WHEN YEAR(entry_date) = YEAR(${dt}) AND entry_date <= ${dt} THEN production_amount ELSE 0 END) as cy_ytd_tvp,
        SUM(CASE WHEN YEAR(entry_date) = YEAR(${dt}) - 1 AND DAYOFYEAR(entry_date) <= DAYOFYEAR(${dt}) THEN hours ELSE 0 END) as py_ytd_hours,
        SUM(CASE WHEN YEAR(entry_date) = YEAR(${dt}) - 1 AND DAYOFYEAR(entry_date) <= DAYOFYEAR(${dt}) THEN production_amount ELSE 0 END) as py_ytd_tvp
      FROM ${tbl('fct_time_entry')} t
      WHERE t.is_billable = true ${ff()}
      GROUP BY service_line ORDER BY service_line
    `;
    return runQuery(sql);
  },

  utilization: async (dt, ff) => {
    const sql = `
      SELECT p.staff_level, p.staff_level_index,
        COUNT(DISTINCT t.person_key) as headcount,
        SUM(t.hours) as total_hours,
        SUM(t.production_amount) as total_tvp
      FROM ${tbl('fct_time_entry')} t
      JOIN ${tbl('dim_person')} p ON t.person_key = p.person_key
      WHERE t.is_billable = true
        AND WEEKOFYEAR(t.entry_date) = WEEKOFYEAR(${dt})
        AND YEAR(t.entry_date) = YEAR(${dt})
        AND p.staff_level IS NOT NULL AND p.staff_level != ''
        ${ff()}
      GROUP BY p.staff_level, p.staff_level_index
      ORDER BY p.staff_level_index
    `;
    return runQuery(sql);
  },

  'ar-aging': async (dt, ff) => {
    const sql = `
      SELECT
        SUM(ar_0_30) as current_bucket,
        SUM(ar_31_60) as bucket_31_60,
        SUM(ar_61_90) as bucket_61_90,
        SUM(ar_91_120) as bucket_91_120,
        SUM(ar_121_150 + ar_151_180 + ar_181_364 + ar_365_plus) as bucket_120_plus,
        SUM(ar_total) as total
      FROM ${tbl('fct_ar_aging_snapshot')} t
      WHERE t.report_date = (
        SELECT MAX(report_date) FROM ${tbl('fct_ar_aging_snapshot')} WHERE report_date <= ${dt}
      ) ${ff()}
    `;
    return runQuery(sql);
  },

  'wip-risk': async (dt, ff) => {
    const sql = `
      SELECT c.client_name, c.partner_name,
        SUM(t.wip_61_90 + t.wip_91_120 + t.wip_121_150 + t.wip_151_180 +
            t.wip_181_210 + t.wip_211_240 + t.wip_241_270 + t.wip_271_300 +
            t.wip_301_330 + t.wip_331_364 + t.wip_1yr_2yr + t.wip_2yr_plus) as wip_amount
      FROM ${tbl('fct_wip_aging_snapshot')} t
      JOIN ${tbl('dim_client')} c ON t.client_key = c.client_key
      WHERE t.report_date = (
        SELECT MAX(report_date) FROM ${tbl('fct_wip_aging_snapshot')} WHERE report_date <= ${dt}
      ) ${ff()}
      GROUP BY c.client_name, c.partner_name
      HAVING wip_amount > 0 ORDER BY wip_amount DESC LIMIT 50
    `;
    return runQuery(sql);
  },

  'ar-risk': async (dt, ff) => {
    const sql = `
      SELECT c.client_name, c.partner_name,
        SUM(t.ar_61_90 + t.ar_91_120 + t.ar_121_150 + t.ar_151_180 +
            t.ar_181_364 + t.ar_365_plus) as ar_amount
      FROM ${tbl('fct_ar_aging_snapshot')} t
      JOIN ${tbl('dim_client')} c ON t.client_key = c.client_key
      WHERE t.report_date = (
        SELECT MAX(report_date) FROM ${tbl('fct_ar_aging_snapshot')} WHERE report_date <= ${dt}
      ) ${ff()}
      GROUP BY c.client_name, c.partner_name
      HAVING ar_amount > 0 ORDER BY ar_amount DESC LIMIT 50
    `;
    return runQuery(sql);
  },
};

export async function GET(request, { params }) {
  const slug = (await params).slug;
  const endpoint = slug.join('/');
  const handler = handlers[endpoint];
  if (!handler) {
    return NextResponse.json({ error: `Unknown endpoint: ${endpoint}` }, { status: 404 });
  }

  const { searchParams } = new URL(request.url);
  const asOfDate = searchParams.get('as_of_date');
  const memberFirm = searchParams.get('member_firm');

  try {
    if (endpoint === 'firms') {
      return NextResponse.json(await handler());
    }
    const { dt, ff } = flashQuery(asOfDate, memberFirm);
    return NextResponse.json(await handler(dt, ff));
  } catch (e) {
    console.error(`Flash ${endpoint} failed:`, e);
    return NextResponse.json({ error: String(e.message || e) }, { status: 500 });
  }
}
