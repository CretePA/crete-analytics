import { NextResponse } from 'next/server';
const { runQuery, cached, tbl, CACHE_TTL, CACHE_TTL_SLOW } = require('@/lib/databricks');

const WARN_HOURS = 6;
const ERROR_HOURS = 24;

const HEARTBEAT_SQL = `
  WITH agg AS (
    SELECT firm, system,
      MAX(completed_at) AS last_completed,
      COUNT(DISTINCT DATE(started_at)) AS run_days,
      COUNT(*) AS total_runs,
      SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed_runs
    FROM sandbox.gold.vw_ops__ingestion_run_history
    GROUP BY firm, system
  ),
  latest AS (
    SELECT firm, run_mode AS latest_mode, status AS latest_status,
      ROW_NUMBER() OVER (PARTITION BY firm ORDER BY completed_at DESC) AS rn
    FROM sandbox.gold.vw_ops__ingestion_run_history
  )
  SELECT a.firm, a.system, a.last_completed, a.run_days, a.total_runs, a.failed_runs,
    l.latest_mode, l.latest_status,
    CASE WHEN a.run_days > 1 THEN 'ongoing' ELSE 'historical' END AS classification,
    ROUND((UNIX_TIMESTAMP(CURRENT_TIMESTAMP) - UNIX_TIMESTAMP(a.last_completed)) / 3600.0, 1) AS hours_since_last,
    CASE
      WHEN a.run_days <= 1 THEN 'historical'
      WHEN (UNIX_TIMESTAMP(CURRENT_TIMESTAMP) - UNIX_TIMESTAMP(a.last_completed)) / 3600.0 <= ${WARN_HOURS} THEN 'ok'
      WHEN (UNIX_TIMESTAMP(CURRENT_TIMESTAMP) - UNIX_TIMESTAMP(a.last_completed)) / 3600.0 <= ${ERROR_HOURS} THEN 'warn'
      ELSE 'error'
    END AS freshness_status
  FROM agg a LEFT JOIN latest l ON a.firm = l.firm AND l.rn = 1
  ORDER BY CASE WHEN a.run_days <= 1 THEN 2 ELSE 1 END, a.last_completed ASC
`;

const JOBS_SQL = `
  WITH job_stats AS (
    SELECT j.name AS job_name, COUNT(*) AS total_runs,
      SUM(CASE WHEN rt.result_state = 'SUCCEEDED' THEN 1 ELSE 0 END) AS passed,
      SUM(CASE WHEN rt.result_state IN ('FAILED', 'ERROR', 'TIMEDOUT') THEN 1 ELSE 0 END) AS failed,
      ROUND(100.0 * SUM(CASE WHEN rt.result_state IN ('FAILED', 'ERROR', 'TIMEDOUT') THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1) AS error_pct,
      MAX(rt.period_end_time) AS last_run
    FROM system.lakeflow.job_run_timeline rt
    JOIN system.lakeflow.jobs j ON rt.job_id = j.job_id
    WHERE rt.period_end_time >= CURRENT_TIMESTAMP - INTERVAL 24 HOURS AND rt.result_state IS NOT NULL
    GROUP BY j.name
  ),
  latest_errors AS (
    SELECT j.name AS job_name, rt.termination_code,
      ROW_NUMBER() OVER (PARTITION BY j.name ORDER BY rt.period_end_time DESC) AS rn
    FROM system.lakeflow.job_run_timeline rt
    JOIN system.lakeflow.jobs j ON rt.job_id = j.job_id
    WHERE rt.period_end_time >= CURRENT_TIMESTAMP - INTERVAL 24 HOURS
      AND rt.result_state IN ('FAILED', 'ERROR', 'TIMEDOUT')
  )
  SELECT s.*, e.termination_code AS last_error
  FROM job_stats s LEFT JOIN latest_errors e ON s.job_name = e.job_name AND e.rn = 1
  ORDER BY s.error_pct DESC, s.job_name
`;

const SPEND_SQL = `
  SELECT
    CASE WHEN u.usage_date >= CURRENT_DATE - INTERVAL 30 DAYS THEN 'last_30' ELSE 'prior_30' END AS period,
    ROUND(SUM(u.usage_quantity * lp.pricing.default), 2) AS total_cost,
    ROUND(SUM(u.usage_quantity), 1) AS total_dbus
  FROM system.billing.usage u
  JOIN system.billing.list_prices lp
    ON u.sku_name = lp.sku_name AND u.usage_start_time >= lp.price_start_time
    AND (lp.price_end_time IS NULL OR u.usage_start_time < lp.price_end_time)
  WHERE u.usage_date >= CURRENT_DATE - INTERVAL 60 DAYS
  GROUP BY 1 ORDER BY 1
`;

const SPEND_BREAKDOWN_SQL = `
  SELECT u.sku_name,
    ROUND(SUM(CASE WHEN u.usage_date >= CURRENT_DATE - INTERVAL 30 DAYS THEN u.usage_quantity * lp.pricing.default ELSE 0 END), 2) AS last_30_cost,
    ROUND(SUM(CASE WHEN u.usage_date < CURRENT_DATE - INTERVAL 30 DAYS THEN u.usage_quantity * lp.pricing.default ELSE 0 END), 2) AS prior_30_cost,
    ROUND(SUM(CASE WHEN u.usage_date >= CURRENT_DATE - INTERVAL 30 DAYS THEN u.usage_quantity * lp.pricing.default ELSE 0 END)
      - SUM(CASE WHEN u.usage_date < CURRENT_DATE - INTERVAL 30 DAYS THEN u.usage_quantity * lp.pricing.default ELSE 0 END), 2) AS delta
  FROM system.billing.usage u
  JOIN system.billing.list_prices lp
    ON u.sku_name = lp.sku_name AND u.usage_start_time >= lp.price_start_time
    AND (lp.price_end_time IS NULL OR u.usage_start_time < lp.price_end_time)
  WHERE u.usage_date >= CURRENT_DATE - INTERVAL 60 DAYS
  GROUP BY u.sku_name HAVING last_30_cost > 1 OR prior_30_cost > 1
  ORDER BY delta DESC
`;

const GOLD_FRESHNESS_SQL = `
  WITH firm_freshness AS (
    SELECT mf.member_firm_name AS firm, 'fct_time_entry' AS gold_table,
      MAX(LEAST(te.entry_date, CURRENT_DATE)) AS latest_data_date,
      COUNT(*) AS row_count
    FROM sandbox.gold.fct_time_entry te
    JOIN sandbox.gold.dim_member_firm mf ON te.member_firm_id = mf.member_firm_id
    WHERE te.entry_date >= CURRENT_DATE - INTERVAL 90 DAYS
    GROUP BY mf.member_firm_name
  )
  SELECT firm, gold_table, latest_data_date, row_count,
    GREATEST(DATEDIFF(CURRENT_DATE, latest_data_date), 0) AS days_stale,
    CASE
      WHEN DATEDIFF(CURRENT_DATE, latest_data_date) <= 2 THEN 'ok'
      WHEN DATEDIFF(CURRENT_DATE, latest_data_date) <= 7 THEN 'warn'
      ELSE 'error'
    END AS freshness_status
  FROM firm_freshness ORDER BY latest_data_date DESC
`;

async function heartbeatRaw() {
  const result = await runQuery(HEARTBEAT_SQL);
  if (!result?.rows || !Array.isArray(result.rows)) {
    console.error('heartbeat query returned unexpected shape:', result);
    return [];
  }
  return result.rows;
}

async function jobsRaw() {
  const result = await runQuery(JOBS_SQL);
  if (!result?.rows || !Array.isArray(result.rows)) {
    console.error('jobs query returned unexpected shape:', result);
    return [];
  }
  return result.rows;
}

const handlers = {
  heartbeat: async () => {
    return cached('ops_heartbeat', heartbeatRaw);
  },

  jobs: async () => {
    return cached('ops_jobs', jobsRaw);
  },

  spend: async () => {
    const data = await cached('ops_spend', async () => (await runQuery(SPEND_SQL)).rows, CACHE_TTL_SLOW);
    const breakdown = await cached('ops_spend_breakdown', async () => (await runQuery(SPEND_BREAKDOWN_SQL)).rows, CACHE_TTL_SLOW);
    const last = data.find(d => d.period === 'last_30') || {};
    const prior = data.find(d => d.period === 'prior_30') || {};
    const lastCost = last.total_cost || 0;
    const priorCost = prior.total_cost || 0;
    const pctChange = priorCost ? Math.round(((lastCost - priorCost) / priorCost) * 1000) / 10 : 0;
    return {
      last_30: lastCost, prior_30: priorCost,
      last_30_dbus: last.total_dbus || 0, prior_30_dbus: prior.total_dbus || 0,
      pct_change: pctChange, breakdown,
    };
  },

  'gold-freshness': async () => {
    return cached('ops_gold', async () => {
      const result = await runQuery(GOLD_FRESHNESS_SQL);
      return result?.rows || [];
    }, CACHE_TTL_SLOW);
  },

  summary: async () => {
    const [hbData, jobsData, goldData] = await Promise.all([
      cached('ops_heartbeat', heartbeatRaw),
      cached('ops_jobs', jobsRaw),
      cached('ops_gold', async () => (await runQuery(GOLD_FRESHNESS_SQL)).rows, CACHE_TTL_SLOW),
    ]);

    const ongoing = hbData.filter(f => f.classification === 'ongoing');
    const fresh = ongoing.filter(f => f.freshness_status === 'ok');
    const healthyJobs = jobsData.filter(j => (j.error_pct || 0) === 0);
    const goldFresh = goldData.filter(g => g.freshness_status === 'ok');
    const goldStale = goldData.filter(g => g.freshness_status !== 'ok').map(g => `${g.firm} (${g.days_stale || '?'}d)`);
    const totalRuns = hbData.reduce((s, f) => s + (f.total_runs || 0), 0);
    const totalFailed = hbData.reduce((s, f) => s + (f.failed_runs || 0), 0);
    const staleFirms = ongoing.filter(f => f.freshness_status !== 'ok').map(f => `${f.firm} (${f.hours_since_last || '?'}h)`);
    const failingJobs = jobsData.filter(j => (j.error_pct || 0) > 0).map(j => `${j.job_name} (${j.error_pct}%)`);

    return {
      freshness: {
        ok: fresh.length, total: ongoing.length,
        status: fresh.length === ongoing.length ? 'ok' : (fresh.length >= ongoing.length - 1 ? 'warn' : 'error'),
        failing: staleFirms,
      },
      jobs: {
        healthy: healthyJobs.length, total: jobsData.length,
        status: healthyJobs.length === jobsData.length ? 'ok' : (healthyJobs.length >= jobsData.length - 1 ? 'warn' : 'error'),
        failing: failingJobs,
      },
      gold: {
        fresh: goldFresh.length, total: goldData.length,
        status: goldFresh.length === goldData.length ? 'ok' : (goldStale.length <= 3 ? 'warn' : 'error'),
        stale: goldStale,
      },
      pipeline: { total_runs: totalRuns, total_failed: totalFailed, firms_monitored: hbData.length },
    };
  },
};

export async function GET(request, { params }) {
  const slug = (await params).slug;
  const endpoint = slug.join('/');
  const handler = handlers[endpoint];
  if (!handler) {
    return NextResponse.json({ error: `Unknown endpoint: ${endpoint}` }, { status: 404 });
  }
  try {
    const data = await handler();
    return NextResponse.json(data);
  } catch (e) {
    console.error(`Ops ${endpoint} failed:`, e);
    return NextResponse.json({ error: String(e.message || e) }, { status: 500 });
  }
}
