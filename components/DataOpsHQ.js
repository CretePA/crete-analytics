'use client';
import { useState, useEffect, useCallback } from 'react';
import '@/styles/dataops.css';

const REFRESH_INTERVAL = 60_000;

function formatAge(hoursFloat) {
  if (hoursFloat == null) return '\u2014';
  const h = Math.abs(hoursFloat);
  if (h < 1) return `${Math.round(h * 60)}m ago`;
  if (h < 24) return `${Math.round(h)}h ago`;
  const days = Math.floor(h / 24);
  return `${days}d ago`;
}

function StatusDot({ status }) {
  return <span className={`ops-status-dot ${status}`} />;
}

function KpiBar({ items, okCount }) {
  if (!items || items.length === 0) return null;
  return (
    <div className="ops-kpi-bar">
      {items.map((item, i) => (
        <div key={i} className={`ops-kpi-bar-segment ${i < okCount ? 'filled' : 'fail'}`} title={item.name} />
      ))}
    </div>
  );
}

function StatusTag({ status }) {
  if (status === 'historical') return <span className="ops-historical-tag">ARCHIVE</span>;
  if (status === 'error') return <span className="ops-stale-tag">STALE</span>;
  if (status === 'warn') return <span className="ops-warn-tag">WARN</span>;
  return <span className="ops-ok-tag">OK</span>;
}

function ErrorBar({ pct }) {
  const cls = pct === 0 ? 'ok' : pct < 15 ? 'warn' : 'error';
  return (
    <div className="ops-error-bar-cell">
      <div className="ops-error-bar">
        <div className={`ops-error-bar-fill ${cls}`} style={{ width: `${Math.min(pct, 100)}%` }} />
      </div>
      <span>{pct}%</span>
    </div>
  );
}

function KpiTooltip({ items, label }) {
  if (!items || items.length === 0) return null;
  return (
    <div className="ops-kpi-tooltip">
      <div className="ops-kpi-tooltip-title">{label}</div>
      {items.map((item, i) => <div key={i} className="ops-kpi-tooltip-item">{item}</div>)}
    </div>
  );
}

export default function DataOpsHQ() {
  const [summary, setSummary] = useState(null);
  const [heartbeat, setHeartbeat] = useState(null);
  const [jobs, setJobs] = useState(null);
  const [goldFreshness, setGoldFreshness] = useState(null);
  const [spend, setSpend] = useState(null);
  const [showSpend, setShowSpend] = useState(false);
  const [error, setError] = useState(null);
  const [lastRefresh, setLastRefresh] = useState(null);

  const fetchData = useCallback(async () => {
    const safeFetch = async (url) => {
      try { const res = await fetch(url); if (!res.ok) return null; return await res.json(); } catch { return null; }
    };
    try {
      const [sumData, hbData, jobsData, goldData, spendData] = await Promise.all([
        safeFetch('/api/ops/summary'),
        safeFetch('/api/ops/heartbeat'),
        safeFetch('/api/ops/jobs'),
        safeFetch('/api/ops/gold-freshness'),
        safeFetch('/api/ops/spend'),
      ]);
      if (sumData) setSummary(sumData);
      if (hbData) setHeartbeat(Array.isArray(hbData) ? hbData : []);
      if (jobsData) setJobs(Array.isArray(jobsData) ? jobsData : []);
      if (goldData) setGoldFreshness(Array.isArray(goldData) ? goldData : []);
      if (spendData) setSpend(spendData);
      setError(null);
      setLastRefresh(new Date());
    } catch (e) { setError(e.message); }
  }, []);

  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, REFRESH_INTERVAL);
    return () => clearInterval(interval);
  }, [fetchData]);

  const freshnessBarItems = heartbeat
    ? heartbeat.filter(r => r.classification === 'ongoing')
        .sort((a, b) => (a.freshness_status === 'ok' ? -1 : 1))
        .map(r => ({ name: `${r.firm} (${formatAge(r.hours_since_last)})`, status: r.freshness_status }))
    : [];
  const freshnessOk = freshnessBarItems.filter(i => i.status === 'ok').length;

  const goldBarItems = goldFreshness
    ? [...goldFreshness].sort((a, b) => (a.freshness_status === 'ok' ? -1 : 1))
        .map(r => ({ name: `${r.firm} (${r.days_stale}d)`, status: r.freshness_status === 'ok' ? 'ok' : 'fail' }))
    : [];
  const goldOk = goldBarItems.filter(i => i.status === 'ok').length;

  const jobsBarItems = jobs
    ? [...jobs].sort((a, b) => (a.error_pct || 0) - (b.error_pct || 0))
        .map(r => ({ name: `${r.job_name} (${r.error_pct || 0}%)`, status: r.error_pct === 0 ? 'ok' : 'fail' }))
    : [];
  const jobsOk = jobsBarItems.filter(i => i.status === 'ok').length;

  return (
    <div className="ops-container">
      {/* Header */}
      <div className="ops-header">
        <div className="ops-header-top">
          <div>
            <div className="ops-header-subtitle">Data Ops</div>
            <div className="ops-header-title">Mission Control</div>
          </div>
          <div className="ops-header-right">
            {spend && (
              <div className="ops-spend-wrapper">
                <div
                  className={`ops-spend-badge ${spend.pct_change > 10 ? 'up' : spend.pct_change < -10 ? 'down' : 'flat'}`}
                  onClick={() => setShowSpend(s => !s)}
                  title={`Last 30d: $${spend.last_30?.toLocaleString()} | Prior 30d: $${spend.prior_30?.toLocaleString()} | ${spend.pct_change > 0 ? '+' : ''}${spend.pct_change}%`}
                >
                  <span className="ops-spend-amount">${Math.round((spend.last_30 || 0) / 1000)}k</span>
                  <span className="ops-spend-change">{spend.pct_change > 0 ? '+' : ''}{spend.pct_change}%</span>
                </div>
                {showSpend && spend.breakdown && (
                  <div className="ops-spend-dropdown">
                    <div className="ops-spend-dropdown-header">
                      <span>30-Day Spend Breakdown</span>
                      <span className="ops-spend-close" onClick={() => setShowSpend(false)}>x</span>
                    </div>
                    <div className="ops-spend-totals">
                      <div>Last 30d: <strong>${spend.last_30?.toLocaleString()}</strong> ({spend.last_30_dbus?.toLocaleString()} DBUs)</div>
                      <div>Prior 30d: <strong>${spend.prior_30?.toLocaleString()}</strong> ({spend.prior_30_dbus?.toLocaleString()} DBUs)</div>
                    </div>
                    <table className="ops-table ops-spend-table">
                      <thead><tr><th>SKU</th><th>Last 30d</th><th>Prior 30d</th><th>Delta</th></tr></thead>
                      <tbody>
                        {spend.breakdown.map((row, i) => (
                          <tr key={i}>
                            <td>{row.sku_name.replace(/PREMIUM_/g, '').replace(/_US_EAST/g, '').replace(/_/g, ' ')}</td>
                            <td className="tabnum">${row.last_30_cost?.toLocaleString()}</td>
                            <td className="tabnum">${row.prior_30_cost?.toLocaleString()}</td>
                            <td className={`tabnum ${row.delta > 0 ? 'ops-delta-up' : 'ops-delta-down'}`}>
                              {row.delta > 0 ? '+' : ''}${row.delta?.toLocaleString()}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>
            )}
          </div>
        </div>
      </div>

      {error && <div className="ops-error">ERROR: {error}</div>}

      {/* KPI Cards */}
      <div className="ops-kpi-row">
        <div className="ops-kpi-card" data-status={summary?.freshness?.status || 'ok'}>
          <div className="ops-kpi-label">Data Freshness</div>
          <div className="ops-kpi-value">{summary ? `${summary.freshness.ok}/${summary.freshness.total}` : '\u2014/\u2014'}</div>
          <div className="ops-kpi-detail">ongoing firms within SLA</div>
          <KpiBar items={freshnessBarItems} okCount={freshnessOk} />
          <KpiTooltip items={summary?.freshness?.failing} label="STALE FIRMS" />
        </div>
        <div className="ops-kpi-card" data-status={summary?.jobs?.status || 'ok'}>
          <div className="ops-kpi-label">Job Health</div>
          <div className="ops-kpi-value">{summary ? `${summary.jobs.healthy}/${summary.jobs.total}` : '\u2014/\u2014'}</div>
          <div className="ops-kpi-detail">jobs passing (24h)</div>
          <KpiBar items={jobsBarItems} okCount={jobsOk} />
          <KpiTooltip items={summary?.jobs?.failing} label="FAILING JOBS" />
        </div>
        <div className="ops-kpi-card" data-status={summary?.gold?.status || 'ok'}>
          <div className="ops-kpi-label">Gold Layer</div>
          <div className="ops-kpi-value">{summary ? `${summary.gold.fresh}/${summary.gold.total}` : '\u2014/\u2014'}</div>
          <div className="ops-kpi-detail">firms with fresh data (&le;7d)</div>
          <KpiBar items={goldBarItems} okCount={goldOk} />
          <KpiTooltip items={summary?.gold?.stale} label="STALE FIRMS" />
        </div>
        <div className="ops-kpi-card" data-status="ok">
          <div className="ops-kpi-label">Pipeline</div>
          <div className="ops-kpi-value">{summary ? summary.pipeline.total_runs.toLocaleString() : '\u2014'}</div>
          <div className="ops-kpi-detail">
            total syncs | {summary ? summary.pipeline.firms_monitored : '\u2014'} firms | {summary ? summary.pipeline.total_failed.toLocaleString() : '\u2014'} failures
          </div>
        </div>
      </div>

      {/* Gold Layer Freshness */}
      <div className="ops-section">
        <div className="ops-section-header">
          <span className="wf-section-number">1</span>
          <div>
            <h2 className="wf-section-title">Gold Layer Freshness</h2>
            <p className="wf-section-subtitle">Latest data date per firm in sandbox.gold</p>
          </div>
        </div>
        <div className="wf-card">
          {!goldFreshness ? <div className="wf-loading"><div className="wf-spinner" />Loading</div> : goldFreshness.length === 0 ? <div style={{ padding: 20, color: '#6b7280' }}>No gold data</div> : (
            <div className="wf-table-wrap">
              <table className="wf-data-table">
                <thead><tr><th></th><th>Firm</th><th>Latest Data</th><th>Days Stale</th><th>Rows</th><th>Status</th></tr></thead>
                <tbody>
                  {goldFreshness.map((row, i) => (
                    <tr key={i} className={row.freshness_status === 'error' ? 'ops-row-error' : row.freshness_status === 'warn' ? 'ops-row-warn' : ''}>
                      <td><StatusDot status={row.freshness_status} /></td>
                      <td style={{ fontWeight: 700 }}>{row.firm}</td>
                      <td>{row.latest_data_date}</td>
                      <td>{row.days_stale}d</td>
                      <td className="tabnum">{row.row_count?.toLocaleString()}</td>
                      <td><StatusTag status={row.freshness_status} /></td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </div>

      {/* Heartbeat */}
      <div className="ops-section">
        <div className="ops-section-header">
          <span className="wf-section-number">2</span>
          <div>
            <h2 className="wf-section-title">Ingestion Heartbeat</h2>
            <p className="wf-section-subtitle">Auto-refresh 60s</p>
          </div>
        </div>
        <div className="wf-card">
          {!heartbeat ? <div className="wf-loading"><div className="wf-spinner" />Loading</div> : heartbeat.length === 0 ? <div style={{ padding: 20, color: '#6b7280' }}>No ingestion data</div> : (
            <div className="wf-table-wrap">
              <table className="wf-data-table">
                <thead><tr><th></th><th>Firm</th><th>System</th><th>Last Sync</th><th>Mode</th><th>Runs</th><th>Status</th></tr></thead>
                <tbody>
                  {heartbeat.map((row, i) => (
                    <tr key={i} className={row.freshness_status === 'error' ? 'ops-row-error' : row.freshness_status === 'warn' ? 'ops-row-warn' : row.freshness_status === 'historical' ? 'ops-row-historical' : ''}>
                      <td><StatusDot status={row.freshness_status} /></td>
                      <td style={{ fontWeight: 700 }}>{row.firm}</td>
                      <td>{row.system}</td>
                      <td>{formatAge(row.hours_since_last)}</td>
                      <td>{(row.latest_mode || '\u2014').toUpperCase()}</td>
                      <td className="tabnum">{row.total_runs?.toLocaleString()}</td>
                      <td><StatusTag status={row.freshness_status} /></td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </div>

      {/* Jobs */}
      <div className="ops-section">
        <div className="ops-section-header">
          <span className="wf-section-number">3</span>
          <div>
            <h2 className="wf-section-title">Databricks Jobs (24h)</h2>
            <p className="wf-section-subtitle">Source: system.lakeflow</p>
          </div>
        </div>
        <div className="wf-card">
          {!jobs ? <div className="wf-loading"><div className="wf-spinner" />Loading</div> : jobs.length === 0 ? <div style={{ padding: 20, color: '#6b7280' }}>No job runs in last 24h</div> : (
            <div className="wf-table-wrap">
              <table className="wf-data-table">
                <thead><tr><th></th><th>Job</th><th>Runs</th><th>Passed</th><th>Failed</th><th>Error %</th></tr></thead>
                <tbody>
                  {jobs.map((row, i) => {
                    const status = row.error_pct === 0 ? 'ok' : row.error_pct < 15 ? 'warn' : 'error';
                    return (
                      <tr key={i} className={status === 'error' ? 'ops-row-error' : status === 'warn' ? 'ops-row-warn' : ''} title={row.last_error ? `Last error: ${row.last_error}` : ''}>
                        <td><StatusDot status={status} /></td>
                        <td style={{ fontWeight: 700 }}>{row.job_name}</td>
                        <td className="tabnum">{row.total_runs}</td>
                        <td className="tabnum">{row.passed}</td>
                        <td className="tabnum">{row.failed}</td>
                        <td><ErrorBar pct={row.error_pct || 0} /></td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </div>

      {/* Footer */}
      <footer className="wf-footer">
        Data Ops HQ v2.0 | {heartbeat?.length || 0} ingestion | {goldFreshness?.length || 0} gold | {jobs?.length || 0} jobs
        {lastRefresh && <span> | Last refresh: {lastRefresh.toLocaleTimeString()}</span>}
      </footer>
    </div>
  );
}
