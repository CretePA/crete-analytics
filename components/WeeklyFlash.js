'use client';
import React, { useState, useEffect, useCallback } from 'react';
import './weeklyflash.css';

/* ========================================================================
   UTILITY FUNCTIONS (ported from Domo vanilla JS)
   ======================================================================== */

function fmt(n) {
  if (n == null || isNaN(n)) return '$0';
  const abs = Math.abs(n);
  const sign = n < 0 ? '-' : '';
  if (abs >= 1e9) return sign + '$' + (abs / 1e9).toFixed(1) + 'B';
  if (abs >= 1e6) return sign + '$' + (abs / 1e6).toFixed(1) + 'M';
  if (abs >= 1e3) return sign + '$' + (abs / 1e3).toFixed(0) + 'K';
  return sign + '$' + Math.round(abs);
}

function fmtFull(n) {
  if (n == null || isNaN(n)) return '$0';
  const sign = n < 0 ? '-' : '';
  const abs = Math.abs(Math.round(n));
  const str = abs.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ',');
  return sign + '$' + str;
}

function fmtNum(n) {
  if (n == null || isNaN(n)) return '0';
  return Math.round(n).toString().replace(/\B(?=(\d{3})+(?!\d))/g, ',');
}

function pct(n) {
  if (n == null || isNaN(n) || !isFinite(n)) return 'N/A';
  const sign = n > 0 ? '+' : '';
  return sign + n.toFixed(1) + '%';
}

function getRYG(pctChange) {
  if (pctChange == null || isNaN(pctChange) || !isFinite(pctChange))
    return { color: '#6b7280', bgClass: '', label: 'N/A', icon: '\u2014' };
  if (pctChange > 0)
    return { color: '#1a7a3a', bgClass: 'badge-green', label: 'Ahead', icon: '\u25B2' };
  if (pctChange >= -5)
    return { color: '#b8860b', bgClass: 'badge-yellow', label: 'Flat', icon: '\u25C6' };
  return { color: '#c0392b', bgClass: 'badge-red', label: 'Behind', icon: '\u25BC' };
}

function pctChange(current, prior) {
  if (!prior || prior === 0) return null;
  return ((current - prior) / Math.abs(prior)) * 100;
}

function num(v) { const n = Number(v); return isNaN(n) ? 0 : n; }

function lastFridayStr() {
  const d = new Date();
  const day = d.getDay();
  let diff = (day + 2) % 7;
  if (diff === 0 && d.getHours() >= 17) diff = 0;
  else if (diff === 0) diff = 7;
  d.setDate(d.getDate() - diff);
  return d.toISOString().split('T')[0];
}

function fmtDate(str) {
  const d = new Date(str + 'T12:00:00');
  const months = ['January','February','March','April','May','June','July','August','September','October','November','December'];
  return months[d.getMonth()] + ' ' + d.getDate() + ', ' + d.getFullYear();
}

/* ========================================================================
   SUB-COMPONENTS
   ======================================================================== */

function GaugeSvg({ ratio, color }) {
  const arcLen = 213.6;
  const fill = Math.min(Math.max(ratio, 0), 1) * arcLen;
  return (
    <svg viewBox="0 0 160 90" width="160" height="90">
      <path d="M 12 82 A 68 68 0 0 1 148 82" fill="none" stroke="#e5e1dc" strokeWidth="12" strokeLinecap="round"/>
      <path d="M 12 82 A 68 68 0 0 1 148 82" fill="none" stroke={color} strokeWidth="12" strokeLinecap="round"
        strokeDasharray={`${fill} ${arcLen}`}/>
    </svg>
  );
}

function GaugeCard({ label, current, pyMtd, pyFull, pctChg, periodLabel, isCurrency }) {
  const ryg = getRYG(pctChg);
  const ratio = pyFull ? num(current) / Math.abs(num(pyFull)) : 0;
  const fmtVal = isCurrency ? fmt : fmtNum;
  return (
    <div className="wf-gauge-card">
      <div className="wf-gauge-label">{label}</div>
      <div className="wf-gauge-pct" style={{ color: ryg.color }}>
        {pct(pctChg)} <span className="wf-gauge-pct-suffix">vs PY</span>
      </div>
      <div className="wf-gauge-svg-wrap">
        <GaugeSvg ratio={ratio} color={ryg.color} />
        <div className="wf-gauge-inner">
          <div className="wf-gauge-py-label">PY {periodLabel}</div>
          <div className="wf-gauge-py-value">{fmtVal(num(pyMtd))}</div>
          <div className="wf-gauge-current-value">{fmtVal(num(current))}</div>
        </div>
      </div>
      <div className="wf-gauge-period" style={{ color: ryg.color }}>CY {periodLabel}</div>
      <div className="wf-gauge-full-value">{fmtVal(num(pyFull))}</div>
      <div className="wf-gauge-full-label">PY Full {periodLabel === 'MTD' ? 'Month' : 'Year'}</div>
    </div>
  );
}

function TvpHero({ data }) {
  if (!data) return null;
  const mtdPct = pctChange(num(data.cy_mtd_tvp), num(data.py_mtd_tvp));
  const ytdPct = pctChange(num(data.cy_ytd_tvp), num(data.py_ytd_tvp));
  const mtdRyg = getRYG(mtdPct);
  const ytdRyg = getRYG(ytdPct);
  const badgeBg = (ryg) => ryg.bgClass === 'badge-green' ? '#e6f4ea' : ryg.bgClass === 'badge-yellow' ? '#fef9e7' : '#fdecea';

  return (
    <div className="wf-tvp-hero">
      <div>
        <div className="wf-tvp-label">Total Value of Production</div>
        <div className="wf-tvp-sublabel">Billable Hours &times; Billable Rate</div>
      </div>
      <div className="wf-tvp-divider" />
      <div className="wf-tvp-period">
        <div className="wf-tvp-period-label">MTD</div>
        <div className="wf-tvp-value">{fmt(num(data.cy_mtd_tvp))}</div>
        <div className="wf-tvp-py">PY MTD: {fmt(num(data.py_mtd_tvp))}</div>
      </div>
      <div className="wf-tvp-divider" />
      <div className="wf-tvp-period">
        <div className="wf-tvp-period-label">YTD</div>
        <div className="wf-tvp-value">{fmt(num(data.cy_ytd_tvp))}</div>
        <div className="wf-tvp-py">PY YTD: {fmt(num(data.py_ytd_tvp))}</div>
      </div>
      <div className="wf-tvp-divider" />
      <div className="wf-tvp-status">
        {[{ label: 'MTD', ryg: mtdRyg, val: mtdPct }, { label: 'YTD', ryg: ytdRyg, val: ytdPct }].map(s => (
          <div className="wf-tvp-status-row" key={s.label}>
            <span className="wf-tvp-status-label">{s.label}</span>
            <span className="wf-tvp-status-badge" style={{ background: badgeBg(s.ryg), color: s.ryg.color }}>
              {s.ryg.icon} {pct(s.val)}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}

function PerfTable({ rows, label }) {
  if (!rows) return null;
  return (
    <div className="wf-perf-section">
      <div className="wf-sub-label">{label}</div>
      <div className="wf-table-wrap">
        <table className="wf-data-table">
          <thead><tr><th>Metric</th><th>CY</th><th>PY</th><th>% Chg</th><th>Status</th></tr></thead>
          <tbody>
            {rows.map((r, i) => {
              const ryg = getRYG(r.pctChg);
              const fmtFn = r.isCurrency ? fmtFull : (r.isPercent ? (v => v != null && isFinite(v) ? v.toFixed(1) + '%' : 'N/A') : fmtNum);
              const curStr = r.isRate ? (r.current != null ? '$' + r.current.toFixed(0) : 'N/A') : fmtFn(r.current);
              const priStr = r.isRate ? (r.prior != null ? '$' + r.prior.toFixed(0) : 'N/A') : fmtFn(r.prior);
              return (
                <tr key={i} className={r.metric === 'TVP' ? 'wf-tvp-row' : ''}>
                  <td>{r.metric}</td>
                  <td className="tabnum">{curStr}</td>
                  <td className="tabnum">{priStr}</td>
                  <td className="tabnum" style={{ color: ryg.color }}>{pct(r.pctChg)}</td>
                  <td><span className={`badge ${ryg.bgClass}`}>{ryg.icon} {ryg.label}</span></td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function UtilizationTable({ data }) {
  if (!data || data.length === 0) return <div className="wf-note-box">No utilization data available.</div>;
  const maxAvg = Math.max(...data.map(d => d.headcount > 0 ? d.total_hours / d.headcount : 0), 40);

  return (
    <div className="wf-table-wrap">
      <table className="wf-data-table">
        <thead><tr><th>Level</th><th>HC</th><th>Avg Hrs/Person</th><th>Avg TVP/Person</th><th>Avg Rate/Hr</th><th style={{ width: '28%' }}>Hours Distribution</th></tr></thead>
        <tbody>
          {data.map((d, i) => {
            const hc = d.headcount || 0;
            const avgHrs = hc > 0 ? d.total_hours / hc : 0;
            const avgTvp = hc > 0 ? d.total_tvp / hc : 0;
            const avgRate = d.total_hours > 0 ? d.total_tvp / d.total_hours : 0;
            const barPct = Math.min((avgHrs / maxAvg) * 100, 100);
            const barColor = avgHrs >= 30 ? '#1a7a3a' : avgHrs >= 20 ? '#c8964a' : '#6b7280';
            return (
              <tr key={i}>
                <td>{d.staff_level}</td>
                <td className="tabnum">{fmtNum(hc)}</td>
                <td className="tabnum fw700">{avgHrs.toFixed(1)}</td>
                <td className="tabnum">{fmt(avgTvp)}</td>
                <td className="tabnum">${avgRate.toFixed(0)}/hr</td>
                <td>
                  <div className="wf-util-bar-outer">
                    <div className="wf-util-bar-inner" style={{ width: barPct + '%', background: barColor }}>
                      <span className="wf-util-bar-text">{avgHrs.toFixed(1)}</span>
                    </div>
                  </div>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function ARAgingChart({ data }) {
  if (!data) return null;
  const buckets = [
    { label: 'Current', value: num(data.current_bucket), color: '#1a7a3a' },
    { label: '31-60', value: num(data.bucket_31_60), color: '#5cb85c' },
    { label: '61-90', value: num(data.bucket_61_90), color: '#c8964a' },
    { label: '91-120', value: num(data.bucket_91_120), color: '#b8860b' },
    { label: '120+', value: num(data.bucket_120_plus), color: '#c0392b' },
  ];
  const total = num(data.total);
  const maxVal = Math.max(...buckets.map(b => b.value), 1);

  return (
    <div>
      <div className="wf-sub-label">AR Aging Distribution</div>
      <div className="wf-ar-total">Total Outstanding: {fmtFull(total)}</div>
      <div className="wf-ar-bars">
        {buckets.map((b, i) => {
          const h = Math.max((b.value / maxVal) * 120, 2);
          return (
            <div className="wf-ar-bar-col" key={i}>
              <div className="wf-ar-bar-amount" style={{ color: b.color }}>{fmt(b.value)}</div>
              <div className="wf-ar-bar" style={{ height: h + 'px', background: b.color }} />
              <div className="wf-ar-bar-label">{b.label}</div>
            </div>
          );
        })}
      </div>
      <div className="wf-ar-proportion-bar">
        {buckets.map((b, i) => {
          const w = total > 0 ? (b.value / total * 100) : 0;
          return <div key={i} style={{ width: w + '%', background: b.color }} />;
        })}
      </div>
      <div className="wf-ar-proportion-pcts">
        {buckets.map((b, i) => {
          const w = total > 0 ? (b.value / total * 100) : 0;
          return <div key={i} style={{ width: w + '%' }}>{w >= 3 ? w.toFixed(0) + '%' : ''}</div>;
        })}
      </div>
    </div>
  );
}

function RiskTable({ items, type }) {
  const [search, setSearch] = useState('');
  if (!items || items.length === 0) return <div className="wf-risk-empty">No at-risk {type} items over 60 days.</div>;

  const total = items.reduce((s, it) => s + num(it.wip_amount || it.ar_amount), 0);
  const filtered = search
    ? items.filter(it => (it.client_name || '').toLowerCase().includes(search.toLowerCase()) || (it.partner_name || '').toLowerCase().includes(search.toLowerCase()))
    : items;

  return (
    <div>
      <div className="wf-risk-header">
        <span className="wf-risk-count">{items.length} item{items.length !== 1 ? 's' : ''} over 60 days</span>
        <span className="wf-risk-total">Total: {fmtFull(total)}</span>
      </div>
      <input className="wf-risk-search" placeholder="Search client or partner..." value={search} onChange={e => setSearch(e.target.value)} />
      <div className="wf-risk-list">
        {filtered.map((it, i) => {
          const amount = num(it.wip_amount || it.ar_amount);
          return (
            <div className="wf-risk-item" key={i}>
              <div className="wf-risk-item-top">
                <div className="wf-risk-client">{it.client_name}</div>
                <div className="wf-risk-item-right">
                  <span className="wf-risk-amount">{fmtFull(amount)}</span>
                </div>
              </div>
              {it.partner_name && <div className="wf-risk-detail">Partner: {it.partner_name}</div>}
            </div>
          );
        })}
      </div>
    </div>
  );
}

/* ========================================================================
   MAIN WEEKLY FLASH COMPONENT
   ======================================================================== */

function WeeklyFlash() {
  const [asOfDate, setAsOfDate] = useState(lastFridayStr());
  const [memberFirm, setMemberFirm] = useState('');
  const [firms, setFirms] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  // Data states
  const [timeData, setTimeData] = useState(null);
  const [invoiceData, setInvoiceData] = useState(null);
  const [cashData, setCashData] = useState(null);
  const [serviceLines, setServiceLines] = useState([]);
  const [utilization, setUtilization] = useState([]);
  const [arAging, setArAging] = useState(null);
  const [wipRisk, setWipRisk] = useState([]);
  const [arRisk, setArRisk] = useState([]);
  const [slExpanded, setSlExpanded] = useState(false);

  const fetchData = useCallback(async () => {
    setLoading(true);
    setError(null);
    const params = new URLSearchParams();
    if (asOfDate) params.set('as_of_date', asOfDate);
    if (memberFirm) params.set('member_firm', memberFirm);
    const qs = params.toString() ? '?' + params.toString() : '';

    try {
      const [timeRes, invRes, cashRes, slRes, utilRes, arAgingRes, wipRiskRes, arRiskRes] = await Promise.all([
        fetch('/api/flash/time-gauges' + qs).then(r => r.json()),
        fetch('/api/flash/invoice-gauges' + qs).then(r => r.json()),
        fetch('/api/flash/cash-gauges' + qs).then(r => r.json()),
        fetch('/api/flash/service-lines' + qs).then(r => r.json()),
        fetch('/api/flash/utilization' + qs).then(r => r.json()),
        fetch('/api/flash/ar-aging' + qs).then(r => r.json()),
        fetch('/api/flash/wip-risk' + qs).then(r => r.json()),
        fetch('/api/flash/ar-risk' + qs).then(r => r.json()),
      ]);
      setTimeData(timeRes.rows?.[0] || null);
      setInvoiceData(invRes.rows?.[0] || null);
      setCashData(cashRes.rows?.[0] || null);
      setServiceLines(slRes.rows || []);
      setUtilization(utilRes.rows || []);
      setArAging(arAgingRes.rows?.[0] || null);
      setWipRisk(wipRiskRes.rows || []);
      setArRisk(arRiskRes.rows || []);
    } catch (e) {
      setError(e.message || 'Failed to load data');
    } finally {
      setLoading(false);
    }
  }, [asOfDate, memberFirm]);

  useEffect(() => {
    fetch('/api/flash/firms').then(r => r.json()).then(d => setFirms(d.rows || [])).catch(() => {});
    fetchData();
  }, [fetchData]);

  // Build performance table rows
  const buildPerfRows = (period) => {
    if (!timeData || !invoiceData || !cashData) return [];
    const isMtd = period === 'MTD';
    const t = timeData, inv = invoiceData, c = cashData;
    const cyHrs = num(isMtd ? t.cy_mtd_hours : t.cy_ytd_hours);
    const pyHrs = num(isMtd ? t.py_mtd_hours : t.py_ytd_hours);
    const cyTvp = num(isMtd ? t.cy_mtd_tvp : t.cy_ytd_tvp);
    const pyTvp = num(isMtd ? t.py_mtd_tvp : t.py_ytd_tvp);
    const cyWuwd = num(isMtd ? t.cy_mtd_wuwd : t.cy_ytd_wuwd);
    const pyWuwd = num(isMtd ? t.py_mtd_wuwd : t.py_ytd_wuwd);
    const cyBill = num(isMtd ? inv.cy_mtd_billings : inv.cy_ytd_billings);
    const pyBill = num(isMtd ? inv.py_mtd_billings : inv.py_ytd_billings);
    const cyCash = num(isMtd ? c.cy_mtd_cash : c.cy_ytd_cash);
    const pyCash = num(isMtd ? c.py_mtd_cash : c.py_ytd_cash);
    const cyRate = cyHrs > 0 ? cyTvp / cyHrs : 0;
    const pyRate = pyHrs > 0 ? pyTvp / pyHrs : 0;
    const cyReal = cyTvp > 0 ? ((cyTvp + cyWuwd) / cyTvp) * 100 : 0;
    const pyReal = pyTvp > 0 ? ((pyTvp + pyWuwd) / pyTvp) * 100 : 0;

    return [
      { metric: 'Billable Hours', current: cyHrs, prior: pyHrs, pctChg: pctChange(cyHrs, pyHrs) },
      { metric: 'TVP', current: cyTvp, prior: pyTvp, pctChg: pctChange(cyTvp, pyTvp), isCurrency: true },
      { metric: 'Avg Billable Rate', current: cyRate, prior: pyRate, pctChg: pctChange(cyRate, pyRate), isRate: true },
      { metric: 'Billings', current: cyBill, prior: pyBill, pctChg: pctChange(cyBill, pyBill), isCurrency: true },
      { metric: 'Realization %', current: cyReal, prior: pyReal, pctChg: pctChange(cyReal, pyReal), isPercent: true },
      { metric: 'Collections', current: cyCash, prior: pyCash, pctChg: pctChange(cyCash, pyCash), isCurrency: true },
    ];
  };

  // Build gauge data
  const buildGauges = (period) => {
    if (!timeData || !invoiceData || !cashData) return [];
    const isMtd = period === 'MTD';
    const t = timeData, inv = invoiceData, c = cashData;
    return [
      { label: 'Hours', current: num(isMtd ? t.cy_mtd_hours : t.cy_ytd_hours), pyMtd: num(isMtd ? t.py_mtd_hours : t.py_ytd_hours), pyFull: num(isMtd ? t.py_full_month_hours : t.py_full_year_hours), pctChg: pctChange(num(isMtd ? t.cy_mtd_hours : t.cy_ytd_hours), num(isMtd ? t.py_mtd_hours : t.py_ytd_hours)) },
      { label: 'TVP', current: num(isMtd ? t.cy_mtd_tvp : t.cy_ytd_tvp), pyMtd: num(isMtd ? t.py_mtd_tvp : t.py_ytd_tvp), pyFull: num(isMtd ? t.py_full_month_tvp : t.py_full_year_tvp), pctChg: pctChange(num(isMtd ? t.cy_mtd_tvp : t.cy_ytd_tvp), num(isMtd ? t.py_mtd_tvp : t.py_ytd_tvp)), isCurrency: true },
      { label: 'Billings', current: num(isMtd ? inv.cy_mtd_billings : inv.cy_ytd_billings), pyMtd: num(isMtd ? inv.py_mtd_billings : inv.py_ytd_billings), pyFull: num(isMtd ? inv.py_full_month_billings : inv.py_full_year_billings), pctChg: pctChange(num(isMtd ? inv.cy_mtd_billings : inv.cy_ytd_billings), num(isMtd ? inv.py_mtd_billings : inv.py_ytd_billings)), isCurrency: true },
      { label: 'Collections', current: num(isMtd ? c.cy_mtd_cash : c.cy_ytd_cash), pyMtd: num(isMtd ? c.py_mtd_cash : c.py_ytd_cash), pyFull: num(isMtd ? c.py_full_month_cash : c.py_full_year_cash), pctChg: pctChange(num(isMtd ? c.cy_mtd_cash : c.cy_ytd_cash), num(isMtd ? c.py_mtd_cash : c.py_ytd_cash)), isCurrency: true },
    ];
  };

  const firmName = memberFirm ? (firms.find(f => f.member_firm_id === memberFirm)?.member_firm_name || memberFirm) : 'All Firms';

  return (
    <div className="wf-container">
      {/* Header with TVP hero inside */}
      <div className="wf-header">
        <div className="wf-header-top">
          <div>
            <div className="wf-header-subtitle">Weekly Flash</div>
            <div className="wf-header-title">Performance Scorecard</div>
            <div className="wf-header-meta">Week Ending: {fmtDate(asOfDate)} &mdash; {firmName}</div>
          </div>
          <div className="wf-header-controls">
            <select className="wf-select" value={memberFirm} onChange={e => setMemberFirm(e.target.value)}>
              <option value="">All Firms</option>
              {firms.map(f => <option key={f.member_firm_id} value={f.member_firm_id}>{f.member_firm_name}</option>)}
            </select>
            <input className="wf-date-input" type="date" value={asOfDate} onChange={e => setAsOfDate(e.target.value)} />
            <button className="wf-refresh-btn" onClick={fetchData} disabled={loading}>
              {loading ? 'Loading...' : 'Apply & Refresh'}
            </button>
          </div>
        </div>
        {!loading && timeData && <TvpHero data={timeData} />}
      </div>

      {/* Content area */}
      <div className="wf-content">
        {/* Error */}
        {error && <div className="wf-error">{error}</div>}

        {/* Loading */}
        {loading && <div className="wf-loading"><div className="wf-spinner" /><div>Loading Weekly Flash...</div></div>}

        {/* Content */}
        {!loading && !error && (
          <>
            {/* Section 1: Performance */}
            <section className="wf-section">
              <div className="wf-section-header">
                <span className="wf-section-number">1</span>
                <div>
                  <h2 className="wf-section-title">Performance vs. Prior Year</h2>
                  <p className="wf-section-subtitle">Green = beating PY, Yellow = flat to -5%, Red = below -5%.</p>
                </div>
              </div>

              {/* MTD Gauges */}
              <div className="wf-gauge-row">
                <div className="wf-gauge-row-header">Month-to-Date</div>
                <div className="wf-gauge-grid">
                  {buildGauges('MTD').map((g, i) => <GaugeCard key={i} {...g} periodLabel="MTD" />)}
                </div>
              </div>

              {/* YTD Gauges */}
              <div className="wf-gauge-row">
                <div className="wf-gauge-row-header">Year-to-Date</div>
                <div className="wf-gauge-grid">
                  {buildGauges('YTD').map((g, i) => <GaugeCard key={i} {...g} periodLabel="YTD" />)}
                </div>
              </div>

              {/* Performance Tables */}
              <div className="wf-card">
                <div className="wf-perf-grid">
                  <PerfTable rows={buildPerfRows('MTD')} label="Month-to-Date" />
                  <PerfTable rows={buildPerfRows('YTD')} label="Year-to-Date" />
                </div>

                {/* Service Line Toggle */}
                <div style={{ borderTop: '1px solid #e5e1dc', paddingTop: 16, marginTop: 8 }}>
                  <button className="wf-sl-toggle" onClick={() => setSlExpanded(!slExpanded)}>
                    {slExpanded ? '\u25BC' : '\u25B6'} Service Line Breakdown
                  </button>
                  {slExpanded && serviceLines.length > 0 && (
                    <div className="wf-table-wrap" style={{ marginTop: 16 }}>
                      <table className="wf-data-table">
                        <thead><tr><th>Service Line</th><th>CY MTD Hrs</th><th>PY MTD Hrs</th><th>% Chg</th><th>CY MTD TVP</th><th>PY MTD TVP</th><th>% Chg</th></tr></thead>
                        <tbody>
                          {serviceLines.map((r, i) => {
                            const hp = pctChange(num(r.cy_mtd_hours), num(r.py_mtd_hours));
                            const tp = pctChange(num(r.cy_mtd_tvp), num(r.py_mtd_tvp));
                            return (
                              <tr key={i}>
                                <td>{r.service_line}</td>
                                <td className="tabnum">{fmtNum(r.cy_mtd_hours)}</td>
                                <td className="tabnum">{fmtNum(r.py_mtd_hours)}</td>
                                <td className="tabnum" style={{ color: getRYG(hp).color }}>{pct(hp)}</td>
                                <td className="tabnum">{fmtFull(r.cy_mtd_tvp)}</td>
                                <td className="tabnum">{fmtFull(r.py_mtd_tvp)}</td>
                                <td className="tabnum" style={{ color: getRYG(tp).color }}>{pct(tp)}</td>
                              </tr>
                            );
                          })}
                        </tbody>
                      </table>
                    </div>
                  )}
                </div>
              </div>
            </section>

            {/* Section 1b: Utilization */}
            <section className="wf-section">
              <div className="wf-section-header">
                <span className="wf-section-number">1b</span>
                <div>
                  <h2 className="wf-section-title">Utilization &mdash; Avg Billable Hours by Level</h2>
                  <p className="wf-section-subtitle">Average billable hours per person per week.</p>
                </div>
              </div>
              <div className="wf-card">
                <UtilizationTable data={utilization} />
                <div className="wf-note-box">
                  <strong>Note:</strong> Average billable hours per person per week at each level.
                </div>
              </div>
            </section>

            {/* Section 2: Working Capital Risk */}
            <section className="wf-section">
              <div className="wf-section-header">
                <span className="wf-section-number">2</span>
                <div>
                  <h2 className="wf-section-title">Working Capital Risk</h2>
                  <p className="wf-section-subtitle">AR aging distribution, at-risk WIP and AR over 60 days.</p>
                </div>
              </div>

              <div className="wf-card">
                <ARAgingChart data={arAging} />
                <div className="wf-divider" />
                <div className="wf-sub-label">At-Risk WIP</div>
                <RiskTable items={wipRisk} type="WIP" />
              </div>

              <div className="wf-card" style={{ marginTop: 16 }}>
                <div className="wf-sub-label">At-Risk AR</div>
                <RiskTable items={arRisk} type="AR" />
              </div>
            </section>

            {/* Footer */}
            <footer className="wf-footer">
              {firmName} &middot; Generated {new Date().toLocaleString()}
            </footer>
          </>
        )}
      </div>
    </div>
  );
}

export default WeeklyFlash;
