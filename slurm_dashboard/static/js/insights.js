// Insights: outcomes per day, how well resource requests fit, and where time goes.

import { $, bindTips, fmtDuration, fmtNumber, fmtPercent, html, setHTML, store } from './util.js';
import { api } from './api.js';
import { on, setFilter, setView, state } from './state.js';

const PERIODS = [[7, '7 days'], [30, '30 days'], [90, '90 days']];
// Stacking order from the baseline up; the legend follows the same order.
const OUTCOMES = [
    ['completed', 'Completed'],
    ['failed', 'Failed'],
    ['timeout', 'Timed out'],
    ['cancelled', 'Cancelled'],
    ['running', 'Still running'],
];

let root;
let days = store.get('insights.days', 30);
let data = null;
let error = null;
let loadedAt = 0;

async function load() {
    root.classList.add('loading');
    try {
        data = await api.get(`/api/insights?days=${days}`);
        error = data.error || null;
    } catch (err) {
        error = err.message;
    }
    loadedAt = Date.now();
    root.classList.remove('loading');
    render();
}

/** Round a maximum up to 1, 2 or 5 times a power of ten, so the axis reads cleanly. */
function niceMax(value) {
    if (value <= 4) return 4;
    const power = 10 ** Math.floor(Math.log10(value));
    for (const step of [1, 2, 5, 10]) if (value <= step * power) return step * power;
    return 10 * power;
}

const dayText = (iso, options) => new Date(`${iso}T12:00:00`).toLocaleDateString([], options);

function dailyChart(daily) {
    const present = OUTCOMES.filter(([key]) => daily.some(day => day[key] > 0));
    const top = niceMax(Math.max(...daily.map(day => day.total), 1));
    const every = Math.ceil(daily.length / 8);
    return html`
        <div class="legend">${present.map(([key, label]) => html`<span><i class="sw s-${key}"></i>${label}</span>`)}</div>
        <div class="chart" role="img" aria-label="Jobs per day by outcome. The same numbers are in the table below.">
            <div class="chart-y"><span>${top}</span><span>${top / 2}</span><span>0</span></div>
            <div class="chart-plot">
                <div class="chart-grid"><i></i><i></i><i></i></div>
                ${daily.map((day, index) => html`
                    <div class="col" data-day="${index}" tabindex="0" aria-label="${dayText(day.date, { day: 'numeric', month: 'long' })}: ${day.total} jobs">
                        <div class="stack" style="height:${((day.total / top) * 100).toFixed(2)}%">
                            ${present.filter(([key]) => day[key] > 0).map(([key]) => html`<i class="s-${key}" style="flex-grow:${day[key]}"></i>`)}
                        </div>
                    </div>`)}
            </div>
            <div class="chart-x">
                ${daily.map((day, index) => html`<span>${(daily.length - 1 - index) % every === 0 ? dayText(day.date, { day: 'numeric', month: 'short' }) : ''}</span>`)}
            </div>
        </div>
        <details class="as-table">
            <summary>Show as table</summary>
            <table class="table">
                <thead><tr><th>Day</th>${present.map(([, label]) => html`<th class="num">${label}</th>`)}<th class="num">Total</th></tr></thead>
                <tbody>${daily.filter(day => day.total).reverse().map(day => html`
                    <tr><td>${dayText(day.date, { weekday: 'short', day: 'numeric', month: 'short' })}</td>${present.map(([key]) => html`<td class="num">${day[key] || ''}</td>`)}<td class="num">${day.total}</td></tr>`)}
                </tbody>
            </table>
        </details>`;
}

function fit(label, value, sentence, recommendation) {
    if (value === null || value === undefined) return '';
    return html`
        <div class="fit">
            <div class="fit-head"><span class="fit-label">${label}</span><span class="fit-val">${fmtPercent(value)}</span></div>
            <span class="m-bar"><i style="width:${Math.max(1, Math.min(100, value)).toFixed(1)}%"></i></span>
            <p>${sentence}</p>
            ${recommendation ? html`<p class="rec">${recommendation}</p>` : ''}
        </div>`;
}

function outcomeBar(row) {
    const parts = [['completed', row.completed], ['failed', row.failed], ['cancelled', row.cancelled]].filter(([, n]) => n > 0);
    const title = parts.map(([key, n]) => `${n} ${key}`).join(', ');
    return html`<span class="split" title="${title}">${parts.map(([key, n]) => html`<i class="s-${key}" style="flex-grow:${n}"></i>`)}</span>`;
}

function breakdown(title, note, rows, field, extended) {
    if (!rows.length) return '';
    return html`
        <section class="block">
            <h2>${title}</h2>
            <p class="page-sub">${note}</p>
            <div class="table-wrap">
                <table class="table rows-link">
                    <thead><tr>
                        <th>${field === 'name' ? 'Job name' : 'Partition'}</th>
                        <th class="num">Runs</th>
                        <th>Outcome</th>
                        <th class="num">Succeeded</th>
                        ${extended ? html`<th class="num">Typical runtime</th><th class="num">CPU used</th><th class="num">Memory used</th>` : ''}
                        <th class="num">CPU hours</th>
                        <th class="num">GPU hours</th>
                    </tr></thead>
                    <tbody>${rows.map(row => html`
                        <tr data-filter="${field}:${row.key.replace(/\s+/g, '*')}" tabindex="0" title="Show these jobs">
                            <td class="strong">${row.key}</td>
                            <td class="num">${row.runs.toLocaleString()}</td>
                            <td>${outcomeBar(row)}</td>
                            <td class="num">${fmtPercent(row.success_rate)}</td>
                            ${extended ? html`
                                <td class="num">${row.median_runtime_seconds ? fmtDuration(row.median_runtime_seconds) : '–'}</td>
                                <td class="num">${fmtPercent(row.cpu_eff)}</td>
                                <td class="num">${fmtPercent(row.mem_eff)}</td>` : ''}
                            <td class="num">${fmtNumber(row.cpu_hours)}</td>
                            <td class="num">${row.gpu_hours ? fmtNumber(row.gpu_hours) : '–'}</td>
                        </tr>`)}
                    </tbody>
                </table>
            </div>
        </section>`;
}

function render() {
    const scope = state.meta.all_users ? 'all users' : state.meta.user || 'you';
    const head = html`
        <div class="page-head">
            <div>
                <h1>Insights</h1>
                <p class="page-sub">Finished jobs of ${scope} in the last ${days} days.</p>
            </div>
            <div class="seg" role="group" aria-label="Period">
                ${PERIODS.map(([value, label]) => html`<button class="${value === days ? 'on' : ''}" data-days="${value}">${label}</button>`)}
            </div>
        </div>
        ${error ? html`<p class="notice">${error}</p>` : ''}`;

    const stats = data && data.job_stats;
    if (!stats) {
        setHTML(root, html`${head}<p class="empty">${loadedAt ? 'No finished jobs in this period yet.' : 'Loading…'}</p>`);
        return;
    }
    const score = data.efficiency_score || {};
    const memory = data.memory_insights;
    const time = data.time_insights;
    const problems = [
        stats.failed ? `${stats.failed.toLocaleString()} failed` : '',
        stats.timeout ? `${stats.timeout.toLocaleString()} timed out` : '',
        stats.cancelled ? `${stats.cancelled.toLocaleString()} cancelled` : '',
    ].filter(Boolean).join(', ');

    setHTML(root, html`
        ${head}
        <div class="tiles">
            <div class="tile"><span class="tile-label">Jobs finished</span><span class="tile-val">${stats.total_jobs.toLocaleString()}</span></div>
            <div class="tile"><span class="tile-label">Succeeded</span><span class="tile-val">${fmtPercent(stats.success_rate)}</span><span class="tile-note">${problems || 'none failed'}</span></div>
            <div class="tile"><span class="tile-label">CPU hours</span><span class="tile-val">${fmtNumber(data.usage.cpu_hours)}</span></div>
            ${data.usage.gpu_hours ? html`<div class="tile"><span class="tile-label">GPU hours</span><span class="tile-val">${fmtNumber(data.usage.gpu_hours)}</span></div>` : ''}
        </div>

        <section class="block">
            <h2>Jobs per day</h2>
            <p class="page-sub">Counted on the day a job started, coloured by how it ended.</p>
            ${dailyChart(data.daily)}
        </section>

        <section class="block">
            <h2>Do requests fit what jobs use?</h2>
            <p class="page-sub">Averages over ${(score.jobs_analyzed || 0).toLocaleString()} completed jobs. Low values mean resources were reserved but left idle.</p>
            <div class="fits">
                ${fit('CPU', score.cpu_efficiency, 'Share of the allocated cores that was actually busy.', '')}
                ${fit('Memory', score.memory_efficiency, memory ? `Peak use against the request. Median request ${memory.median_requested}, median peak ${memory.median_used}.` : 'Peak use against the request.', memory && memory.recommendation)}
                ${fit('Time limit', score.time_efficiency, time ? `Runtime against the limit. 90% of jobs finish within ${time.p90_runtime}; the median limit is ${time.median_limit}.` : 'Runtime against the limit.', time && time.recommendation)}
            </div>
        </section>

        ${breakdown('By job name', 'The most frequent job names. Select a row to see those jobs.', data.by_name, 'name', true)}
        ${data.by_partition.length > 1 ? breakdown('By partition', 'Select a row to see those jobs.', data.by_partition, 'partition', false) : ''}`);
}

export function initInsights() {
    root = $('#view-insights');
    const openRow = target => {
        const rowEl = target.closest('[data-filter]');
        if (!rowEl) return false;
        setFilter({ query: rowEl.dataset.filter, stateFilter: '' });
        setView('jobs');
        return true;
    };
    root.addEventListener('click', event => {
        const period = event.target.closest('[data-days]');
        if (period) {
            days = Number(period.dataset.days);
            store.set('insights.days', days);
            render();
            return load();
        }
        openRow(event.target);
    });
    root.addEventListener('keydown', event => {
        if (event.key === 'Enter' && openRow(event.target)) event.preventDefault();
    });
    bindTips(root, '.col', col => {
        const day = data.daily[Number(col.dataset.day)];
        return html`
            <div class="tip-title">${dayText(day.date, { weekday: 'long', day: 'numeric', month: 'long' })}</div>
            ${OUTCOMES.filter(([key]) => day[key] > 0).map(([key, label]) => html`<div class="tip-row"><i class="sw s-${key}"></i><strong>${day[key]}</strong><span>${label}</span></div>`)}
            ${day.total ? html`<div class="tip-sub">${day.total} started in total</div>` : html`<div class="tip-sub">No jobs started</div>`}`;
    });
    on('view', () => {
        if (state.view === 'insights') {
            render();
            load();
        }
    });
    on('jobs', () => {
        if (state.view === 'insights' && Date.now() - loadedAt > 120000) load();
    });
}
