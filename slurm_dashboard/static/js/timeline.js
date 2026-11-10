// Timeline: when each job ran. One lane per job name; overlapping runs stack.

import { $, bindTips, fmtDuration, fmtTime, hideTip, html, setHTML, stateLabel, store } from './util.js';
import { api } from './api.js';
import { on, selectJob, setView, state } from './state.js';

const RANGES = [[1, '24 hours'], [3, '3 days'], [7, '7 days'], [14, '14 days'], [30, '30 days']];
const TICK_HOURS = { 1: 3, 3: 12, 7: 24, 14: 48, 30: 120 };
const LEGEND = [['running', 'Running'], ['completed', 'Completed'], ['failed', 'Failed'], ['timeout', 'Timed out'], ['cancelled', 'Cancelled']];
const BAR = 10;
const GAP = 3;
const PAD = 9;
const MAX_LANES = 8;
const MAX_GROUPS = 60;

let root;
let days = store.get('timeline.days', 7);
let jobs = [];
let byId = new Map();
let error = null;
let loadedAt = 0;
let loading = false;

async function load() {
    if (loading) return;
    loading = true;
    root.classList.add('loading');
    try {
        const data = await api.get(`/api/job_history?days=${days}&limit=2000`);
        jobs = data.jobs || [];
        error = data.error || null;
    } catch (err) {
        error = err.message;
    }
    loadedAt = Date.now();
    loading = false;
    root.classList.remove('loading');
    render();
}

function ticks(t0, t1) {
    const step = TICK_HOURS[days] * 3600 * 1000;
    const first = new Date(t0);
    first.setHours(0, 0, 0, 0);
    const out = [];
    for (let t = first.getTime(); t <= t1; t += step) {
        if (t < t0) continue;
        const date = new Date(t);
        const midnight = date.getHours() === 0;
        const label = step < 86400000 && !midnight
            ? date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
            : date.toLocaleDateString([], { day: 'numeric', month: 'short' });
        out.push({ at: ((t - t0) / (t1 - t0)) * 100, label, strong: midnight && step < 86400000 });
    }
    return out;
}

/** Group runs by job name and pack overlapping runs into as few lanes as possible. */
function layout(t0, t1) {
    const groups = new Map();
    for (const job of jobs) {
        const start = Date.parse(job.start);
        const end = job.end ? Date.parse(job.end) : t1;
        if (isNaN(start) || end < t0) continue;
        const key = job.name || '(unnamed)';
        if (!groups.has(key)) groups.set(key, { name: key, bars: [], latest: 0 });
        const group = groups.get(key);
        group.bars.push({ job, start: Math.max(start, t0), end: Math.min(Math.max(end, start), t1) });
        group.latest = Math.max(group.latest, start);
    }
    const ordered = [...groups.values()].sort((a, b) => b.latest - a.latest);
    for (const group of ordered) {
        group.bars.sort((a, b) => a.start - b.start);
        const laneEnds = [];
        for (const bar of group.bars) {
            let lane = laneEnds.findIndex(end => end <= bar.start);
            if (lane === -1) {
                if (laneEnds.length < MAX_LANES) lane = laneEnds.length;
                else lane = laneEnds.indexOf(Math.min(...laneEnds));
            }
            laneEnds[lane] = bar.end;
            bar.lane = lane;
        }
        group.lanes = Math.max(1, laneEnds.length);
    }
    return ordered;
}

function render() {
    const t1 = Date.now();
    const t0 = t1 - days * 86400 * 1000;
    const span = t1 - t0;
    const groups = layout(t0, t1);
    byId = new Map(jobs.map(job => [job.id, job]));
    const axis = ticks(t0, t1);
    const shown = groups.slice(0, MAX_GROUPS);
    const scope = state.meta.all_users ? 'every user' : state.meta.user || 'you';

    setHTML(root, html`
        <div class="page-head">
            <div>
                <h1>Timeline</h1>
                <p class="page-sub">When jobs of ${scope} ran. One lane per job name; runs that overlap stack.</p>
            </div>
            <div class="seg" role="group" aria-label="Time range">
                ${RANGES.map(([value, label]) => html`<button class="${value === days ? 'on' : ''}" data-days="${value}">${label}</button>`)}
            </div>
        </div>
        ${error ? html`<p class="notice">${error}</p>` : ''}
        ${shown.length ? html`
            <div class="legend">${LEGEND.map(([key, label]) => html`<span><i class="sw s-${key}"></i>${label}</span>`)}</div>
            <div class="tl">
                <div class="tl-axis">
                    <div class="tl-label"></div>
                    <div class="tl-plot">${axis.map(tick => html`<span class="tl-tick ${tick.strong ? 'strong' : ''}" style="left:${tick.at.toFixed(3)}%">${tick.label}</span>`)}</div>
                </div>
                <div class="tl-body">
                    <div class="tl-grid">
                        ${axis.map(tick => html`<i style="left:${tick.at.toFixed(3)}%"></i>`)}
                        <i class="now" style="left:100%"></i>
                    </div>
                    ${shown.map(group => html`
                        <div class="tl-row">
                            <div class="tl-label" title="${group.name}">
                                <span class="tl-name">${group.name}</span>
                                <span class="tl-n">${group.bars.length} run${group.bars.length === 1 ? '' : 's'}</span>
                            </div>
                            <div class="tl-plot" style="height:${group.lanes * (BAR + GAP) - GAP + 2 * PAD}px">
                                ${group.bars.map(bar => html`<button class="bar s-${bar.job.state_category}" data-id="${bar.job.id}"
                                    style="left:${(((bar.start - t0) / span) * 100).toFixed(3)}%;width:${(((bar.end - bar.start) / span) * 100).toFixed(3)}%;top:${PAD + bar.lane * (BAR + GAP)}px"
                                    aria-label="${bar.job.name} ${bar.job.id}, ${stateLabel(bar.job.state)}"></button>`)}
                            </div>
                        </div>`)}
                </div>
            </div>
            ${groups.length > shown.length ? html`<p class="page-sub">${groups.length - shown.length} more job names are not shown. Pick a shorter range to see them.</p>` : ''}
        ` : html`<p class="empty">${loadedAt ? 'No jobs started in this period.' : 'Loading…'}</p>`}`);
}

export function initTimeline() {
    root = $('#view-timeline');
    root.addEventListener('click', event => {
        const range = event.target.closest('[data-days]');
        if (range) {
            days = Number(range.dataset.days);
            store.set('timeline.days', days);
            render();
            return load();
        }
        const bar = event.target.closest('.bar');
        if (bar) {
            hideTip();
            setView('jobs');
            selectJob(bar.dataset.id, { tab: null });
        }
    });
    bindTips(root, '.bar', bar => {
        const job = byId.get(bar.dataset.id);
        if (!job) return '';
        const end = job.end ? fmtTime(job.end) : 'now';
        const seconds = ((job.end ? Date.parse(job.end) : Date.now()) - Date.parse(job.start)) / 1000;
        return html`
            <div class="tip-title">${job.name}</div>
            <div class="tip-row"><i class="sw s-${job.state_category}"></i><strong>${stateLabel(job.state)}</strong><span>${fmtDuration(seconds)}</span></div>
            <div class="tip-sub">${fmtTime(job.start)} → ${end}</div>
            <div class="tip-sub mono">${job.id}${job.partition ? ` · ${job.partition}` : ''}${state.meta.all_users && job.user ? ` · ${job.user}` : ''}</div>`;
    });
    on('view', () => {
        if (state.view === 'timeline') {
            render();
            load();
        }
    });
    on('jobs', () => {
        if (state.view === 'timeline' && Date.now() - loadedAt > 60000) load();
    });
}
