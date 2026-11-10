// Entry point: wires the header, keeps job data fresh and routes keyboard shortcuts.

import { $, $$, fmtClock, store } from './util.js';
import { api } from './api.js';
import { emit, liveElapsed, on, readHash, selectJob, setFilter, setTab, setView, state, TABS } from './state.js';
import { initList, moveSelection, renderList } from './jobs.js';
import { detail, initDetail } from './detail.js';
import { initTimeline } from './timeline.js';
import { initPipelines } from './pipelines.js';
import { initInsights } from './insights.js';
import { openHelp, openSubmit, toast } from './dialogs.js';

const POLL_MS = 8000;
const NARROW = window.matchMedia('(max-width: 900px)');

// ---------------------------------------------------------------- status line

const banners = new Map();

function setBanner(key, message) {
    if (message) banners.set(key, message);
    else banners.delete(key);
    const el = $('#banner');
    el.hidden = banners.size === 0;
    el.textContent = [...banners.values()].join('  ·  ');
}

function setLive(ok, title) {
    const el = $('#live');
    el.classList.toggle('off', !ok);
    el.title = title;
    $('span', el).textContent = ok ? 'Live' : 'Not updating';
}

function updateTitle() {
    const running = state.active.filter(job => job.state_category === 'running').length;
    const pending = state.active.length - running;
    const host = state.meta.host ? ` · ${state.meta.host}` : '';
    const counts = [running ? `${running} running` : '', pending ? `${pending} pending` : ''].filter(Boolean).join(', ');
    document.title = counts ? `${counts} · Slurm${host}` : `Slurm Dashboard${host}`;
}

// -------------------------------------------------------------------- refresh

let refreshing = false;
let firstLoad = true;

async function refresh() {
    if (refreshing) return;
    refreshing = true;
    try {
        const data = await api.get(state.historyDays ? `/api/jobs?days=${state.historyDays}` : '/api/jobs');
        state.active = data.running;
        state.finished = data.recent;
        state.errors = data.errors || [];
        state.meta = { ...state.meta, ...data.meta };
        state.loaded = true;
        state.loadedAt = Date.now();

        setBanner('server', '');
        setBanner('auth', '');
        setBanner('slurm', state.errors.length ? `Slurm is not answering (${state.errors.join('; ')}). Showing the last data that loaded.` : '');
        setLive(!state.errors.length, `Last updated ${new Date().toLocaleTimeString()}`);
        updateTitle();

        if (firstLoad) {
            firstLoad = false;
            if (!state.selectedId && !NARROW.matches) {
                const first = state.active.find(job => job.state_category === 'running') || state.active[0] || state.finished[0];
                if (first) state.selectedId = first.id;
            }
            emit('select', { open: Boolean(state.selectedId) && location.hash.includes('job=') });
        }
        emit('jobs');
    } catch (err) {
        if (err.status === 401) {
            setBanner('auth', 'This browser is signed out. Reopen the link with the token that slurm-dashboard printed when it started.');
        } else {
            setBanner('server', `${err.message}. Is slurm-dashboard still running, and is your SSH tunnel up?`);
        }
        setLive(false, 'The last update failed');
    } finally {
        refreshing = false;
    }
}

/** Advance the clocks and progress bars of running jobs between refreshes. */
function tick() {
    if (document.hidden) return;
    const running = new Map(state.active.filter(job => job.state_category === 'running').map(job => [job.id, job]));
    for (const el of $$('[data-tick]')) {
        const job = running.get(el.dataset.tick);
        if (job) el.textContent = fmtClock(liveElapsed(job));
    }
    for (const el of $$('[data-prog]')) {
        const job = running.get(el.dataset.prog);
        if (job && job.limit_seconds) el.style.width = `${Math.min(100, (liveElapsed(job) / job.limit_seconds) * 100).toFixed(2)}%`;
    }
}

// ---------------------------------------------------------------------- views

function applyView() {
    for (const section of $$('main > .view')) section.hidden = section.id !== `view-${state.view}`;
    for (const button of $$('.views [data-view]')) {
        const current = button.dataset.view === state.view;
        button.classList.toggle('on', current);
        button.setAttribute('aria-current', current ? 'page' : 'false');
    }
}

function toggleTheme() {
    const next = document.documentElement.dataset.theme === 'dark' ? 'light' : 'dark';
    document.documentElement.dataset.theme = next;
    store.set('theme', next);
}

// ------------------------------------------------------------------- keyboard

function onKey(event) {
    if (event.defaultPrevented || $('dialog[open]')) return;
    const target = event.target instanceof Element ? event.target : document.body;
    const typing = target.matches('input, textarea, select') || target.isContentEditable;
    const key = event.key;

    if ((event.ctrlKey || event.metaKey) && key.toLowerCase() === 'f' && state.view === 'jobs' && detail.logIsOpen()) {
        event.preventDefault();
        return detail.focusFind();
    }
    if (event.ctrlKey || event.metaKey || event.altKey) return;

    if (key === 'Escape') {
        if (target.id === 'q' && state.query) {
            target.value = '';
            setFilter({ query: '' });
        } else if (typing) {
            target.blur();
        }
        return;
    }
    if (typing) return;
    // Arrow keys scroll a focused log instead of changing the selection.
    const inLog = Boolean(target.closest('.log-body, .pane-scroll'));

    if (key === '?') return openHelp();
    if (key === 'n') return openSubmit();
    if (key === 'r') return refresh();
    if (key === '/') {
        event.preventDefault();
        if (state.view !== 'jobs') setView('jobs');
        $('#q').focus();
        return $('#q').select();
    }
    if (state.view !== 'jobs') return;
    if (key === 'j' || (key === 'ArrowDown' && !inLog)) {
        event.preventDefault();
        moveSelection(1);
    } else if (key === 'k' || (key === 'ArrowUp' && !inLog)) {
        event.preventDefault();
        moveSelection(-1);
    } else if (key >= '1' && key <= '4' && state.selectedId) {
        setTab(TABS[Number(key) - 1]);
    } else if (key === 'f' && detail.logIsOpen()) {
        detail.toggleFollow();
    }
}

// ----------------------------------------------------------------------- boot

readHash();
state.historyDays = store.get('historyDays', null);
initList();
initDetail();
initTimeline();
initPipelines();
initInsights();

$('.top').addEventListener('click', event => {
    const view = event.target.closest('[data-view]');
    if (view) return setView(view.dataset.view);
    const action = event.target.closest('[data-action]')?.dataset.action;
    if (action === 'submit') openSubmit().catch(err => toast(err.message, { kind: 'error' }));
    else if (action === 'help') openHelp();
    else if (action === 'theme') toggleTheme();
});

on('view', applyView);
on('select', info => {
    // On narrow screens the job page replaces the list; `detail-open` switches between them.
    const view = $('#view-jobs');
    if (!state.selectedId) view.classList.remove('detail-open');
    else if (info && info.open) view.classList.add('detail-open');
});
on('refresh', () => {
    refresh();
    // Slurm can take a moment to reflect a submit or cancel.
    setTimeout(refresh, 2500);
});

window.addEventListener('hashchange', () => {
    readHash();
    applyView();
    emit('view');
    emit('filter');
    emit('select', { open: Boolean(state.selectedId) });
});
document.addEventListener('keydown', onKey);
document.addEventListener('visibilitychange', () => {
    if (!document.hidden) refresh();
});

applyView();
emit('view');
renderList();
refresh();
setInterval(() => { if (!document.hidden) refresh(); }, POLL_MS);
setInterval(tick, 1000);
setInterval(() => { if (!document.hidden && state.view === 'jobs') renderList(); }, 60000);
