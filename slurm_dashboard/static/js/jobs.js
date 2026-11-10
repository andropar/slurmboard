// The job list: search, state chips, array-task grouping, selection and batch actions.

import {
    $, $$, CATEGORY_LABELS, dayLabel, debounce, fmtClock, fmtDateTime, fmtDuration,
    fmtRelative, fmtTime, html, raw, setHTML, stateBadge, store,
} from './util.js';
import { emit, findJob, liveElapsed, on, selectJob, setFilter, state } from './state.js';
import { api, jobPath } from './api.js';
import { confirmDialog, openSubmit, toast } from './dialogs.js';

const CHIPS = [
    ['', 'All'],
    ['running', 'Running'],
    ['pending', 'Pending'],
    ['failed', 'Failed'],
    ['completed', 'Completed'],
    ['cancelled', 'Cancelled'],
];

const CHEVRON = raw('<svg class="chev" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" aria-hidden="true"><polyline points="9 6 15 12 9 18"/></svg>');

// ------------------------------------------------------------------ filtering

const FIELDS = {
    name: job => job.name,
    partition: job => job.partition,
    state: job => job.state,
    user: job => job.user,
    id: job => job.id,
    exit: job => job.exit_code,
    node: job => job.nodelist,
};
const SUBSTRING_FIELDS = new Set(['name', 'node', 'id']);

/** Turn search text into a predicate. See the help dialog for the syntax. */
export function parseQuery(text) {
    const tests = [];
    for (const token of text.trim().split(/\s+/).filter(Boolean)) {
        const fielded = /^(\w+):(.+)$/.exec(token);
        const field = fielded && fielded[1].toLowerCase();
        if (fielded && FIELDS[field]) {
            let wanted = fielded[2].toLowerCase();
            const negate = wanted.startsWith('!');
            if (negate) wanted = wanted.slice(1);
            let test;
            if (wanted.includes('*')) {
                const pattern = new RegExp(`^${wanted.split('*').map(s => s.replace(/[.+?^${}()|[\]\\]/g, '\\$&')).join('.*')}$`);
                test = actual => pattern.test(actual);
            } else if (SUBSTRING_FIELDS.has(field)) {
                test = actual => actual.includes(wanted);
            } else {
                test = actual => actual === wanted;
            }
            tests.push(job => {
                const value = FIELDS[field](job);
                if (value === null || value === undefined || value === '') return false;
                return test(String(value).toLowerCase()) !== negate;
            });
        } else {
            const word = token.toLowerCase();
            tests.push(job => (job.name || '').toLowerCase().includes(word) || String(job.id).includes(word));
        }
    }
    return job => tests.every(test => test(job));
}

function chipCategory(job) {
    return job.state_category === 'timeout' ? 'failed' : job.state_category;
}

const matchesState = job => !state.stateFilter || chipCategory(job) === state.stateFilter;

// ------------------------------------------------------------------- grouping

/** Fold array tasks into one entry per array when a list holds two or more of them. */
function groupTasks(jobs) {
    const sizes = new Map();
    for (const job of jobs) {
        if (job.array_job_id) sizes.set(job.array_job_id, (sizes.get(job.array_job_id) || 0) + 1);
    }
    const out = [];
    const groups = new Map();
    for (const job of jobs) {
        const key = job.array_job_id;
        if (key && sizes.get(key) > 1) {
            if (!groups.has(key)) {
                groups.set(key, { arrayId: key, name: job.name, tasks: [] });
                out.push(groups.get(key));
            }
            groups.get(key).tasks.push(job);
        } else {
            out.push(job);
        }
    }
    return out;
}

function sortActive(jobs) {
    const stamp = value => (value ? Date.parse(value) : 0);
    return [...jobs].sort((a, b) => {
        const rank = job => (job.state_category === 'running' ? 0 : 1);
        if (rank(a) !== rank(b)) return rank(a) - rank(b);
        return stamp(b.start || b.submit) - stamp(a.start || a.submit);
    });
}

// ------------------------------------------------------------------ templates

function subline(job, { active, task }) {
    const parts = [html`<span class="mono">${job.id}</span>`];
    if (state.meta.all_users && job.user) parts.push(html`<span>${job.user}</span>`);
    if (job.partition && !task && active) parts.push(html`<span>${job.partition}</span>`);
    if (active) {
        if (job.state_category === 'pending') {
            if (job.expected_start) parts.push(html`<span title="Start time planned by the scheduler">starts ${fmtTime(job.expected_start)}</span>`);
            else if (job.reason) parts.push(html`<span title="Why Slurm has not started this job yet">${job.reason}</span>`);
        } else if (job.nodelist) {
            parts.push(html`<span>${job.nodelist}</span>`);
        }
    } else {
        if (job.elapsed_seconds) parts.push(html`<span title="Runtime">ran ${fmtDuration(job.elapsed_seconds)}</span>`);
        if (job.exit_code && job.exit_code !== '0') parts.push(html`<span class="bad">exit ${job.exit_code}</span>`);
        if (job.partition && !task) parts.push(html`<span>${job.partition}</span>`);
    }
    return parts.map((part, i) => html`${i ? raw('<span class="dot">·</span>') : ''}${part}`);
}

function timeCell(job, active) {
    if (!active) {
        return html`<span class="row-time" title="${fmtDateTime(job.updated)}">${fmtRelative(job.updated)}</span>`;
    }
    if (job.state_category === 'running') {
        const limit = job.limit_seconds ? ` of ${fmtClock(job.limit_seconds)}` : '';
        return html`<span class="row-time" data-tick="${job.id}" title="Runtime${limit}">${fmtClock(liveElapsed(job))}</span>`;
    }
    const waited = job.submit ? (Date.now() - Date.parse(job.submit)) / 1000 : 0;
    return html`<span class="row-time faint" title="Waiting since ${fmtDateTime(job.submit)}">${waited > 0 ? fmtDuration(waited) : ''}</span>`;
}

function progress(job) {
    if (job.state_category !== 'running' || !job.limit_seconds) return '';
    const share = Math.min(100, (liveElapsed(job) / job.limit_seconds) * 100);
    return html`<i class="prog ${share >= 90 ? 'near' : ''}" data-prog="${job.id}" style="width:${share.toFixed(2)}%"></i>`;
}

function rowTpl(job, { active, task = false }) {
    const selected = job.id === state.selectedId;
    const checked = state.checked.has(job.id);
    // A condensed entry such as "3-8%2" stands for every task that has not started yet.
    const several = /[-,]/.test(String(job.array_task_id ?? ''));
    const name = task ? `${several ? 'Tasks' : 'Task'} ${job.array_task_id ?? job.id}` : job.name || `Job ${job.id}`;
    return html`
        <div class="row ${selected ? 'sel' : ''} ${task ? 'task' : ''}" data-id="${job.id}" role="option" aria-selected="${selected}">
            <label class="ck" title="Select"><input type="checkbox" data-check="${job.id}" ${checked ? raw('checked') : ''} aria-label="Select job ${job.id}"></label>
            <div class="row-main">
                <div class="row-name" title="${job.name}">${name}</div>
                <div class="row-sub">${subline(job, { active, task })}</div>
            </div>
            ${stateBadge(job)}
            ${timeCell(job, active)}
            ${progress(job)}
        </div>`;
}

function groupTpl(group, section) {
    const key = `${section}:${group.arrayId}`;
    const open = state.openGroups.has(key);
    const counts = {};
    for (const task of group.tasks) {
        const category = task.state_category || 'unknown';
        counts[category] = (counts[category] || 0) + 1;
    }
    const order = ['running', 'pending', 'failed', 'timeout', 'cancelled', 'completed', 'unknown'];
    const summary = order.filter(c => counts[c]).map(c => html`<span class="state s-${c}"><i></i>${counts[c]} ${CATEGORY_LABELS[c]}</span>`);
    const allChecked = group.tasks.every(task => state.checked.has(task.id));
    return html`
        <div class="row group ${open ? 'open' : ''}" data-group="${key}" role="button" aria-expanded="${open}">
            <label class="ck" title="Select all tasks"><input type="checkbox" data-check-group="${key}" ${allChecked ? raw('checked') : ''} aria-label="Select all tasks of array ${group.arrayId}"></label>
            <div class="row-main">
                <div class="row-name" title="${group.name}">${CHEVRON}${group.name}</div>
                <div class="row-sub"><span class="mono">${group.arrayId}</span><span class="dot">·</span><span>array, ${group.tasks.length} tasks</span></div>
            </div>
            <div class="group-states">${summary}</div>
        </div>
        ${open ? group.tasks.map(task => rowTpl(task, { active: section === 'active', task: true })) : ''}`;
}

function sectionTpl(title, note, entries, section) {
    if (!entries.length) return '';
    const count = entries.reduce((n, entry) => n + (entry.tasks ? entry.tasks.length : 1), 0);
    let lastDay = null;
    const rows = entries.map(entry => {
        let day = '';
        if (section === 'finished') {
            const label = dayLabel((entry.tasks ? entry.tasks[0] : entry).updated);
            if (label !== lastDay) {
                lastDay = label;
                day = html`<div class="day">${label}</div>`;
            }
        }
        const row = entry.tasks ? groupTpl(entry, section) : rowTpl(entry, { active: section === 'active' });
        return html`${day}${row}`;
    });
    return html`
        <section class="sec" data-section="${section}">
            <div class="sec-head"><h2>${title}</h2><span class="count">${count}</span>${note ? html`<span class="sec-note">${note}</span>` : ''}</div>
            ${rows}
        </section>`;
}

// ------------------------------------------------------------------ rendering

let visibleGroups = new Map();   // group key -> task ids, for the group checkboxes

function renderChips(queried) {
    const counts = { '': queried.length };
    for (const job of queried) {
        const category = chipCategory(job);
        counts[category] = (counts[category] || 0) + 1;
    }
    setHTML($('#chips'), CHIPS.filter(([key]) => !key || counts[key] || state.stateFilter === key).map(([key, label]) => html`
        <button class="chip ${state.stateFilter === key ? 'on' : ''}" data-state="${key}" aria-pressed="${state.stateFilter === key}">
            ${key ? html`<i class="s-${key}"></i>` : ''}${label}<span class="chip-n">${counts[key] || 0}</span>
        </button>`));
}

export function renderList() {
    const list = $('#list');
    const match = parseQuery(state.query);
    renderChips([...state.active, ...state.finished].filter(match));

    const active = groupTasks(sortActive(state.active.filter(job => match(job) && matchesState(job))));
    const finished = groupTasks(state.finished.filter(job => match(job) && matchesState(job)));

    visibleGroups = new Map();
    for (const [section, entries] of [['active', active], ['finished', finished]]) {
        for (const entry of entries) {
            if (entry.tasks) visibleGroups.set(`${section}:${entry.arrayId}`, entry.tasks.map(task => task.id));
        }
    }

    const total = state.active.length + state.finished.length;
    if (!state.loaded) {
        setHTML(list, html`<p class="empty">Loading jobs…</p>`);
    } else if (!total) {
        setHTML(list, html`<div class="empty">
            <p>No jobs in the last ${state.meta.history_days || 7} days.</p>
            <button class="btn" data-action="submit">Submit a job</button>
        </div>`);
    } else if (!active.length && !finished.length) {
        setHTML(list, html`<div class="empty">
            <p>No jobs match this search.</p>
            <button class="btn" data-action="clear-filters">Clear search and filters</button>
        </div>`);
    } else {
        setHTML(list, html`
            ${sectionTpl('Active', '', active, 'active')}
            ${sectionTpl('Finished', `last ${state.meta.history_days || 7} days`, finished, 'finished')}
            ${historyTpl()}`);
    }
    list.classList.toggle('has-checked', state.checked.size > 0);
    renderBatch();
}

/** Footer of the list: how far back it goes, and a way to go further. */
function historyTpl() {
    const shown = state.meta.history_days || 7;
    const usual = state.meta.default_history_days || 7;
    const options = [...new Set([usual, 30, 90])].filter(days => days !== shown);
    return html`
        <div class="history">
            <span>Finished jobs go back ${shown} days${state.meta.truncated ? '; only the newest 1,500 are listed' : ''}.</span>
            ${options.map(days => html`<button class="link" data-history="${days}">${days < shown ? `Back to ${days} days` : `Show ${days} days`}</button>`)}
        </div>`;
}

function renderBatch() {
    const bar = $('#batch');
    const jobs = [...state.checked].map(findJob).filter(Boolean);
    bar.hidden = jobs.length === 0;
    if (!jobs.length) return;
    const activeIds = new Set(state.active.map(job => job.id));
    const cancellable = jobs.filter(job => job.owned && activeIds.has(job.id)).length;
    const resubmittable = jobs.filter(job => job.owned).length;
    setHTML(bar, html`
        <span class="batch-n">${jobs.length} selected</span>
        <button data-batch="cancel" ${cancellable ? '' : raw('disabled')}>Cancel${cancellable && cancellable !== jobs.length ? ` ${cancellable}` : ''}</button>
        <button data-batch="resubmit" ${resubmittable ? '' : raw('disabled')}>Resubmit</button>
        <button data-batch="export">Export CSV</button>
        <button data-batch="clear" class="batch-x" aria-label="Clear selection">×</button>`);
}

// -------------------------------------------------------------------- actions

async function batchCancel() {
    const activeIds = new Set(state.active.map(job => job.id));
    const jobs = [...state.checked].map(findJob).filter(job => job && job.owned && activeIds.has(job.id));
    if (!jobs.length) return;
    const names = [...new Set(jobs.map(job => job.name))].slice(0, 3).join(', ');
    const ok = await confirmDialog({
        title: `Cancel ${jobs.length} job${jobs.length > 1 ? 's' : ''}?`,
        message: `${names}${jobs.length > 3 ? ' and others' : ''}. Running jobs stop immediately and lose unsaved work.`,
        confirm: `Cancel ${jobs.length > 1 ? `${jobs.length} jobs` : 'job'}`,
        dismiss: 'Keep running',
        danger: true,
    });
    if (!ok) return;
    const failures = [];
    for (const job of jobs) {
        try {
            await api.post(`/api/cancel/${jobPath(job.id)}`);
            state.checked.delete(job.id);
        } catch (err) {
            failures.push(`${job.id}: ${err.message}`);
        }
    }
    emit('refresh');
    if (failures.length) toast(`Could not cancel ${failures.length} of ${jobs.length} jobs. ${failures[0]}`, { kind: 'error' });
    else toast(`Cancelled ${jobs.length} job${jobs.length > 1 ? 's' : ''}.`);
}

async function batchResubmit() {
    const jobs = [...state.checked].map(findJob).filter(job => job && job.owned);
    if (!jobs.length) return;
    const ok = await confirmDialog({
        title: `Resubmit ${jobs.length} job${jobs.length > 1 ? 's' : ''}?`,
        message: 'Each job is submitted again with its original script and settings.',
        confirm: 'Resubmit',
    });
    if (!ok) return;
    const failures = [];
    for (const job of jobs) {
        try {
            await api.post('/api/resubmit', { job_id: job.id });
        } catch (err) {
            failures.push(`${job.id}: ${err.message}`);
        }
    }
    state.checked.clear();
    emit('refresh');
    if (failures.length) toast(`Could not resubmit ${failures.length} of ${jobs.length} jobs. ${failures[0]}`, { kind: 'error' });
    else toast(`Resubmitted ${jobs.length} job${jobs.length > 1 ? 's' : ''}.`);
}

function exportCsv() {
    const jobs = [...state.checked].map(findJob).filter(Boolean);
    const columns = ['id', 'name', 'user', 'state', 'partition', 'submit', 'start', 'end', 'elapsed', 'exit_code', 'nodelist'];
    const cell = value => {
        const text = String(value ?? '');
        return /[",\n]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
    };
    const rows = [columns.join(',')].concat(jobs.map(job => columns.map(c => cell(c === 'elapsed' ? job.elapsed || job.runtime : job[c])).join(',')));
    const link = document.createElement('a');
    link.href = URL.createObjectURL(new Blob([rows.join('\n') + '\n'], { type: 'text/csv' }));
    link.download = `slurm-jobs-${new Date().toISOString().slice(0, 10)}.csv`;
    link.click();
    URL.revokeObjectURL(link.href);
}

/** Move the selection by `delta` rows among the jobs currently visible. */
export function moveSelection(delta) {
    const rows = $$('#list .row[data-id]');
    if (!rows.length) return;
    const index = rows.findIndex(row => row.dataset.id === state.selectedId);
    const next = rows[Math.max(0, Math.min(rows.length - 1, index === -1 ? 0 : index + delta))];
    selectJob(next.dataset.id, { open: false });
    $(`#list .row[data-id="${CSS.escape(next.dataset.id)}"]`)?.scrollIntoView({ block: 'nearest' });
}

// --------------------------------------------------------------------- wiring

export function initList() {
    const list = $('#list');
    const search = $('#q');
    search.value = state.query;
    search.addEventListener('input', debounce(() => setFilter({ query: search.value }), 120));

    $('#chips').addEventListener('click', event => {
        const chip = event.target.closest('[data-state]');
        if (!chip) return;
        const key = chip.dataset.state;
        setFilter({ stateFilter: state.stateFilter === key ? '' : key });
    });

    let lastChecked = null;
    list.addEventListener('click', event => {
        const action = event.target.closest('[data-action]')?.dataset.action;
        if (action === 'submit') return openSubmit();
        if (action === 'clear-filters') {
            search.value = '';
            return setFilter({ query: '', stateFilter: '' });
        }
        const history = event.target.closest('[data-history]');
        if (history) {
            const days = Number(history.dataset.history);
            state.historyDays = days === (state.meta.default_history_days || 7) ? null : days;
            store.set('historyDays', state.historyDays);
            history.textContent = 'Loading…';
            history.disabled = true;
            return emit('refresh');
        }

        const box = event.target.closest('input[data-check]');
        if (box) {
            // Shift-click extends the selection from the last ticked row.
            if (event.shiftKey && lastChecked) {
                const ids = $$('#list input[data-check]').map(input => input.dataset.check);
                const [from, to] = [ids.indexOf(lastChecked), ids.indexOf(box.dataset.check)].sort((a, b) => a - b);
                if (from >= 0) ids.slice(from, to + 1).forEach(id => state.checked.add(id));
            }
            if (box.checked) state.checked.add(box.dataset.check);
            else state.checked.delete(box.dataset.check);
            lastChecked = box.dataset.check;
            return renderList();
        }
        const groupBox = event.target.closest('input[data-check-group]');
        if (groupBox) {
            const ids = visibleGroups.get(groupBox.dataset.checkGroup) || [];
            ids.forEach(id => (groupBox.checked ? state.checked.add(id) : state.checked.delete(id)));
            return renderList();
        }
        if (event.target.closest('.ck')) return;

        const group = event.target.closest('[data-group]');
        if (group) {
            const key = group.dataset.group;
            if (state.openGroups.has(key)) state.openGroups.delete(key);
            else state.openGroups.add(key);
            return renderList();
        }
        const row = event.target.closest('.row[data-id]');
        if (row) selectJob(row.dataset.id);
    });

    $('#batch').addEventListener('click', event => {
        const action = event.target.closest('[data-batch]')?.dataset.batch;
        if (action === 'cancel') batchCancel();
        else if (action === 'resubmit') batchResubmit();
        else if (action === 'export') exportCsv();
        else if (action === 'clear') {
            state.checked.clear();
            renderList();
        }
    });

    let signature = '';
    on('jobs', () => {
        // Forget ticks for jobs that left the lists.
        for (const id of [...state.checked]) if (!findJob(id)) state.checked.delete(id);
        // Running clocks tick on their own, so only redraw when something else changed.
        const next = JSON.stringify([
            state.active.map(({ runtime, runtime_seconds, ...rest }) => rest),
            state.finished,
            state.meta.history_days,
            state.loaded,
        ]);
        if (next === signature) return;
        signature = next;
        renderList();
    });
    on('filter', () => {
        if (search.value !== state.query) search.value = state.query;
        renderList();
    });
    on('select', () => {
        // A selection made elsewhere (timeline, pipelines) may sit inside a closed array group.
        const job = findJob(state.selectedId);
        if (job && job.array_job_id) {
            state.openGroups.add(`active:${job.array_job_id}`);
            state.openGroups.add(`finished:${job.array_job_id}`);
        }
        renderList();
        $(`#list .row.sel`)?.scrollIntoView({ block: 'nearest' });
    });
}
