// The job page: header with state, timing and actions; tabs for output, errors, script and details.

import {
    $, $$, copyText, fmtBytes, fmtClock, fmtDateTime, fmtDuration, fmtPercent, fmtRange, fmtRelative,
    fmtTime, html, raw, setHTML, stateBadge, stateLabel, store,
} from './util.js';
import { emit, findJob, liveElapsed, on, selectJob, setTab, state } from './state.js';
import { api, jobPath } from './api.js';
import { confirmDialog, openResubmit, toast } from './dialogs.js';
import { LogView } from './logview.js';

const INFO_REFRESH_MS = 30000;

// What Slurm's pending reasons mean in practice.
const REASONS = {
    Priority: 'Other jobs are ahead of it in the queue',
    Resources: 'Waiting for nodes to become free',
    Dependency: 'Waiting for another job to finish',
    DependencyNeverSatisfied: 'A job it depends on failed, so it will never start',
    BeginTime: 'Scheduled to start later',
    JobHeldUser: 'Held by you (release it with scontrol release)',
    JobHeldAdmin: 'Held by an administrator',
    ReqNodeNotAvail: 'The requested nodes are unavailable',
    PartitionDown: 'The partition is down',
    PartitionNodeLimit: 'Asks for more nodes than the partition allows',
    PartitionTimeLimit: 'Asks for more time than the partition allows',
    JobArrayTaskLimit: 'The array is at its limit of simultaneously running tasks',
    Reservation: 'Waiting for its reservation',
    Licenses: 'Waiting for a licence',
};

const OUTCOMES = {
    FAILED: job => (job.exit_code && job.exit_code !== '0' ? `Exited with code ${job.exit_code}` : 'Failed'),
    TIMEOUT: job => `Stopped at its time limit${job.limit_seconds ? ` of ${fmtDuration(job.limit_seconds)}` : ''}`,
    OUT_OF_MEMORY: job => `Used more memory than it requested${job.req_mem ? ` (${job.req_mem})` : ''}`,
    NODE_FAIL: () => 'The node it ran on failed',
    PREEMPTED: () => 'Preempted by a higher-priority job',
    CANCELLED: () => 'Cancelled before it finished',
    BOOT_FAIL: () => 'The node could not be started',
};

let logView;
let info = null;          // last /api/job reply
let infoFor = null;       // job that reply describes
let infoError = null;
let infoAt = 0;
let loadingFor = null;
let openLog = null;       // `${id}:${kind}:${phase}` currently shown in the log pane
let script = null;
let scriptFor = null;
let usage = null;
let usageFor = null;
let usageAt = 0;
let headSignature = '';
const watched = new Set(store.get('watched', []));

// -------------------------------------------------------------------- the job

/** The selected job as currently known: the fresh list row merged with the full record. */
function currentJob() {
    const id = state.selectedId;
    const listed = findJob(id);
    const full = infoFor === id && info ? info.job : null;
    if (!listed && !full) return { id, name: '', state: '', state_category: 'unknown', pendingLoad: true };
    const job = { ...(listed || {}), ...(full || {}) };
    if (listed) {
        // The list refreshes every few seconds, so its state wins over the fetched record.
        job.state = listed.state;
        job.state_category = listed.state_category;
        job.active = state.active.includes(listed);
        job.owned = listed.owned;
        if (job.active) {
            job.elapsed_seconds = liveElapsed(listed);
            job.limit_seconds = listed.limit_seconds || job.limit_seconds;
            job.reason = listed.reason;
            job.expected_start = listed.expected_start;
        }
    }
    return job;
}

async function loadInfo(id) {
    if (loadingFor === id) return;
    loadingFor = id;
    infoAt = Date.now();
    try {
        const data = await api.get(`/api/job/${jobPath(id)}`);
        if (state.selectedId !== id) return;
        info = data;
        infoFor = id;
        infoError = null;
    } catch (err) {
        if (state.selectedId !== id) return;
        if (infoFor !== id) info = null;
        infoError = err.message;
    } finally {
        if (loadingFor === id) loadingFor = null;
    }
    render();
}

function resolveTab() {
    const sameFile = info && infoFor === state.selectedId && info.logs.same_file;
    if (state.tab) return state.tab === 'errors' && sameFile ? 'output' : state.tab;
    if (!info || infoFor !== state.selectedId) return 'output';
    const job = currentJob();
    const failed = job.state_category === 'failed' || job.state_category === 'timeout';
    return failed && !sameFile && info.logs.stderr.size > 0 ? 'errors' : 'output';
}

// --------------------------------------------------------------------- header

function facts(job) {
    const items = [html`<button class="copy mono" data-copy="${job.id}" title="Copy job ID">${job.id}</button>`];
    if (state.meta.all_users && job.user) items.push(job.user);
    if (job.array_job_id) {
        const several = /[-,]/.test(String(job.array_task_id));
        items.push(`${several ? 'tasks' : 'task'} ${job.array_task_id} of array ${job.array_job_id}`);
    }
    if (job.partition) items.push(job.partition);
    if (job.nodelist && job.state_category !== 'pending') items.push(job.nodelist);
    if (job.cpus) items.push(`${job.cpus} CPU${job.cpus === 1 ? '' : 's'}`);
    if (job.gpus) items.push(`${job.gpus} GPU${job.gpus === 1 ? '' : 's'}`);
    if (job.req_mem) items.push(`${job.req_mem} memory`);
    return items.map((item, i) => html`${i ? raw('<span class="dot">·</span>') : ''}<span>${item}</span>`);
}

function timing(job) {
    if (job.state_category === 'running') {
        const elapsed = job.elapsed_seconds || 0;
        const limit = job.limit_seconds;
        const share = limit ? Math.min(100, (elapsed / limit) * 100) : 0;
        const endsBy = limit && job.start ? new Date(Date.parse(job.start) + limit * 1000).toISOString() : null;
        return html`
            ${limit ? html`<div class="runbar ${share >= 90 ? 'near' : ''}" role="img" aria-label="Runtime as a share of the time limit"><i data-prog="${job.id}" style="width:${share.toFixed(2)}%"></i></div>` : ''}
            <p class="d-line">
                <strong data-tick="${job.id}">${fmtClock(elapsed)}</strong>${limit ? ` of ${fmtClock(limit)}` : ' so far'}
                ${job.start ? html`<span class="dot">·</span>started ${fmtTime(job.start)}` : ''}
                ${endsBy ? html`<span class="dot">·</span>time limit ends ${fmtTime(endsBy)}` : ''}
            </p>`;
    }
    if (job.state_category === 'pending') {
        const reason = job.reason ? REASONS[job.reason] || `Waiting (${job.reason})` : 'Waiting to start';
        return html`<p class="d-line">
            <strong>${reason}</strong>
            ${job.expected_start ? html`<span class="dot">·</span>planned start ${fmtTime(job.expected_start)}` : ''}
            ${job.submit ? html`<span class="dot">·</span>submitted ${fmtRelative(job.submit)}` : ''}
            ${job.dependency ? html`<span class="dot">·</span><span class="mono">${job.dependency}</span>` : ''}
        </p>`;
    }
    if (job.pendingLoad || job.state_category === 'unknown') {
        return job.pendingLoad ? '' : html`<p class="d-line">Slurm no longer has a record of this job; only its log files remain.</p>`;
    }
    const outcome = OUTCOMES[job.state] ? OUTCOMES[job.state](job) : null;
    return html`<p class="d-line">
        ${outcome ? html`<strong>${outcome}</strong><span class="dot">·</span>` : ''}
        ${job.elapsed_seconds ? html`ran ${fmtDuration(job.elapsed_seconds)}` : 'did not run'}
        ${job.start && job.end ? html`<span class="dot">·</span>${fmtRange(job.start, job.end)}` : ''}
        ${job.end ? html`<span class="dot">·</span>ended ${fmtRelative(job.end)}` : ''}
    </p>`;
}

function meter(label, value, detail, title) {
    if (value === null || value === undefined) return '';
    return html`<div class="meter" title="${title}">
        <span class="m-label">${label}</span>
        <span class="m-bar"><i style="width:${Math.max(1, Math.min(100, value)).toFixed(1)}%"></i></span>
        <span class="m-val">${fmtPercent(value)}</span>
        ${detail ? html`<span class="m-detail">${detail}</span>` : ''}
    </div>`;
}

function meters(job) {
    const memory = job.max_rss_bytes && job.req_mem_bytes ? `${fmtBytes(job.max_rss_bytes)} of ${fmtBytes(job.req_mem_bytes)}` : '';
    const time = !job.active && job.limit_seconds ? `${fmtDuration(job.elapsed_seconds)} of ${fmtDuration(job.limit_seconds)}` : '';
    const rows = [
        meter('CPU', job.cpu_eff, '', 'CPU time used, as a share of cores × runtime'),
        meter('Memory', job.mem_eff, memory, 'Peak memory, as a share of the memory requested'),
        job.active ? '' : meter('Time', job.time_eff, time, 'Runtime, as a share of the time limit'),
    ].filter(Boolean);
    return rows.length ? html`<div class="meters">${rows}</div>` : '';
}

function headTpl(job) {
    const watching = watched.has(job.id);
    const loaded = info && infoFor === job.id;
    return html`
        <div class="d-title">
            <h1 title="${job.name}">${job.name || `Job ${job.id}`}</h1>
            ${job.state ? stateBadge(job) : ''}
            <div class="d-actions">
                ${job.active ? html`<button class="btn ${watching ? 'on' : ''}" data-detail="watch" aria-pressed="${watching}" title="Get a browser notification when this job ends">${watching ? 'Will notify' : 'Notify when done'}</button>` : ''}
                ${job.owned && loaded && info.submission ? html`<button class="btn" data-detail="resubmit">Resubmit</button>` : ''}
                ${job.active && job.owned ? html`<button class="btn danger" data-detail="cancel">Cancel job</button>` : ''}
            </div>
        </div>
        <div class="d-facts">${facts(job)}</div>
        ${timing(job)}
        ${meters(job)}
        ${infoError && !loaded ? html`<p class="d-line bad">${infoError}</p>` : ''}`;
}

function tabsTpl(active) {
    const logs = info && infoFor === state.selectedId ? info.logs : null;
    const size = entry => (entry && entry.size !== null ? html`<span class="tab-n">${entry.size ? fmtBytes(entry.size) : 'empty'}</span>` : '');
    const tabs = [
        ['output', logs && logs.same_file ? 'Output and errors' : 'Output', size(logs && logs.stdout)],
        ...(logs && logs.same_file ? [] : [['errors', 'Errors', size(logs && logs.stderr)]]),
        ['script', 'Script', ''],
        ['details', 'Details', ''],
    ];
    return tabs.map(([key, label, extra]) => html`
        <button class="tab ${key === active ? 'on' : ''}" data-tab="${key}" role="tab" aria-selected="${key === active}">${label}${extra}</button>`);
}

// ----------------------------------------------------------------------- tabs

function showLog(job, tab) {
    const kind = tab === 'errors' ? 'stderr' : 'stdout';
    const waiting = job.state_category === 'pending';
    const key = `${job.id}:${kind}:${waiting ? 'waiting' : 'started'}`;
    if (openLog === key) return;
    if (waiting) {
        openLog = key;
        logView.showOnly('This job has not started yet. Its output appears here as soon as it runs.');
        return;
    }
    // Wait for the job record: it decides which stream to show first.
    if (infoFor !== job.id && !infoError) return;
    openLog = key;
    logView.open(job.id, kind, { live: Boolean(job.active) });
}

async function showScript(job) {
    const pane = $('[data-pane="script"]');
    if (scriptFor !== job.id) {
        scriptFor = job.id;
        script = null;
        setHTML(pane, html`<p class="pane-msg">Loading…</p>`);
        try {
            script = await api.get(`/api/job_script/${jobPath(job.id)}`);
        } catch (err) {
            script = { error: err.message };
        }
        if (scriptFor !== job.id) return;
    }
    if (!script) return;
    if (script.error) return setHTML(pane, html`<p class="pane-msg">${script.error}</p>`);
    const note = script.source === 'slurm'
        ? 'The copy Slurm kept when the job was submitted.'
        : html`Read from <span class="mono">${script.path}</span> as it is now. It may have changed since the job was submitted.`;
    setHTML(pane, html`
        <div class="pane-note"><span>${note}</span><button class="link" data-copy-script>Copy script</button></div>
        <pre class="code">${script.content}</pre>`);
}

function row(label, value, { mono = false, copy = false } = {}) {
    if (value === null || value === undefined || value === '') return '';
    return html`<dt>${label}</dt><dd class="${mono ? 'mono' : ''}">${value}${copy ? html` <button class="link" data-copy="${value}">Copy</button>` : ''}</dd>`;
}

function showDetails(job) {
    const logs = infoFor === job.id && info ? info.logs : null;
    const submission = infoFor === job.id && info ? info.submission : null;
    const waited = job.submit && job.start ? (Date.parse(job.start) - Date.parse(job.submit)) / 1000 : null;
    const liveUsage = usageFor === job.id && usage && !usage.error ? usage : null;
    const mem = value => {
        const match = /^([\d.]+)([KMGT]?)$/i.exec(value || '');
        if (!match) return value;
        return fmtBytes(parseFloat(match[1]) * ({ K: 1024, M: 1024 ** 2, G: 1024 ** 3, T: 1024 ** 4 }[match[2].toUpperCase()] || 1024));
    };
    setHTML($('[data-pane="details"]'), html`
        <div class="details">
            <section><h3>Status</h3><dl>
                ${row('State', stateLabel(job.state))}
                ${row('Exit code', job.exit_code)}
                ${row('Waiting for', job.state_category === 'pending' ? job.reason : '')}
                ${row('Depends on', job.dependency, { mono: true })}
                ${row('Priority', job.state_category === 'pending' ? job.priority : '')}
            </dl></section>
            <section><h3>Timing</h3><dl>
                ${row('Submitted', fmtDateTime(job.submit))}
                ${row('Queued for', waited !== null && waited >= 0 ? fmtDuration(waited) : '')}
                ${row('Planned start', job.state_category === 'pending' ? fmtDateTime(job.expected_start) : '')}
                ${row('Started', fmtDateTime(job.start))}
                ${row('Ended', fmtDateTime(job.end))}
                ${row('Runtime', job.elapsed_seconds ? fmtClock(job.elapsed_seconds) : '')}
                ${row('Time limit', job.limit_seconds ? fmtClock(job.limit_seconds) : '')}
            </dl></section>
            <section><h3>Resources</h3><dl>
                ${row('Partition', job.partition)}
                ${row('Nodes', job.nodelist)}
                ${row('CPUs', job.cpus || '')}
                ${row('GPUs', job.gpus || '')}
                ${row('Allocated', job.gres, { mono: true })}
                ${row('Memory requested', job.req_mem)}
                ${row('Peak memory', job.max_rss_bytes ? fmtBytes(job.max_rss_bytes) : '')}
                ${row('CPU efficiency', job.cpu_eff !== null && job.cpu_eff !== undefined ? fmtPercent(job.cpu_eff) : '')}
                ${row('Memory efficiency', job.mem_eff !== null && job.mem_eff !== undefined ? fmtPercent(job.mem_eff) : '')}
            </dl></section>
            ${job.state_category === 'running' ? html`<section><h3>Right now</h3>
                ${liveUsage ? html`<dl>
                    ${row('Peak memory', mem(liveUsage.max_rss))}
                    ${row('Average memory', mem(liveUsage.ave_rss))}
                    ${row('CPU time', liveUsage.ave_cpu)}
                    ${row('Tasks', liveUsage.ntasks)}
                </dl>` : html`<p class="pane-msg">${usageFor === job.id && usage ? usage.error : 'Asking Slurm…'}</p>`}
            </section>` : ''}
            <section class="wide"><h3>Submission</h3><dl>
                ${row('User', job.user)}
                ${row('Working directory', job.work_dir || (submission && submission.work_dir), { mono: true, copy: true })}
                ${row('Command', (submission && submission.submit_line) || job.submit_line, { mono: true })}
                ${row('Script', submission && submission.script_path, { mono: true, copy: true })}
                ${row('Output file', logs && logs.stdout.path, { mono: true, copy: true })}
                ${row('Error file', logs && !logs.same_file ? logs.stderr.path : '', { mono: true, copy: true })}
            </dl></section>
        </div>`);

    if (job.state_category === 'running' && (usageFor !== job.id || Date.now() - usageAt > INFO_REFRESH_MS)) {
        usageFor = job.id;
        usageAt = Date.now();
        api.get(`/api/job_resources/${jobPath(job.id)}`)
            .then(data => { usage = data; })
            .catch(err => { usage = { error: err.message }; })
            .finally(() => { if (state.selectedId === job.id && resolveTab() === 'details') showDetails(currentJob()); });
    }
}

// --------------------------------------------------------------------- render

function render() {
    const root = $('#detail');
    const id = state.selectedId;
    $('.d-empty', root).hidden = Boolean(id);
    $('.d-job', root).hidden = !id;
    if (!id) {
        logView.close();
        openLog = null;
        return;
    }

    const job = currentJob();
    const tab = resolveTab();
    const signature = JSON.stringify([
        job.id, job.name, job.state, job.owned, job.active, job.reason, job.expected_start, job.end, job.exit_code,
        job.cpu_eff, job.mem_eff, job.nodelist, watched.has(job.id), infoFor === id, infoError, tab,
        info && infoFor === id ? [info.logs.stdout.size, info.logs.stderr.size, info.logs.same_file] : null,
        job.active ? null : job.elapsed_seconds,
    ]);
    if (signature !== headSignature) {
        headSignature = signature;
        setHTML($('.d-head', root), headTpl(job));
        setHTML($('.tabs', root), tabsTpl(tab));
    }

    const pane = tab === 'script' ? 'script' : tab === 'details' ? 'details' : 'log';
    $$('.pane', root).forEach(el => { el.hidden = el.dataset.pane !== pane; });
    if (pane === 'log') {
        showLog(job, tab);
    } else {
        logView.close();
        openLog = null;
        if (pane === 'script') showScript(job);
        else showDetails(job);
    }
}

function refresh() {
    const id = state.selectedId;
    if (!id) return render();
    const listed = findJob(id);
    const stale = infoFor === id && info && listed && info.job.state !== listed.state;
    const due = listed && state.active.includes(listed) && Date.now() - infoAt > INFO_REFRESH_MS;
    if (infoFor !== id || stale || due) loadInfo(id);
    render();
}

// -------------------------------------------------------------------- actions

async function cancelJob(job) {
    const ok = await confirmDialog({
        title: `Cancel ${job.name || `job ${job.id}`}?`,
        message: job.state_category === 'running'
            ? `Job ${job.id} stops immediately. Anything it has not saved is lost.`
            : `Job ${job.id} is removed from the queue.`,
        confirm: 'Cancel job',
        dismiss: job.state_category === 'running' ? 'Keep running' : 'Keep in queue',
        danger: true,
    });
    if (!ok) return;
    try {
        await api.post(`/api/cancel/${jobPath(job.id)}`);
        toast(`Cancelled ${job.name || job.id}.`);
        emit('refresh');
    } catch (err) {
        toast(`Could not cancel job ${job.id}: ${err.message}`, { kind: 'error' });
    }
}

async function toggleWatch(job) {
    if (watched.has(job.id)) {
        watched.delete(job.id);
    } else {
        watched.add(job.id);
        if ('Notification' in window && Notification.permission === 'default') {
            await Notification.requestPermission();
        }
        const system = 'Notification' in window && Notification.permission === 'granted';
        toast(system
            ? `You will get a notification when ${job.name || job.id} ends.`
            : `This page will tell you when ${job.name || job.id} ends. Browser notifications are off for this site.`);
    }
    store.set('watched', [...watched]);
    headSignature = '';
    render();
}

/** Announce watched jobs that have ended since the last refresh. */
function announceFinished() {
    const activeIds = new Set(state.active.map(job => job.id));
    for (const id of [...watched]) {
        if (activeIds.has(id)) continue;
        watched.delete(id);
        const job = state.finished.find(candidate => candidate.id === id);
        if (!job) continue;
        const failed = !['completed'].includes(job.state_category);
        const message = `${job.name || `Job ${id}`}: ${stateLabel(job.state).toLowerCase()}${job.elapsed_seconds ? ` after ${fmtDuration(job.elapsed_seconds)}` : ''}`;
        toast(message, { kind: failed ? 'error' : 'info', action: { label: 'Show', run: () => selectJob(id, { tab: null }) } });
        if ('Notification' in window && Notification.permission === 'granted') {
            const note = new Notification(failed ? 'Slurm job ended with a problem' : 'Slurm job finished', { body: message, tag: `job-${id}` });
            note.onclick = () => {
                window.focus();
                selectJob(id, { tab: null });
                note.close();
            };
        }
    }
    store.set('watched', [...watched]);
}

// --------------------------------------------------------------------- wiring

export function initDetail() {
    const root = $('#detail');
    setHTML(root, html`
        <div class="d-empty">
            <p>Select a job to see its output.</p>
            <p class="faint">Use <kbd>↑</kbd> <kbd>↓</kbd> to move through the list and <kbd>?</kbd> for all shortcuts.</p>
        </div>
        <div class="d-job" hidden>
            <button class="back link" data-detail="back">← All jobs</button>
            <div class="d-head"></div>
            <div class="tabs" role="tablist"></div>
            <div class="pane pane-log" data-pane="log"></div>
            <div class="pane pane-scroll" data-pane="script" hidden></div>
            <div class="pane pane-scroll" data-pane="details" hidden></div>
        </div>`);
    logView = new LogView($('[data-pane="log"]', root));

    root.addEventListener('click', event => {
        const tab = event.target.closest('[data-tab]');
        if (tab) return setTab(tab.dataset.tab);

        const copy = event.target.closest('[data-copy]');
        if (copy) {
            return copyText(copy.dataset.copy).then(ok => toast(ok ? `Copied ${copy.dataset.copy}` : 'Could not copy to the clipboard', { kind: ok ? 'info' : 'error' }));
        }
        if (event.target.closest('[data-copy-script]') && script && script.content) {
            return copyText(script.content).then(ok => toast(ok ? 'Script copied' : 'Could not copy to the clipboard', { kind: ok ? 'info' : 'error' }));
        }
        const action = event.target.closest('[data-detail]')?.dataset.detail;
        const job = currentJob();
        if (action === 'cancel') cancelJob(job);
        else if (action === 'resubmit') openResubmit(job, info && infoFor === job.id ? info.submission : null);
        else if (action === 'watch') toggleWatch(job);
        else if (action === 'back') $('#view-jobs').classList.remove('detail-open');
    });

    on('select', () => {
        if (state.selectedId !== infoFor) {
            headSignature = '';
            infoError = null;
            openLog = null;
            if (state.selectedId) loadInfo(state.selectedId);
        }
        render();
    });
    on('jobs', () => {
        announceFinished();
        refresh();
    });
}

export const detail = {
    focusFind: () => logView && logView.focusFind(),
    toggleFollow: () => logView && logView.toggleFollow(),
    logIsOpen: () => Boolean(state.selectedId) && ['output', 'errors'].includes(resolveTab()),
};
