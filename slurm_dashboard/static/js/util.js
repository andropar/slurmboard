// Small helpers shared by every module: DOM access, safe HTML templating, formatting.

export const $ = (selector, root = document) => root.querySelector(selector);
export const $$ = (selector, root = document) => [...root.querySelectorAll(selector)];

// ---------------------------------------------------------------- templating

const ESCAPES = { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' };
export const esc = value => String(value ?? '').replace(/[&<>"']/g, ch => ESCAPES[ch]);

class Safe {
    constructor(text) { this.text = text; }
    toString() { return this.text; }
}

/** Mark a string as already-safe HTML. */
export const raw = text => new Safe(String(text ?? ''));

function part(value) {
    if (value === null || value === undefined || value === false || value === true) return '';
    if (value instanceof Safe) return value.text;
    if (Array.isArray(value)) return value.map(part).join('');
    return esc(value);
}

/**
 * Tagged template that escapes every interpolated value unless it is itself
 * the result of `html` or `raw`. Job names, paths and log text come from the
 * cluster and must never be inserted unescaped.
 */
export function html(strings, ...values) {
    let out = strings[0];
    values.forEach((value, i) => { out += part(value) + strings[i + 1]; });
    return new Safe(out);
}

export function setHTML(element, template) {
    element.innerHTML = part(template);
}

// ---------------------------------------------------------------- formatting

const pad = n => String(n).padStart(2, '0');

/** Seconds as a stopwatch: 2:31:05, 14:27, 1-02:03:04. */
export function fmtClock(seconds) {
    seconds = Math.max(0, Math.floor(seconds || 0));
    const days = Math.floor(seconds / 86400);
    const h = Math.floor((seconds % 86400) / 3600);
    const m = Math.floor((seconds % 3600) / 60);
    const s = seconds % 60;
    if (days) return `${days}-${pad(h)}:${pad(m)}:${pad(s)}`;
    return h ? `${h}:${pad(m)}:${pad(s)}` : `${m}:${pad(s)}`;
}

/** Seconds in words: 45s, 56m, 2h 31m, 3d 4h. */
export function fmtDuration(seconds) {
    seconds = Math.max(0, Math.round(seconds || 0));
    if (seconds < 60) return `${seconds}s`;
    if (seconds < 3600) {
        const s = seconds % 60;
        return seconds < 600 && s ? `${Math.floor(seconds / 60)}m ${s}s` : `${Math.floor(seconds / 60)}m`;
    }
    if (seconds < 86400) {
        const m = Math.floor((seconds % 3600) / 60);
        return m ? `${Math.floor(seconds / 3600)}h ${m}m` : `${Math.floor(seconds / 3600)}h`;
    }
    const h = Math.floor((seconds % 86400) / 3600);
    return h ? `${Math.floor(seconds / 86400)}d ${h}h` : `${Math.floor(seconds / 86400)}d`;
}

const sameDay = (a, b) => a.toDateString() === b.toDateString();

/** "19:37" today, "10 Nov 19:37" otherwise. */
export function fmtTime(iso) {
    const date = new Date(iso);
    if (!iso || isNaN(date)) return '';
    const clock = date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    if (sameDay(date, new Date())) return clock;
    return `${date.toLocaleDateString([], { day: 'numeric', month: 'short' })} ${clock}`;
}

/** A start and end time without repeating the date when both fall on one day. */
export function fmtRange(startIso, endIso) {
    const start = new Date(startIso);
    const end = new Date(endIso);
    if (isNaN(start) || isNaN(end)) return '';
    const clock = date => date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    return `${fmtTime(startIso)} → ${sameDay(start, end) ? clock(end) : fmtTime(endIso)}`;
}

/** Full date and time for tooltips and the details tab. */
export function fmtDateTime(iso) {
    const date = new Date(iso);
    if (!iso || isNaN(date)) return '';
    return date.toLocaleString([], { day: 'numeric', month: 'short', year: 'numeric', hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

export function fmtRelative(iso) {
    const date = new Date(iso);
    if (!iso || isNaN(date)) return '';
    const seconds = (Date.now() - date.getTime()) / 1000;
    if (seconds < 60) return 'just now';
    if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
    if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
    return `${Math.floor(seconds / 86400)}d ago`;
}

/** Heading for a day in the finished list: Today, Yesterday, Mon 9 Nov. */
export function dayLabel(iso) {
    const date = new Date(iso);
    if (!iso || isNaN(date)) return 'Earlier';
    const today = new Date();
    if (sameDay(date, today)) return 'Today';
    const yesterday = new Date(today);
    yesterday.setDate(today.getDate() - 1);
    if (sameDay(date, yesterday)) return 'Yesterday';
    return date.toLocaleDateString([], { weekday: 'short', day: 'numeric', month: 'short' });
}

export function fmtBytes(bytes) {
    if (bytes === null || bytes === undefined) return '';
    if (bytes >= 1024 ** 3) return `${(bytes / 1024 ** 3).toFixed(1)} GB`;
    if (bytes >= 1024 ** 2) return `${(bytes / 1024 ** 2).toFixed(bytes < 10 * 1024 ** 2 ? 1 : 0)} MB`;
    if (bytes >= 1024) return `${Math.round(bytes / 1024)} KB`;
    return `${bytes} B`;
}

export function fmtNumber(value) {
    if (value === null || value === undefined) return '–';
    if (value >= 1e6) return `${(value / 1e6).toFixed(1)}M`;
    if (value >= 1e4) return `${(value / 1e3).toFixed(1)}K`;
    return Number(value).toLocaleString(undefined, { maximumFractionDigits: value < 100 ? 1 : 0 });
}

export const fmtPercent = value => (value === null || value === undefined ? '–' : `${Math.round(value)}%`);

// ----------------------------------------------------------------- job state

const STATE_LABELS = {
    RUNNING: 'Running', PENDING: 'Pending', COMPLETED: 'Completed', FAILED: 'Failed',
    CANCELLED: 'Cancelled', TIMEOUT: 'Timed out', OUT_OF_MEMORY: 'Out of memory',
    NODE_FAIL: 'Node failed', PREEMPTED: 'Preempted', COMPLETING: 'Finishing',
    CONFIGURING: 'Starting', SUSPENDED: 'Suspended', REQUEUED: 'Requeued',
    BOOT_FAIL: 'Boot failed', DEADLINE: 'Deadline missed', UNKNOWN: 'Logs only',
};

export const CATEGORY_LABELS = {
    running: 'running', pending: 'pending', completed: 'completed', failed: 'failed',
    timeout: 'timed out', cancelled: 'cancelled', unknown: 'unknown',
};

export function stateLabel(state) {
    const key = String(state || 'UNKNOWN').toUpperCase();
    return STATE_LABELS[key] || key.charAt(0) + key.slice(1).toLowerCase().replace(/_/g, ' ');
}

/** A coloured dot plus the state in words. The word carries the meaning. */
export function stateBadge(job) {
    return html`<span class="state s-${job.state_category || 'unknown'}"><i></i>${stateLabel(job.state)}</span>`;
}

// ---------------------------------------------------------------------- misc

export function debounce(fn, ms) {
    let timer;
    return (...args) => {
        clearTimeout(timer);
        timer = setTimeout(() => fn(...args), ms);
    };
}

export async function copyText(text) {
    try {
        await navigator.clipboard.writeText(text);
        return true;
    } catch (err) {
        // Clipboard access needs a secure context; plain-HTTP hosts other than localhost lack one.
        const area = document.createElement('textarea');
        area.value = text;
        area.style.position = 'fixed';
        area.style.opacity = '0';
        document.body.appendChild(area);
        area.select();
        const ok = document.execCommand('copy');
        area.remove();
        return ok;
    }
}

export const store = {
    get(key, fallback) {
        try {
            const value = localStorage.getItem(`slurmdash.${key}`);
            return value === null ? fallback : JSON.parse(value);
        } catch (err) {
            return fallback;
        }
    },
    set(key, value) {
        try {
            localStorage.setItem(`slurmdash.${key}`, JSON.stringify(value));
        } catch (err) {
            // Private mode or a full quota: preferences just do not persist.
        }
    },
};

// ------------------------------------------------------------------- tooltips

let tipEl = null;

/** Show the shared tooltip above (or below) an element. */
export function showTip(anchor, template) {
    if (!tipEl) {
        tipEl = document.createElement('div');
        tipEl.className = 'tip';
        tipEl.setAttribute('role', 'tooltip');
        document.body.appendChild(tipEl);
    }
    setHTML(tipEl, template);
    tipEl.hidden = false;
    const box = anchor.getBoundingClientRect();
    const tip = tipEl.getBoundingClientRect();
    const left = Math.max(8, Math.min(window.innerWidth - tip.width - 8, box.left + box.width / 2 - tip.width / 2));
    const above = box.top - tip.height - 8;
    tipEl.style.left = `${left}px`;
    tipEl.style.top = `${above < 8 ? box.bottom + 8 : above}px`;
}

export function hideTip() {
    if (tipEl) tipEl.hidden = true;
}

/** Tooltips for every `selector` inside `root`, on hover and on keyboard focus alike. */
export function bindTips(root, selector, build) {
    const show = event => {
        const target = event.target.closest(selector);
        if (target && root.contains(target)) showTip(target, build(target));
    };
    const hide = event => {
        if (event.target.closest(selector)) hideTip();
    };
    root.addEventListener('pointerover', show);
    root.addEventListener('pointerout', hide);
    root.addEventListener('focusin', show);
    root.addEventListener('focusout', hide);
}
