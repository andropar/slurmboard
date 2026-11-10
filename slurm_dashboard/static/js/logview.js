// Live log viewer: streams a job's output, flags problems, and searches what is loaded.

import { $, copyText, esc, fmtBytes, html, setHTML, store } from './util.js';
import { jobPath } from './api.js';

const MAX_LINES = 20000;
const MAX_LINE_CHARS = 5000;
const FULL_LOAD_LIMIT = 8_000_000;
const KEPT_PER_ISSUE = 500;

// Colour codes and cursor movement that terminals interpret but a page cannot.
const ANSI = /\x1b\[[0-9;?]*[ -/]*[@-~]|\x1b[@-Z\\-_]/g;

// First match wins, so specific causes come before the generic "Error".
const ISSUES = [
    { type: 'oom', label: 'Out of memory', level: 'error', test: /out of memory|oom[-_]kill|Cannot allocate memory|MemoryError/i },
    { type: 'timelimit', label: 'Time limit reached', level: 'error', test: /DUE TO TIME LIMIT/ },
    { type: 'traceback', label: 'Python traceback', level: 'error', test: /Traceback \(most recent call last\)/ },
    { type: 'cuda', label: 'CUDA error', level: 'error', test: /CUDA error|cudaError|device-side assert|NCCL error|cuDNN error/i },
    { type: 'segfault', label: 'Segmentation fault', level: 'error', test: /Segmentation fault|core dumped|SIGSEGV/i },
    { type: 'slurm', label: 'Slurm error', level: 'error', test: /slurmstepd: error|srun: error|NODE_FAIL|CANCELLED AT/ },
    { type: 'killed', label: 'Killed', level: 'error', test: /^Killed\b|\bKilled$/ },
    { type: 'permission', label: 'Permission denied', level: 'error', test: /Permission denied|Operation not permitted/i },
    { type: 'missing', label: 'Not found', level: 'error', test: /No such file or directory|FileNotFoundError|ModuleNotFoundError|command not found/i },
    { type: 'error', label: 'Error', level: 'error', test: /\b(Error|ERROR|Exception|FATAL|Fatal)\b/ },
    { type: 'warning', label: 'Warning', level: 'warn', test: /\bwarn(ing)?\b|Warning:/i },
];

/** What a terminal would show for this line: last carriage-return segment, no escape codes. */
function clean(text) {
    if (text.endsWith('\r')) text = text.slice(0, -1);
    const cr = text.lastIndexOf('\r');
    if (cr >= 0) text = text.slice(cr + 1);
    text = text.replace(ANSI, '');
    if (text.length > MAX_LINE_CHARS) {
        text = `${text.slice(0, MAX_LINE_CHARS)} … (+${(text.length - MAX_LINE_CHARS).toLocaleString()} characters)`;
    }
    return text;
}

export class LogView {
    constructor(root) {
        this.root = root;
        setHTML(root, html`
            <div class="log-bar">
                <div class="log-issues"></div>
                <div class="log-tools">
                    <div class="find">
                        <input type="search" placeholder="Find in log" aria-label="Find in log" spellcheck="false">
                        <span class="find-n" aria-live="polite"></span>
                        <button class="icon-btn" data-log="prev" title="Previous match (Shift+Enter)" aria-label="Previous match">↑</button>
                        <button class="icon-btn" data-log="next" title="Next match (Enter)" aria-label="Next match">↓</button>
                    </div>
                    <button class="toggle" data-log="wrap" title="Wrap long lines">Wrap</button>
                    <button class="toggle" data-log="follow" title="Stay at the newest line (f)">Follow</button>
                </div>
            </div>
            <div class="log-note" hidden></div>
            <div class="log-body" tabindex="0">
                <div class="log-lines"></div>
                <div class="log-msg" hidden></div>
            </div>`);
        this.issuesEl = $('.log-issues', root);
        this.noteEl = $('.log-note', root);
        this.bodyEl = $('.log-body', root);
        this.linesEl = $('.log-lines', root);
        this.msgEl = $('.log-msg', root);
        this.findInput = $('.find input', root);
        this.findCount = $('.find-n', root);

        this.follow = true;
        this.wrap = store.get('wrap', true);
        this.stream = null;
        this.resetContent();
        this.syncToggles();

        root.addEventListener('click', event => this.onClick(event));
        this.bodyEl.addEventListener('scroll', () => this.onScroll(), { passive: true });
        let timer;
        this.findInput.addEventListener('input', () => {
            clearTimeout(timer);
            timer = setTimeout(() => this.setFind(this.findInput.value), 180);
        });
        this.findInput.addEventListener('keydown', event => {
            if (event.key === 'Enter') {
                event.preventDefault();
                this.step(event.shiftKey ? -1 : 1);
            } else if (event.key === 'Escape' && this.findInput.value) {
                event.stopPropagation();
                this.findInput.value = '';
                this.setFind('');
            }
        });
    }

    // ------------------------------------------------------------- lifecycle

    /** Start showing a job's stdout or stderr. `live` keeps the stream open for new output. */
    open(jobId, kind, { live = true } = {}) {
        this.close();
        this.jobId = jobId;
        this.kind = kind;
        this.live = live;
        this.follow = true;
        this.path = null;
        this.findInput.value = '';
        this.query = null;
        this.resetContent();
        this.syncToggles();
        this.root.classList.remove('no-log');
        this.showMessage('Loading…');
        this.connect(false);
    }

    close() {
        if (this.stream) {
            this.stream.close();
            this.stream = null;
        }
    }

    connect(full) {
        const params = new URLSearchParams({ log_key: this.jobId, kind: this.kind });
        if (full) params.set('full', '1');
        const stream = new EventSource(`/stream_log?${params}`);
        this.stream = stream;
        stream.onmessage = event => {
            if (this.stream !== stream) return;
            let payload;
            try {
                payload = JSON.parse(event.data);
            } catch (err) {
                return;
            }
            if (payload.error) {
                this.resetContent();
                this.noteEl.hidden = true;
                this.root.classList.add('no-log');
                this.showMessage(payload.error);
                stream.close();
                return;
            }
            if (payload.path) this.path = payload.path;
            if (payload.reset) {
                this.resetContent();
                this.showMessage('The file was rewritten. Reloading…');
            }
            if ('snapshot' in payload) this.onSnapshot(payload);
            if (payload.append) this.append(payload.append);
        };
        stream.onerror = () => {
            // While the stream is merely interrupted the browser reconnects by itself.
            if (stream.readyState === EventSource.CLOSED && this.stream === stream) this.stream = null;
        };
    }

    resetContent() {
        this.linesEl.textContent = '';
        this.linesEl.style.counterReset = 'ln 0';
        this.count = 0;
        this.base = 0;            // number of the line before the first one shown
        this.partial = '';
        this.partialEl = null;
        this.inTrace = false;
        this.issues = new Map();  // type -> { def, count, els, cursor }
        this.hits = [];
        this.hitIndex = -1;
        this.issuesDirty = false;
        this.findCount.textContent = '';
        setHTML(this.issuesEl, '');
        this.msgEl.hidden = true;
    }

    showMessage(text) {
        this.msgEl.textContent = text;
        this.msgEl.hidden = false;
    }

    /** Stop streaming and show an explanation instead of a log. */
    showOnly(text) {
        this.close();
        this.jobId = null;
        this.resetContent();
        this.noteEl.hidden = true;
        this.root.classList.add('no-log');
        this.showMessage(text);
    }

    onSnapshot(payload) {
        this.resetContent();
        this.size = payload.size;
        this.truncated = payload.truncated;
        this.numbered = payload.first_line !== null && payload.first_line !== undefined;
        this.base = this.numbered ? payload.first_line - 1 : 0;
        this.linesEl.style.counterReset = `ln ${this.base}`;
        this.linesEl.classList.toggle('no-num', !this.numbered);
        this.renderNote();
        if (payload.snapshot) {
            this.append(payload.snapshot);
        } else {
            this.showMessage(this.live ? 'Nothing written yet. New output appears here as it arrives.' : 'This file is empty.');
        }
        if (this.follow) this.bodyEl.scrollTop = this.bodyEl.scrollHeight;
        // A finished job's file no longer grows, so the connection can be released.
        if (!this.live && this.stream) {
            this.stream.close();
            this.stream = null;
        }
    }

    renderNote() {
        if (!this.path) {
            this.noteEl.hidden = true;
            return;
        }
        const canLoadAll = this.truncated && this.size <= FULL_LOAD_LIMIT;
        setHTML(this.noteEl, html`
            <span class="log-path mono" title="${this.path}">\u200e${this.path}</span>
            <span class="log-size">${fmtBytes(this.size)}</span>
            ${this.truncated ? html`<span class="log-trunc">showing the end only</span>` : ''}
            ${canLoadAll ? html`<button class="link" data-log="full">Load whole file</button>` : ''}
            <button class="link" data-log="copy-path">Copy path</button>
            <a class="link" href="/api/log_file/${jobPath(this.jobId)}?kind=${this.kind}" download>Download</a>`);
        this.noteEl.hidden = false;
    }

    // --------------------------------------------------------------- content

    append(text) {
        const data = this.partial + text;
        const pieces = data.split('\n');
        this.partial = pieces.pop();
        // Progress bars rewrite one line with carriage returns; keep only the newest state.
        const cr = this.partial.lastIndexOf('\r', this.partial.length - 2);
        if (cr >= 0) this.partial = this.partial.slice(cr + 1);

        if (this.partialEl) {
            this.partialEl.remove();
            this.partialEl = null;
            this.count--;
        }
        const fragment = document.createDocumentFragment();
        for (const piece of pieces) fragment.appendChild(this.makeLine(piece, true));
        if (this.partial) {
            this.partialEl = this.makeLine(this.partial, false);
            fragment.appendChild(this.partialEl);
        }
        this.msgEl.hidden = true;
        this.linesEl.appendChild(fragment);

        if (this.count > MAX_LINES) {
            const excess = this.count - MAX_LINES;
            for (let i = 0; i < excess; i++) this.linesEl.firstChild.remove();
            this.count -= excess;
            this.base += excess;
            this.linesEl.style.counterReset = `ln ${this.base}`;
            this.hits = this.hits.filter(mark => mark.isConnected);
        }
        if (this.query) this.updateFindCount();
        this.scheduleIssues();
        if (this.follow) this.bodyEl.scrollTop = this.bodyEl.scrollHeight;
    }

    makeLine(rawText, complete) {
        const text = clean(rawText);
        const el = document.createElement('div');
        el.className = 'ln';
        el.textContent = text;
        this.count++;
        if (complete) this.classify(el, text);
        if (this.query && text) this.markLine(el, text);
        return el;
    }

    classify(el, text) {
        let hit = ISSUES.find(issue => issue.test.test(text));
        if (this.inTrace) {
            // Indented lines are the stack; the first unindented one is the exception itself.
            el.classList.add('lvl-error');
            if (/^\s/.test(text) || text === '') return;
            this.inTrace = false;
            if (!hit || hit.type === 'error' || hit.type === 'warning') return;
        }
        if (!hit) return;
        if (hit.type === 'traceback') this.inTrace = true;
        el.classList.add(`lvl-${hit.level}`);
        let entry = this.issues.get(hit.type);
        if (!entry) {
            entry = { def: hit, count: 0, els: [], cursor: -1 };
            this.issues.set(hit.type, entry);
        }
        entry.count++;
        if (entry.els.length < KEPT_PER_ISSUE) entry.els.push(el);
        this.issuesDirty = true;
    }

    scheduleIssues() {
        if (!this.issuesDirty) return;
        this.issuesDirty = false;
        const entries = [...this.issues.values()].sort((a, b) => (a.def.level === b.def.level ? 0 : a.def.level === 'error' ? -1 : 1));
        setHTML(this.issuesEl, entries.map(entry => html`
            <button class="issue lvl-${entry.def.level}" data-issue="${entry.def.type}" title="Jump to the next ${entry.def.label.toLowerCase()} line">
                <i></i>${entry.def.label}${entry.count > 1 ? html`<span class="issue-n">${entry.count.toLocaleString()}</span>` : ''}
            </button>`));
    }

    // ------------------------------------------------------------------ find

    setFind(text) {
        for (const mark of this.hits) {
            const line = mark.parentNode;
            if (line && line.isConnected) line.textContent = line.textContent;
        }
        this.hits = [];
        this.hitIndex = -1;
        this.query = null;
        text = text.trim();
        if (!text) {
            this.findCount.textContent = '';
            return;
        }
        const asRegex = /^\/(.+)\/$/.exec(text);
        try {
            this.query = new RegExp(asRegex ? asRegex[1] : text.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'gi');
        } catch (err) {
            this.findCount.textContent = 'invalid pattern';
            return;
        }
        for (const line of this.linesEl.children) {
            const content = line.textContent;
            if (content) this.markLine(line, content);
        }
        this.updateFindCount();
        if (this.hits.length) this.step(this.follow ? -1 : 1);
    }

    markLine(el, text) {
        this.query.lastIndex = 0;
        let out = '';
        let last = 0;
        let match;
        while ((match = this.query.exec(text)) !== null) {
            if (match[0] === '') {
                this.query.lastIndex++;
                continue;
            }
            out += `${esc(text.slice(last, match.index))}<mark>${esc(match[0])}</mark>`;
            last = match.index + match[0].length;
        }
        if (!last) return;
        el.innerHTML = out + esc(text.slice(last));
        this.hits.push(...el.querySelectorAll('mark'));
    }

    updateFindCount() {
        const total = this.hits.length;
        this.findCount.textContent = total ? `${this.hitIndex >= 0 ? this.hitIndex + 1 : '–'} of ${total.toLocaleString()}` : 'no matches';
    }

    step(delta) {
        if (!this.hits.length) return;
        this.hits[this.hitIndex]?.classList.remove('cur');
        if (this.hitIndex === -1) this.hitIndex = delta > 0 ? 0 : this.hits.length - 1;
        else this.hitIndex = (this.hitIndex + delta + this.hits.length) % this.hits.length;
        const mark = this.hits[this.hitIndex];
        mark.classList.add('cur');
        this.setFollow(false);
        mark.scrollIntoView({ block: 'center' });
        this.updateFindCount();
    }

    focusFind() {
        this.findInput.focus();
        this.findInput.select();
    }

    // -------------------------------------------------------------- controls

    onClick(event) {
        const issue = event.target.closest('[data-issue]');
        if (issue) {
            const entry = this.issues.get(issue.dataset.issue);
            const live = entry.els.filter(el => el.isConnected);
            if (!live.length) return;
            entry.cursor = (entry.cursor + 1) % live.length;
            this.setFollow(false);
            const line = live[entry.cursor];
            line.scrollIntoView({ block: 'center' });
            line.classList.remove('flash');
            void line.offsetWidth;   // restart the animation when the same line is hit again
            line.classList.add('flash');
            return;
        }
        const action = event.target.closest('[data-log]')?.dataset.log;
        if (action === 'follow') this.setFollow(!this.follow);
        else if (action === 'wrap') {
            this.wrap = !this.wrap;
            store.set('wrap', this.wrap);
            this.syncToggles();
        } else if (action === 'prev') this.step(-1);
        else if (action === 'next') this.step(1);
        else if (action === 'full') {
            this.close();
            this.resetContent();
            this.showMessage('Loading the whole file…');
            this.connect(true);
        } else if (action === 'copy-path') {
            copyText(this.path).then(ok => {
                event.target.textContent = ok ? 'Copied' : 'Copy failed';
                setTimeout(() => { event.target.textContent = 'Copy path'; }, 1500);
            });
        }
    }

    onScroll() {
        const gap = this.bodyEl.scrollHeight - this.bodyEl.scrollTop - this.bodyEl.clientHeight;
        // Scrolling up pauses following; returning to the end resumes it.
        if (gap > 48 && this.follow) this.setFollow(false, { scroll: false });
        else if (gap <= 2 && !this.follow) this.setFollow(true, { scroll: false });
    }

    setFollow(on, { scroll = true } = {}) {
        this.follow = on;
        if (on && scroll) this.bodyEl.scrollTop = this.bodyEl.scrollHeight;
        this.syncToggles();
    }

    toggleFollow() {
        this.setFollow(!this.follow);
    }

    syncToggles() {
        const follow = $('[data-log="follow"]', this.root);
        follow.classList.toggle('on', this.follow);
        follow.setAttribute('aria-pressed', this.follow);
        const wrap = $('[data-log="wrap"]', this.root);
        wrap.classList.toggle('on', this.wrap);
        wrap.setAttribute('aria-pressed', this.wrap);
        this.bodyEl.classList.toggle('nowrap', !this.wrap);
    }
}
