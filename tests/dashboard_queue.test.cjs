const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname, '../slurm_dashboard/static/js/queue.js'), 'utf8');

function setup(fetch = async () => ({ok: true, json: async () => ({jobs: [], errors: []})})) {
    const nodes = new Map();
    function element() {
        const attrs = new Map();
        const classes = new Set();
        return {
            textContent: '', innerHTML: '', value: '', hidden: false, disabled: false,
            dataset: {}, scrollTop: 0, scrollHeight: 50, listeners: new Map(),
            classList: {
                add: name => classes.add(name), remove: name => classes.delete(name),
                toggle(name, on) { if (on) classes.add(name); else classes.delete(name); },
            },
            setAttribute: (name, value) => attrs.set(name, value),
            getAttribute: name => attrs.get(name), focus() {}, scrollIntoView() {},
            addEventListener(name, callback) { this.listeners.set(name, callback); },
        };
    }
    const get = id => {
        if (!nodes.has(id)) nodes.set(id, element());
        return nodes.get(id);
    };
    get('page-size').value = '100';
    get('history').value = '7';
    const streams = [];
    class EventSource {
        constructor(url) { this.url = url; this.closed = false; streams.push(this); }
        close() { this.closed = true; }
    }
    const selectors = new Map();
    const document = {
        hidden: false, body: element(), documentElement: element(),
        getElementById: get, addEventListener() {},
        querySelectorAll: () => [],
        querySelector(selector) {
            if (!selectors.has(selector)) selectors.set(selector, element());
            return selectors.get(selector);
        },
    };
    const context = vm.createContext({
        document, fetch, console: {error() {}}, AbortController, EventSource, URLSearchParams,
        localStorage: {setItem() {}}, window: {addEventListener() {}}, setInterval() {},
    });
    // Keep startup calls out so each test controls its own network requests.
    vm.runInContext(source.slice(0, source.lastIndexOf("$('refresh').addEventListener")), context);
    return {context, get, streams, run: code => vm.runInContext(code, context)};
}

function records() {
    return Array.from({length: 212}, (_, index) => ({
        id: String(1000 + index), name: `job-${211 - index}`,
        status: index % 2 ? 'RUNNING' : 'COMPLETED',
        queued: new Date(Date.UTC(2026, 10, 1, 0, index)).toISOString(),
        started: index % 2 ? null : new Date(Date.UTC(2026, 10, 2, 0, index)).toISOString(),
        ended: index % 2 ? null : new Date(Date.UTC(2026, 10, 3, 0, index)).toISOString(),
        log_key: `job::${1000 + index}`,
    }));
}

test('all data columns sort globally in both directions with missing timestamps last', () => {
    const fixture = setup();
    const jobs = records();
    for (const key of ['id', 'name', 'status', 'queued', 'started', 'ended']) {
        for (const direction of [-1, 1]) {
            const rows = fixture.context.selectJobs(jobs, '', 'ALL', '', '', key, direction);
            assert.equal(rows.length, 212);
            let missing = false;
            let previous;
            for (const row of rows) {
                if (!row[key]) { missing = true; continue; }
                assert.equal(missing, false, `missing ${key} must remain last`);
                if (previous !== undefined) {
                    assert.ok(previous.localeCompare(row[key], undefined, {numeric: true}) * direction <= 0);
                }
                previous = row[key];
            }
        }
    }
});

test('pagination, boundary controls, page size and filter resets', () => {
    const {context, get, run} = setup();
    context.records = records();
    run('jobs = records; queueLoading = false; render()');
    assert.match(get('results').textContent, /1–100 of 212/);
    assert.equal(get('previous-page').disabled, true);
    get('next-page').listeners.get('click')();
    assert.match(get('results').textContent, /101–200 of 212/);
    get('last-page').listeners.get('click')();
    assert.match(get('results').textContent, /201–212 of 212/);
    assert.equal(get('next-page').disabled, true);
    get('search').value = 'job-211';
    get('search').listeners.get('input')();
    assert.equal(get('page-status').textContent, 'Page 1 of 1');
    assert.match(get('results').textContent, /1–1 of 1/);
    get('search').value = 'nonexistent';
    get('search').listeners.get('input')();
    assert.equal(get('empty').hidden, false);
    assert.equal(get('next-page').disabled, true);
    get('clear').listeners.get('click')();
    get('page-size').value = '10';
    get('page-size').listeners.get('change')();
    assert.equal(get('page-status').textContent, 'Page 1 of 22');
});

test('filters use actual UTC queued times and include Slurm failure variants', () => {
    const {context} = setup();
    const jobs = [
        {id:'1', name:'OOM', status:'OUT_OF_MEMORY', queued:'2026-11-09T23:59:59Z'},
        {id:'2', name:'timeout', status:'TIMEOUT', queued:'2026-11-10T00:00:00Z'},
        {id:'3', name:'unknown', status:'UNKNOWN', queued:null},
    ];
    assert.equal(context.selectJobs(jobs, '', 'FAILED', '', '', 'id', 1).length, 2);
    const filtered = context.selectJobs(jobs, '', 'ALL', '2026-11-09', '2026-11-09', 'id', 1);
    assert.equal(filtered.length, 1);
    assert.equal(filtered[0].id, '1');
});

test('HTML-like names and timestamps are escaped instead of executed', () => {
    const {context, get, run} = setup();
    context.records = [{id:'123', name:'<img src=x onerror=alert(1)>', status:'<script>', queued:'"><img src=x>', log_key:'x::123'}];
    run('jobs = records; render()');
    assert.ok(!get('jobs').innerHTML.includes('<img'));
    assert.match(get('jobs').innerHTML, /&lt;img/);
    assert.match(get('jobs').innerHTML, /state unknown/);
});

test('slow scripts cannot overwrite newer selection, errors and truncation are explicit', async () => {
    let resolve;
    const first = new Promise(done => { resolve = done; });
    const fixture = setup(async url => url.endsWith('/1') ? first : ({
        ok:true, json: async () => ({source:'accounting', content:'<literal script>', truncated:true, max_lines:200, max_bytes:1048576}),
    }));
    fixture.context.records = [{id:'1', name:'one'}, {id:'2', name:'two'}];
    fixture.run('jobs = records');
    const pending = fixture.context.openViewer('1', 'script');
    await fixture.context.openViewer('2', 'script');
    resolve({ok:true, json:async () => ({content:'old', source:'controller'})});
    await pending;
    assert.equal(fixture.get('file-content').textContent, '<literal script>');
    assert.match(fixture.get('viewer-message').textContent, /accounting.*truncated at 200/);
    assert.equal(fixture.streams.length, 0);
});

test('script HTTP errors never fall back to source files', async () => {
    let calls = 0;
    const fixture = setup(async () => {
        calls++;
        return {ok:false, status:404, json:async () => ({error:'Retained script unavailable', attempts:[{source:'controller', message:'Purged'}]})};
    });
    fixture.context.records = [{id:'1', name:'one'}];
    fixture.run('jobs = records');
    await fixture.context.openViewer('1', 'script');
    assert.equal(calls, 1);
    assert.match(fixture.get('viewer-message').textContent, /unavailable\ncontroller: Purged/);
    assert.equal(fixture.get('retry-file').hidden, false);
    assert.equal(fixture.get('file-content').textContent, '');
});

test('log stream survives table refresh, follows output, bounds memory and closes on selection change', async () => {
    const fixture = setup(async () => ({
        ok:true, json:async () => ({jobs:[{id:'1', name:'one', status:'COMPLETED', log_key:'actual-dir::1'}],errors:[],scope:'7 days',user:'alice'}),
    }));
    fixture.context.records = [{id:'1', name:'one', status:'RUNNING', log_key:'actual-dir::1'}];
    fixture.run('jobs = records');
    await fixture.context.openViewer('1', 'stdout');
    const stream = fixture.streams[0];
    assert.match(stream.url, /log_key=actual-dir%3A%3A1/);
    stream.onmessage({data:JSON.stringify({snapshot:'<literal log>'})});
    assert.equal(fixture.get('file-content').textContent, '<literal log>');
    stream.onmessage({data:JSON.stringify({append:'x'.repeat(210000)})});
    assert.equal(fixture.get('file-content').textContent.length, 200000);
    await fixture.context.refreshJobs();
    assert.equal(stream.closed, false);
    assert.match(fixture.get('viewer-id').textContent, /COMPLETED/);
    await fixture.context.openViewer('1', 'stderr');
    assert.equal(stream.closed, true);
    fixture.streams[1].onerror();
    assert.match(fixture.get('viewer-message').textContent, /unavailable or connection lost/);
    assert.equal(fixture.get('retry-file').hidden, false);
    fixture.context.closeViewer();
    assert.equal(fixture.streams[1].closed, true);
});

test('network failures retain prior queue data with a stale warning', async () => {
    const fixture = setup(async () => { throw new Error('offline'); });
    fixture.context.records = records();
    fixture.run('jobs = records; page = 2');
    await fixture.context.refreshJobs();
    assert.match(fixture.get('queue-error').textContent, /offline.*previous data.*stale/);
    assert.match(fixture.get('results').textContent, /101–200 of 212/);
    assert.equal(fixture.get('refresh').disabled, false);
});
