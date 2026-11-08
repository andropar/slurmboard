const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

const source = fs.readFileSync(
    path.join(__dirname, '../slurm_dashboard/static/js/dashboard.js'), 'utf8'
);
const from = source.indexOf('function showLogPopup()');
const to = source.indexOf('function sortTable(', from);
assert.ok(from >= 0 && to > from);

function classList(initial = []) {
    const set = new Set(initial);
    return {
        add: (...names) => names.forEach(name => set.add(name)),
        remove: (...names) => names.forEach(name => set.delete(name)),
        toggle: (name, on) => (on ? set.add(name) : set.delete(name)),
        contains: name => set.has(name),
    };
}

function setup() {
    const streams = [];
    const card = { classList: classList(['widget-minimized']) };
    const closeButton = { focused: false, focus() { this.focused = true; } };
    const logPopup = { hidden: true, querySelector: () => closeButton };
    const row = { dataset: { logKey: 'train::42' }, classList: classList() };
    const context = vm.createContext({
        URLSearchParams,
        console,
        document: {
            getElementById: id => (id === 'log-panel' ? card : null),
            querySelectorAll: () => [row],
        },
        EventSource: class {
            constructor(url) { this.url = url; this.closed = false; streams.push(this); }
            close() { this.closed = true; }
        },
        logPopup,
        logPanel: { innerHTML: '', textContent: '', scrollTop: 0, scrollHeight: 0 },
        logTitle: { textContent: 'Logs' },
        logStream: null,
        currentLogKey: null,
        currentLogKind: null,
        originalLogContent: '',
        autoScroll: true,
        isNearBottom: () => true,
        clearLogSearch() {},
        getAnnotationsForCurrentLog: () => ({}),
        updateAnnotationCount() {},
        renderLogWithAnnotations() {},
    });
    vm.runInContext(source.slice(from, to), context);
    return { context, streams, card, closeButton, logPopup, row };
}

test('opening a log shows the pop-up and streams the selected file', () => {
    const f = setup();
    f.context.openLog('train::42', 'stderr');
    assert.equal(f.logPopup.hidden, false);
    assert.equal(f.card.classList.contains('widget-minimized'), false);
    assert.equal(f.closeButton.focused, true);
    assert.equal(f.context.logTitle.textContent, 'Logs: train::42 (stderr)');
    assert.equal(f.streams.length, 1);
    assert.equal(f.streams[0].url, '/stream_log?log_key=train%3A%3A42&kind=stderr');
    assert.equal(f.row.classList.contains('active-log'), true);
});

test('closing the pop-up stops streaming and clears the selection', () => {
    const f = setup();
    f.context.openLog('train::42', 'stdout');
    f.context.closeLogPopup();
    assert.equal(f.logPopup.hidden, true);
    assert.equal(f.streams[0].closed, true);
    assert.equal(f.context.logStream, null);
    assert.equal(f.context.currentLogKey, null);
    assert.equal(f.context.logTitle.textContent, 'Logs');
    assert.equal(f.row.classList.contains('active-log'), false);
});

test('switching logs replaces the previous stream', () => {
    const f = setup();
    f.context.openLog('train::42', 'stdout');
    f.context.openLog('train::42', 'stderr');
    assert.equal(f.streams.length, 2);
    assert.equal(f.streams[0].closed, true);
    assert.equal(f.streams[1].closed, false);
    assert.equal(f.logPopup.hidden, false);
});
