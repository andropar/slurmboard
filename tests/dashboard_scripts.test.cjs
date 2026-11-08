const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

const source = fs.readFileSync(
    path.join(__dirname, '../slurm_dashboard/static/js/dashboard.js'), 'utf8'
);
const from = source.indexOf('function closeScriptModal()');
const to = source.indexOf('function showResubmitDialog(', from);
assert.ok(from >= 0 && to > from);

function setup(fetch) {
    const nodes = new Map();
    const listeners = new Map();
    function element() {
        const children = new Map([
            ['.modal-close', { focus() {} }],
            ['.script-status', { textContent: '' }],
            ['.script-content', { textContent: '', hidden: true }],
        ]);
        return {
            innerHTML: '',
            addEventListener(type, handler) { listeners.set(type, handler); },
            querySelector(selector) { return children.get(selector); },
            remove() { if (nodes.get(this.id) === this) nodes.delete(this.id); },
        };
    }
    const document = {
        body: { appendChild(node) { nodes.set(node.id, node); } },
        createElement: element,
        getElementById: id => nodes.get(id),
        addEventListener(type, handler) { listeners.set(type, handler); },
    };
    let logClosed = false;
    const context = vm.createContext({
        document, fetch, console: { error() {} },
        logStream: { close() { logClosed = true; } },
    });
    vm.runInContext(source.slice(from, to), context);
    return { context, nodes, listeners, logClosed: () => logClosed };
}

test('submitted script displays literal content without changing the log stream', async () => {
    const content = '#!/bin/bash\necho "<script>alert(1)</script>"\n';
    const fixture = setup(async url => {
        assert.equal(url, '/api/submitted_script/123_4');
        return { ok: true, json: async () => ({
            source: 'controller', content, truncated: false,
        }) };
    });
    await fixture.context.viewSubmittedScript('123_4');
    const modal = fixture.nodes.get('script-modal');
    assert.equal(modal.querySelector('.script-content').textContent, content);
    assert.equal(modal.querySelector('.script-content').hidden, false);
    assert.match(modal.querySelector('.script-status').textContent, /Slurm controller/);
    assert.match(modal.innerHTML, /not the current source file/);
    assert.equal(fixture.logClosed(), false);
    fixture.listeners.get('keydown')({ key: 'Escape' });
    assert.equal(fixture.nodes.size, 0);
});

test('truncation and accounting provenance are visible', async () => {
    const fixture = setup(async () => ({
        ok: true, json: async () => ({
            source: 'accounting', content: '#!/bin/bash\n', truncated: true,
            max_lines: 200, max_bytes: 1048576,
        }),
    }));
    await fixture.context.viewSubmittedScript('123');
    assert.match(fixture.nodes.get('script-modal').querySelector('.script-status').textContent,
        /Slurm accounting.*truncated at 200 lines or 1048576 bytes/);
});

for (const status of [403, 404, 501, 502, 504]) {
    test(`HTTP ${status} shows the error instead of a source-file fallback`, async () => {
        let calls = 0;
        const fixture = setup(async () => {
            calls++;
            return {
                ok: false, status,
                json: async () => ({
                    error: 'Submitted script unavailable <detail>',
                    attempts: [{ source: 'controller', message: 'Permission denied' }],
                }),
            };
        });
        await fixture.context.viewSubmittedScript('123');
        const modal = fixture.nodes.get('script-modal');
        assert.match(modal.querySelector('.script-status').textContent, /<detail>.*\ncontroller: Permission denied/);
        assert.equal(modal.querySelector('.script-content').hidden, true);
        assert.equal(calls, 1);
    });
}

test('network errors leave an explicit retry message', async () => {
    const fixture = setup(async () => { throw new Error('offline'); });
    await fixture.context.viewSubmittedScript('123');
    assert.match(fixture.nodes.get('script-modal').querySelector('.script-status').textContent,
        /network or response error.*retry/);
});

test('slow response cannot replace a newer selected job', async () => {
    let resolve;
    const first = new Promise(done => { resolve = done; });
    const fixture = setup(async url => {
        if (url.endsWith('/123')) return first;
        return { ok: true, json: async () => ({
            source: 'controller', content: 'newer', truncated: false,
        }) };
    });
    const pending = fixture.context.viewSubmittedScript('123');
    await fixture.context.viewSubmittedScript('456');
    resolve({ ok: true, json: async () => ({
        source: 'controller', content: 'older', truncated: false,
    }) });
    await pending;
    assert.equal(fixture.nodes.get('script-modal').querySelector('.script-content').textContent, 'newer');
});

test('running, recent and modal actions reach the submitted-script viewer', () => {
    for (const [start, end] of [
        ['function renderRunning(', 'function renderRecent('],
        ['function renderRecent(', 'async function toggleJobExpand('],
        ['async function openJobDetailsModal(', 'function closeJobDetailsModal('],
    ]) {
        const section = source.slice(source.indexOf(start), source.indexOf(end));
        assert.match(section, /viewSubmittedScript\(/);
    }
    assert.match(source, /View current source/);
    assert.match(source, /This source file may have changed since submission/);
});
