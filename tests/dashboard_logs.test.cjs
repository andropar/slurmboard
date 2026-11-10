const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

const root = path.resolve(__dirname, '..');
const source = fs.readFileSync(
    path.join(root, 'slurm_dashboard/static/js/dashboard.js'), 'utf8'
);
const template = fs.readFileSync(
    path.join(root, 'slurm_dashboard/templates/index.html'), 'utf8'
);

function section(start, end) {
    const from = source.indexOf(start);
    const to = source.indexOf(end, from);
    assert.ok(from >= 0 && to > from, `Missing dashboard section: ${start}`);
    return source.slice(from, to);
}

function setup() {
    // Keep the fixture tied to the template's real log container, not the old wrapper.
    assert.match(template, /id="log-panel"[^>]*>[\s\S]*?<pre id="log-content"><\/pre>/);
    assert.doesNotMatch(template, /id="log-wrapper"/);
    // Logs open in a hidden pop-up outside the job layout grid.
    assert.match(template, /id="log-popup" hidden>\s*<div class="card widget" id="log-panel"/);
    const nodes = new Map();
    const listeners = new Map();
    const document = {
        activeElement: null,
        getElementById: id => nodes.get(id) || null,
        createElement: () => element(),
        querySelector: selector => {
            assert.equal(selector, '.layout');
            return layout;
        },
        querySelectorAll: () => [],
        addEventListener: (type, listener) => listeners.set(type, listener),
    };
    function element(id) {
        const node = {
            id,
            tagName: 'DIV',
            children: [],
            parentElement: null,
            style: {},
            classes: new Set(),
            classList: {
                add: name => node.classes.add(name),
                remove: name => node.classes.delete(name),
                contains: name => node.classes.has(name),
            },
            innerHTML: '',
            value: '',
            addEventListener() {},
            focus() { document.activeElement = this; },
            blur() { document.activeElement = null; },
            contains(child) {
                return this === child || this.children.some(node => node.contains(child));
            },
            appendChild(child) {
                this.children.push(child);
                child.parentElement = this;
                if (child.id) nodes.set(child.id, child);
            },
            insertBefore(child, reference) {
                assert.equal(reference.parentElement, this);
                const index = this.children.indexOf(reference);
                assert.ok(index >= 0, 'Reference must be a direct child');
                this.children.splice(index, 0, child);
                child.parentElement = this;
                nodes.set(child.id, child);
            },
            remove() {
                const siblings = this.parentElement.children;
                siblings.splice(siblings.indexOf(this), 1);
                this.parentElement = null;
                nodes.delete(this.id);
            },
        };
        if (id) nodes.set(id, node);
        return node;
    }
    const layout = element('dashboard-layout');
    const wrapper = element('log-panel');
    const logPanel = element('log-content');
    const header = element('log-header');
    const logSearchInput = element('log-search-input');
    const searchBox = element('search-box');
    layout.appendChild(wrapper);
    wrapper.appendChild(header);
    header.appendChild(logSearchInput);
    wrapper.appendChild(logPanel);
    layout.appendChild(searchBox);
    const logPopup = element('log-popup');
    logPopup.hidden = true;
    const context = vm.createContext({
        document, logPanel, logPopup, popupCloses: 0,
        closeLogPopup() { context.popupCloses++; logPopup.hidden = true; }, logSearchInput, searchBox,
        logSearchPrev: element(), logSearchNext: element(), logSearchClose: element(),
        currentLogKey: 'synthetic-job', currentLogKind: 'stdout', annotations: {},
        originalLogContent: '', detectedErrors: [], dismissedErrors: new Set(),
        logAnalysisEnabled: true, comparePanes: [], selectedForCompare: new Set(),
        updateAnnotationCount() {}, updateCompareButton() {},
        renderRunning() {}, renderRecent() {}, filterJobs: jobs => jobs,
        allRunningJobs: [], allRecentJobs: [],
        handleLogSearchInput() {}, goToPrevMatch() {}, goToNextMatch() {},
        clearLogSearch() {},
    });
    vm.runInContext([
        section('const errorPatterns = [', 'const isNearBottom'),
        section('function getAnnotationKey()', 'function addAnnotation('),
        section('function renderLogWithAnnotations()', 'function jumpToLogLine('),
        section('function exitCompareMode()', 'function startCompareStream('),
        section('// Log search event listeners', "document.querySelectorAll('#running-table"),
    ].join('\n'), context);
    return { context, document, nodes, listeners, wrapper, header, logPanel, logSearchInput, searchBox };
}

for (const [label, content, issue] of [
    ['warning', 'Starting\nUserWarning: synthetic warning\nCompleted', 'Warning'],
    ['error', 'Starting\nCUDA error: synthetic failure\nStopped', 'CUDA Error'],
    ['ordinary', 'Starting\nProgress <1> & <2>\nCompleted', null],
]) {
    test(`${label} log renders all lines with the expected summary`, () => {
        const { context, nodes, wrapper, header, logPanel } = setup();
        context.originalLogContent = content;
        vm.runInContext('renderLogWithAnnotations()', context);
        assert.equal((logPanel.innerHTML.match(/class="log-line/g) || []).length, 3);
        const summary = nodes.get('error-summary');
        if (issue) {
            assert.ok(summary);
            assert.ok(summary.innerHTML.includes(issue));
            assert.deepEqual(wrapper.children, [header, summary, logPanel]);
            assert.match(logPanel.innerHTML, /error-line/);
        } else {
            assert.equal(summary, undefined);
            assert.match(logPanel.innerHTML, /Progress &lt;1&gt; &amp; &lt;2&gt;/);
        }
    });
}

test('rerender replaces the summary and a clean log removes it', () => {
    const { context, nodes, wrapper, logPanel } = setup();
    context.originalLogContent = 'UserWarning: first synthetic warning';
    vm.runInContext('renderLogWithAnnotations()', context);
    const first = nodes.get('error-summary');
    context.originalLogContent += '\nCUDA error: synthetic failure';
    vm.runInContext('renderLogWithAnnotations()', context);
    assert.notEqual(nodes.get('error-summary'), first);
    assert.equal(first.parentElement, null);
    assert.equal(wrapper.children.filter(node => node.id === 'error-summary').length, 1);
    assert.match(nodes.get('error-summary').innerHTML, /2 issue\(s\) detected/);
    context.originalLogContent = 'Completed';
    vm.runInContext('renderLogWithAnnotations()', context);
    assert.equal(nodes.get('error-summary'), undefined);
    assert.match(logPanel.innerHTML, /Completed/);
});

for (const modifier of ['ctrlKey', 'metaKey']) {
    test(`${modifier}+F focuses log search only inside the log widget`, () => {
        const { document, listeners, header, logSearchInput, searchBox } = setup();
        const event = {
            key: 'f', [modifier]: true, target: header,
            prevented: false,
            preventDefault() { this.prevented = true; },
        };
        document.activeElement = header;
        listeners.get('keydown')(event);
        assert.equal(event.prevented, true);
        assert.equal(document.activeElement, logSearchInput);
        searchBox.focus();
        event.prevented = false;
        event.target = searchBox;
        listeners.get('keydown')(event);
        assert.equal(event.prevented, false);
        assert.equal(document.activeElement, searchBox);
    });
}

test('comparison closes the log pop-up and uses a two-column layout', () => {
    const { context, nodes } = setup();
    let closed = 0;
    context.comparePanes = ['synthetic-job-a', 'synthetic-job-b'].map(logKey => ({
        logKey, kind: 'stdout', stream: { close() { closed++; } },
    }));
    vm.runInContext('renderCompareView()', context);
    assert.equal(context.popupCloses, 1);
    assert.equal(nodes.get('dashboard-layout').classList.contains('comparing'), true);
    assert.ok(nodes.get('compare-view'));
    assert.match(nodes.get('compare-view').innerHTML, /synthetic-job-a/);
    assert.match(nodes.get('compare-view').innerHTML, /synthetic-job-b/);
    vm.runInContext('exitCompareMode()', context);
    assert.equal(nodes.get('dashboard-layout').classList.contains('comparing'), false);
    assert.equal(nodes.get('log-popup').hidden, true);
    assert.equal(nodes.get('compare-view'), undefined);
    assert.equal(closed, 2);
});
