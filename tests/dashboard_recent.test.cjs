const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

const source = fs.readFileSync(
    path.join(__dirname, '../slurm_dashboard/static/js/dashboard.js'), 'utf8'
);
const css = fs.readFileSync(
    path.join(__dirname, '../slurm_dashboard/static/css/style.css'), 'utf8'
);
const from = source.indexOf('function renderRecent(');
const to = source.indexOf('async function cancelJob(', from);
assert.ok(from >= 0 && to > from);

function setup(fetch) {
    const classes = new Set();
    const row = { classList: {
        contains: name => classes.has(name),
        add: name => classes.add(name),
        remove: name => classes.delete(name),
    } };
    const details = {
        innerHTML: '<div class="job-details-loading">Loading details...</div>',
        closest: () => row,
        querySelector: () => details.innerHTML.includes('job-details-loading') ? {} : null,
    };
    const job = {
        id: '123', name: 'training_' + 'very_long_name_'.repeat(30),
        state: 'COMPLETED', log_key: '123.out', updated: 0, partition: 'gpu',
    };
    const context = vm.createContext({
        document: { getElementById: () => details },
        recentBody: { innerHTML: '' }, selectedRecentJobs: new Set(),
        expandedJobs: new Set(), currentLogKey: job.log_key,
        allRecentJobs: [job], allRunningJobs: [], fetch,
        formatStateShort: state => state, relativeTime: () => 'just now',
        updateBatchActionsVisibility() {}, console: { error() {} },
    });
    vm.runInContext(source.slice(from, to), context);
    return { context, classes, details, job };
}

test('expanded-name styles remove truncation and reserve width on narrow layouts', () => {
    function rule(selector) {
        const start = css.indexOf(`${selector} {`);
        assert.ok(start >= 0, `missing ${selector}`);
        return css.slice(start, css.indexOf('}', start));
    }
    assert.match(rule('.job-name'), /white-space: nowrap/);
    assert.match(rule('.job-name'), /text-overflow: ellipsis/);
    assert.match(rule('.job-row.expanded .job-name'), /white-space: normal/);
    assert.match(rule('.job-row.expanded .job-name'), /overflow-wrap: anywhere/);
    assert.match(rule('.job-row.expanded .job-name'), /max-width: none/);
    assert.match(rule('.job-row.expanded .job-name'), /flex-basis: 100%/);
    assert.match(rule('.job-row.expanded .job-row-main'),
        /grid-template-columns: 16px minmax\(0, 1fr\)/);
    assert.match(rule('.job-row.expanded .job-actions'), /grid-column: 2/);
    assert.match(rule('.job-row.expanded .job-actions'), /flex-wrap: wrap/);
});

test('recent rows retain the complete name and existing actions across renders', () => {
    const { context, job } = setup();
    context.selectedRecentJobs.add(job.id);
    for (let i = 0; i < 2; i++) {
        context.renderRecent([job]);
        const html = context.recentBody.innerHTML;
        assert.ok(html.includes(`<span class="job-name">${job.name}</span>`));
        assert.match(html, /active-log.*batch-selected/);
        assert.match(html, /toggleJobSelection\('recent', '123', this.checked\)/);
        assert.match(html, /openLog\('123.out','stdout'\)/);
        assert.match(html, /openLog\('123.out','stderr'\)/);
        assert.match(html, /viewSubmittedScript\('123'\)/);
        assert.match(html, /toggleJobExpand\('123'\)/);
    }
});

test('expanding exposes the name before details arrive and collapsing restores compactness', async () => {
    let resolve;
    const pending = new Promise(done => { resolve = done; });
    let calls = 0;
    const { context, classes } = setup(async url => {
        calls++;
        assert.equal(url, '/api/job_details/123');
        return pending;
    });
    const loading = context.toggleJobExpand('123');
    assert.equal(classes.has('expanded'), true);
    assert.equal(context.expandedJobs.has('123'), true);
    resolve({ ok: true, json: async () => ({ exit_code: '0' }) });
    await loading;
    await context.toggleJobExpand('123');
    assert.equal(classes.has('expanded'), false);
    assert.equal(context.expandedJobs.has('123'), false);
    await context.toggleJobExpand('123');
    assert.equal(classes.has('expanded'), true);
    assert.equal(calls, 1);
});

test('details failure does not hide the expanded name', async () => {
    const { context, classes, details } = setup(async () => { throw new Error('offline'); });
    await context.toggleJobExpand('123');
    assert.equal(classes.has('expanded'), true);
    assert.match(details.innerHTML, /Failed to load details/);
});
