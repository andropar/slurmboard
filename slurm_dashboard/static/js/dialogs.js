// Toasts and modal dialogs: confirm, submit, resubmit, help.

import { $, $$, html, setHTML } from './util.js';
import { api } from './api.js';
import { emit, selectJob, setView } from './state.js';

// --------------------------------------------------------------------- toasts

export function toast(message, { kind = 'info', action } = {}) {
    let host = $('#toasts');
    if (!host) {
        host = document.createElement('div');
        host.id = 'toasts';
        host.setAttribute('role', 'status');
        document.body.appendChild(host);
    }
    const el = document.createElement('div');
    el.className = `toast ${kind === 'error' ? 'toast-error' : ''}`;
    const text = document.createElement('span');
    text.textContent = message;
    el.appendChild(text);
    if (action) {
        const button = document.createElement('button');
        button.type = 'button';
        button.textContent = action.label;
        button.addEventListener('click', () => {
            action.run();
            el.remove();
        });
        el.appendChild(button);
    }
    host.appendChild(el);
    setTimeout(() => el.remove(), kind === 'error' ? 10000 : 5000);
}

// -------------------------------------------------------------------- dialogs

function openDialog(className, template) {
    const dialog = document.createElement('dialog');
    dialog.className = `dialog ${className}`;
    setHTML(dialog, template);
    document.body.appendChild(dialog);
    dialog.addEventListener('close', () => dialog.remove());
    dialog.addEventListener('click', event => {
        // A click on the backdrop lands on the dialog element itself.
        if (event.target === dialog || event.target.closest('[data-close]')) dialog.close('cancel');
    });
    dialog.showModal();
    return dialog;
}

export function confirmDialog({ title, message, confirm = 'Confirm', dismiss = 'Cancel', danger = false }) {
    return new Promise(resolve => {
        const dialog = openDialog('dialog-small', html`
            <form method="dialog" class="dialog-form">
                <div class="dialog-body">
                    <h2>${title}</h2>
                    <p class="dialog-text">${message}</p>
                </div>
                <footer class="dialog-foot">
                    <button class="btn" value="cancel">${dismiss}</button>
                    <button class="btn ${danger ? 'danger' : 'primary'}" value="ok" autofocus>${confirm}</button>
                </footer>
            </form>`);
        dialog.addEventListener('close', () => resolve(dialog.returnValue === 'ok'));
    });
}

function showFormError(dialog, message) {
    const box = $('.form-error', dialog);
    box.textContent = message;
    box.hidden = !message;
}

function formValues(form, names) {
    const values = {};
    for (const name of names) {
        const field = form.elements[name];
        const value = field ? field.value.trim() : '';
        if (value) values[name] = field.type === 'number' ? Number(value) : value;
    }
    return values;
}

function afterSubmit(result, verb) {
    emit('refresh');
    setView('jobs');
    selectJob(result.job_id, { tab: null });
    toast(`${verb} as job ${result.job_id}.`);
}

// --------------------------------------------------------------------- submit

const OPTION_FIELDS = ['job_name', 'partition', 'time_limit', 'memory', 'cpus', 'gpus', 'work_dir', 'dependency'];

export async function openSubmit() {
    const [partitionData, templateData] = await Promise.all([
        api.get('/api/partitions').catch(() => ({ partitions: [] })),
        api.get('/api/templates').catch(() => ({ templates: [] })),
    ]);
    let templates = templateData.templates || [];

    const dialog = openDialog('dialog-wide', html`
        <form class="dialog-form" novalidate>
            <header class="dialog-head">
                <h2>Submit a job</h2>
                <button type="button" class="icon-btn" data-close aria-label="Close">×</button>
            </header>
            <div class="dialog-body">
                <div class="field-row">
                    <label class="field grow"><span>Start from a template</span>
                        <select name="template"></select>
                    </label>
                    <button type="button" class="btn quiet" data-act="delete-template" hidden>Delete template</button>
                </div>

                <div class="seg" role="tablist" aria-label="Script source">
                    <button type="button" class="on" data-mode="content" role="tab">Write script</button>
                    <button type="button" data-mode="path" role="tab">Use a file on the cluster</button>
                </div>
                <label class="field" data-for="content"><span class="sr">Script</span>
                    <textarea name="script_content" rows="9" spellcheck="false" placeholder="#!/bin/bash&#10;#SBATCH --time=01:00:00&#10;&#10;python train.py"></textarea>
                </label>
                <label class="field" data-for="path" hidden><span class="sr">Script path</span>
                    <input name="script_path" type="text" placeholder="/home/you/project/train.sh" spellcheck="false">
                </label>

                <p class="field-note">Options below override the matching <code>#SBATCH</code> lines. Leave them empty to use the script's own.</p>
                <div class="field-grid">
                    <label class="field"><span>Job name</span><input name="job_name" type="text"></label>
                    <label class="field"><span>Partition</span>
                        <select name="partition">
                            <option value="">From script</option>
                            ${(partitionData.partitions || []).map(p => html`<option value="${p.name}">${p.name}${p.default ? ' (default)' : ''}</option>`)}
                        </select>
                    </label>
                    <label class="field"><span>Time limit</span><input name="time_limit" type="text" placeholder="1:00:00"></label>
                    <label class="field"><span>Memory</span><input name="memory" type="text" placeholder="16G"></label>
                    <label class="field"><span>CPUs</span><input name="cpus" type="number" min="1"></label>
                    <label class="field"><span>GPUs</span><input name="gpus" type="number" min="0"></label>
                </div>
                <label class="field"><span>Working directory</span><input name="work_dir" type="text" spellcheck="false" placeholder="Where sbatch runs; defaults to the dashboard's directory"></label>
                <label class="field"><span>Run after</span><input name="dependency" type="text" spellcheck="false" placeholder="afterok:12345"></label>
                <p class="form-error" role="alert" hidden></p>
            </div>
            <footer class="dialog-foot">
                <div class="foot-left">
                    <button type="button" class="btn quiet" data-act="save-template">Save as template</button>
                    <span class="save-template" hidden>
                        <input type="text" name="template_name" placeholder="Template name" aria-label="Template name">
                        <button type="button" class="btn" data-act="confirm-template">Save</button>
                    </span>
                </div>
                <button type="button" class="btn" data-close>Cancel</button>
                <button type="submit" class="btn primary">Submit</button>
            </footer>
        </form>`);

    const form = $('form', dialog);
    const select = form.elements.template;
    let mode = 'content';

    const fillTemplates = chosen => {
        setHTML(select, html`<option value="">None</option>${templates.map(t => html`<option value="${t.id}">${t.name}</option>`)}`);
        select.value = chosen || '';
        $('[data-act="delete-template"]', dialog).hidden = !select.value;
    };
    const setMode = next => {
        mode = next;
        $$('.seg button', dialog).forEach(b => b.classList.toggle('on', b.dataset.mode === mode));
        $$('[data-for]', dialog).forEach(el => { el.hidden = el.dataset.for !== mode; });
    };
    fillTemplates('');

    select.addEventListener('change', () => {
        const template = templates.find(t => t.id === select.value);
        $('[data-act="delete-template"]', dialog).hidden = !template;
        if (!template) return;
        setMode('content');
        form.elements.script_content.value = template.script_content || '';
        for (const name of OPTION_FIELDS) {
            if (form.elements[name]) form.elements[name].value = template[name] ?? '';
        }
    });

    dialog.addEventListener('click', async event => {
        const modeButton = event.target.closest('[data-mode]');
        if (modeButton) return setMode(modeButton.dataset.mode);

        const act = event.target.closest('[data-act]')?.dataset.act;
        if (act === 'save-template') {
            $('.save-template', dialog).hidden = false;
            event.target.closest('[data-act]').hidden = true;
            form.elements.template_name.focus();
        } else if (act === 'confirm-template') {
            await saveTemplate();
        } else if (act === 'delete-template') {
            try {
                await api.del(`/api/templates/${encodeURIComponent(select.value)}`);
                templates = templates.filter(t => t.id !== select.value);
                fillTemplates('');
            } catch (err) {
                showFormError(dialog, err.message);
            }
        }
    });

    async function saveTemplate() {
        const name = form.elements.template_name.value.trim();
        if (!name) return form.elements.template_name.focus();
        try {
            const body = { name, script_content: form.elements.script_content.value, ...formValues(form, OPTION_FIELDS) };
            const saved = (await api.post('/api/templates', body)).template;
            templates = [...templates.filter(t => t.id !== saved.id), saved];
            fillTemplates(saved.id);
            $('.save-template', dialog).hidden = true;
            $('[data-act="save-template"]', dialog).hidden = false;
            showFormError(dialog, '');
            toast(`Saved template “${saved.name}”.`);
        } catch (err) {
            showFormError(dialog, err.message);
        }
    }

    form.elements.template_name.addEventListener('keydown', event => {
        if (event.key === 'Enter') {
            event.preventDefault();
            saveTemplate();
        }
    });

    form.addEventListener('submit', async event => {
        event.preventDefault();
        const body = formValues(form, OPTION_FIELDS);
        if (mode === 'path') body.script_path = form.elements.script_path.value.trim();
        else body.script_content = form.elements.script_content.value;
        if (!(body.script_path || (body.script_content || '').trim())) {
            return showFormError(dialog, mode === 'path' ? 'Enter the path of a script on the cluster.' : 'Write or paste a script first.');
        }
        const button = $('button[type="submit"]', dialog);
        button.disabled = true;
        try {
            const result = await api.post('/api/submit', body);
            dialog.close('ok');
            afterSubmit(result, 'Submitted');
        } catch (err) {
            showFormError(dialog, err.message);
            button.disabled = false;
        }
    });
}

// ------------------------------------------------------------------- resubmit

export function openResubmit(job, submission) {
    const info = submission || {};
    const source = info.script_path
        ? html`Runs <code>${info.script_path}</code> as it is on disk now.`
        : html`The original script file was not found, so the copy Slurm kept at submission is used.`;
    const dialog = openDialog('dialog-wide', html`
        <form class="dialog-form" novalidate>
            <header class="dialog-head">
                <h2>Resubmit ${job.name || `job ${job.id}`}</h2>
                <button type="button" class="icon-btn" data-close aria-label="Close">×</button>
            </header>
            <div class="dialog-body">
                <p class="field-note">${source} Leave a field empty to keep what the script asks for.</p>
                <div class="field-grid">
                    <label class="field"><span>Partition</span><input name="partition" type="text" placeholder="${info.partition || ''}"></label>
                    <label class="field"><span>Time limit</span><input name="time_limit" type="text" placeholder="${info.timelimit || ''}"></label>
                    <label class="field"><span>Memory</span><input name="memory" type="text" placeholder="${info.req_mem || ''}"></label>
                    <label class="field"><span>CPUs</span><input name="cpus" type="number" min="1" placeholder="${info.req_cpus || ''}"></label>
                </div>
                <label class="field"><span>Working directory</span><input name="work_dir" type="text" spellcheck="false" value="${info.work_dir || ''}"></label>
                <p class="form-error" role="alert" hidden></p>
            </div>
            <footer class="dialog-foot">
                <button type="button" class="btn" data-close>Cancel</button>
                <button type="submit" class="btn primary" autofocus>Resubmit</button>
            </footer>
        </form>`);

    const form = $('form', dialog);
    form.addEventListener('submit', async event => {
        event.preventDefault();
        const button = $('button[type="submit"]', dialog);
        button.disabled = true;
        try {
            const body = { job_id: job.id, ...formValues(form, ['partition', 'time_limit', 'memory', 'cpus', 'work_dir']) };
            const result = await api.post('/api/resubmit', body);
            dialog.close('ok');
            afterSubmit(result, 'Resubmitted');
        } catch (err) {
            showFormError(dialog, err.message);
            button.disabled = false;
        }
    });
}

// ----------------------------------------------------------------------- help

export function openHelp() {
    const key = label => html`<kbd>${label}</kbd>`;
    openDialog('dialog-wide', html`
        <div class="dialog-form">
            <header class="dialog-head">
                <h2>Shortcuts and search</h2>
                <button type="button" class="icon-btn" data-close aria-label="Close" autofocus>×</button>
            </header>
            <div class="dialog-body help">
                <section>
                    <h3>Keyboard</h3>
                    <dl>
                        <dt>${key('↑')} ${key('↓')} or ${key('k')} ${key('j')}</dt><dd>Previous / next job</dd>
                        <dt>${key('/')}</dt><dd>Search jobs</dd>
                        <dt>${key('1')} – ${key('4')}</dt><dd>Output, Errors, Script, Details</dd>
                        <dt>${key('f')}</dt><dd>Follow the log (stay at the newest line)</dd>
                        <dt>${key('Ctrl')} ${key('F')}</dt><dd>Find in the open log</dd>
                        <dt>${key('n')}</dt><dd>Submit a job</dd>
                        <dt>${key('r')}</dt><dd>Refresh now</dd>
                        <dt>${key('Esc')}</dt><dd>Clear the search or close a dialog</dd>
                    </dl>
                </section>
                <section>
                    <h3>Search</h3>
                    <p>Plain words match the job name or ID. Narrow further with:</p>
                    <dl>
                        <dt><code>name:train*</code></dt><dd>Name pattern, <code>*</code> matches anything</dd>
                        <dt><code>partition:gpu</code></dt><dd>Partition</dd>
                        <dt><code>state:timeout</code></dt><dd>Exact Slurm state</dd>
                        <dt><code>user:alice</code></dt><dd>Owner (with <code>--all-users</code>)</dd>
                        <dt><code>exit:1</code> or <code>exit:!0</code></dt><dd>Exit code</dd>
                        <dt><code>node:gpu03</code></dt><dd>Node the job ran on</dd>
                    </dl>
                    <p>The address bar keeps the current search, so a filtered view can be bookmarked.</p>
                </section>
            </div>
        </div>`);
}
