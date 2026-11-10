// Pipelines: chains of jobs linked by --dependency, drawn as layered graphs.

import { $, html, raw, setHTML, stateLabel } from './util.js';
import { api } from './api.js';
import { on, selectJob, setView, state } from './state.js';

const NODE_W = 196;
const NODE_H = 52;
const GAP_X = 64;
const GAP_Y = 14;

let root;
let data = null;
let error = null;
let signature = '';

async function load() {
    try {
        data = await api.get('/api/job_dependencies');
        error = data.error || null;
    } catch (err) {
        error = err.message;
    }
    const next = JSON.stringify([data, error]);
    if (next !== signature) {
        signature = next;
        render();
    }
}

/** Place each job in the column after its latest prerequisite. */
function layers(ids, edges) {
    const depth = new Map(ids.map(id => [id, 0]));
    for (let pass = 0; pass < ids.length; pass++) {
        let changed = false;
        for (const edge of edges) {
            const wanted = depth.get(edge.from) + 1;
            if (wanted > depth.get(edge.to) && wanted <= ids.length) {
                depth.set(edge.to, wanted);
                changed = true;
            }
        }
        if (!changed) break;
    }
    const columns = [];
    for (const id of ids) {
        const column = depth.get(id);
        (columns[column] = columns[column] || []).push(id);
    }
    return columns.filter(Boolean);
}

function graph(pipeline, nodes, index) {
    const ids = pipeline.job_ids.filter(id => nodes.has(id));
    const edges = data.edges.filter(edge => ids.includes(edge.from) && ids.includes(edge.to));
    const columns = layers(ids, edges);
    const tallest = Math.max(...columns.map(column => column.length));
    const width = columns.length * NODE_W + (columns.length - 1) * GAP_X;
    const height = tallest * (NODE_H + GAP_Y) - GAP_Y;

    const position = new Map();
    columns.forEach((column, x) => {
        const offset = (height - (column.length * (NODE_H + GAP_Y) - GAP_Y)) / 2;
        column.forEach((id, y) => position.set(id, { x: x * (NODE_W + GAP_X), y: offset + y * (NODE_H + GAP_Y) }));
    });

    const paths = edges.map(edge => {
        const from = position.get(edge.from);
        const to = position.get(edge.to);
        const x1 = from.x + NODE_W;
        const y1 = from.y + NODE_H / 2;
        const x2 = to.x - 7;
        const y2 = to.y + NODE_H / 2;
        const bend = Math.max(24, (x2 - x1) / 2);
        const label = edge.type !== 'afterok'
            ? html`<text x="${(x1 + x2) / 2}" y="${(y1 + y2) / 2 - 5}" text-anchor="middle">${edge.type}</text>`
            : '';
        return html`<path d="M${x1} ${y1} C${x1 + bend} ${y1}, ${x2 - bend} ${y2}, ${x2} ${y2}" marker-end="url(#arrow-${index})"/>${label}`;
    });

    return html`
        <div class="dag" style="width:${width}px;height:${height}px">
            <svg width="${width}" height="${height}" aria-hidden="true">
                <defs><marker id="arrow-${index}" viewBox="0 0 8 8" refX="1" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0 0L8 4L0 8z" class="arrowhead"/></marker></defs>
                ${paths}
            </svg>
            ${ids.map(id => {
                const node = nodes.get(id);
                const at = position.get(id);
                return html`<button class="node s-${node.state_category}" data-id="${id}" style="left:${at.x}px;top:${at.y}px;width:${NODE_W}px;height:${NODE_H}px" title="${node.name}">
                    <span class="node-name">${node.name}</span>
                    <span class="node-sub"><span class="state s-${node.state_category}"><i></i>${stateLabel(node.state)}</span><span class="dot">·</span><span class="mono">${id}</span>${node.tasks > 1 ? html`<span class="dot">·</span>${node.tasks} tasks` : ''}</span>
                </button>`;
            })}
        </div>`;
}

function render() {
    const pipelines = data ? data.pipelines : [];
    const nodes = new Map((data ? data.nodes : []).map(node => [node.id, node]));
    setHTML(root, html`
        <div class="page-head">
            <div>
                <h1>Pipelines</h1>
                <p class="page-sub">Jobs linked with <code>--dependency</code>: what runs after what, and what is holding things up.</p>
            </div>
        </div>
        ${error ? html`<p class="notice">${error}</p>` : ''}
        ${pipelines.length ? pipelines.map((pipeline, index) => html`
            <section class="pipe">
                <div class="pipe-head">
                    <h2>${pipeline.name}</h2>
                    <span class="count">${pipeline.completed} of ${pipeline.total} done</span>
                    <span class="m-bar"><i style="width:${pipeline.progress}%"></i></span>
                </div>
                <div class="pipe-scroll">${graph(pipeline, nodes, index)}</div>
            </section>`)
        : html`<div class="empty">
            <p>${data ? 'No job chains at the moment.' : 'Loading…'}</p>
            ${data ? html`<p class="faint">A job submitted with a dependency, for example ${raw('<code>sbatch --dependency=afterok:12345 next.sh</code>')}, appears here together with the jobs it waits for.</p>` : ''}
        </div>`}`);
}

export function initPipelines() {
    root = $('#view-pipelines');
    root.addEventListener('click', event => {
        const node = event.target.closest('.node');
        if (!node) return;
        const id = node.dataset.id;
        // An array is drawn as one node; open its first task.
        const all = [...state.active, ...state.finished];
        const job = all.find(candidate => candidate.id === id) || all.find(candidate => candidate.array_job_id === id);
        setView('jobs');
        selectJob(job ? job.id : id, { tab: null });
    });
    on('view', () => {
        if (state.view === 'pipelines') {
            if (!data) render();
            load();
        }
    });
    on('jobs', () => {
        if (state.view === 'pipelines') load();
    });
}
