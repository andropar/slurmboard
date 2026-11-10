// Application state shared by the views, with a minimal event bus.
//
// Events:  jobs    job lists were refreshed from the server
//          select  the selected job or tab changed
//          filter  search text or state filter changed
//          view    the visible view changed

export const VIEWS = ['jobs', 'timeline', 'pipelines', 'insights'];
export const TABS = ['output', 'errors', 'script', 'details'];

export const state = {
    settings: window.SETTINGS || {},
    meta: window.SETTINGS || {},
    active: [],
    finished: [],
    errors: [],
    loaded: false,
    loadedAt: Date.now(),   // when the job lists were last fetched; running clocks tick from here
    historyDays: null,      // days of finished jobs to list; null uses the server's default
    view: 'jobs',
    query: '',
    stateFilter: '',        // '', running, pending, failed, completed, cancelled
    selectedId: null,
    tab: null,              // tab the user picked; null lets each job choose a sensible default
    checked: new Set(),
    openGroups: new Set(),
};

const listeners = new Map();

export function on(event, handler) {
    if (!listeners.has(event)) listeners.set(event, []);
    listeners.get(event).push(handler);
}

export function emit(event, detail) {
    (listeners.get(event) || []).forEach(handler => handler(detail));
}

export function findJob(id) {
    return state.active.find(job => job.id === id) || state.finished.find(job => job.id === id) || null;
}

/** Elapsed seconds of an active job right now, extrapolated from the last fetch. */
export function liveElapsed(job) {
    const base = job.runtime_seconds || 0;
    if (job.state_category !== 'running') return base;
    return base + (Date.now() - state.loadedAt) / 1000;
}

/**
 * Select a job. `open` asks narrow layouts to bring the job page forward;
 * automatic selections (first load) pass false so the list stays visible.
 */
export function selectJob(id, { tab, open = true } = {}) {
    if (tab !== undefined) state.tab = tab;
    // Script and Details stay open while moving through jobs (handy for comparing);
    // between the two log tabs each job picks its own default again.
    else if (id !== state.selectedId && (state.tab === 'output' || state.tab === 'errors')) state.tab = null;
    state.selectedId = id;
    writeHash();
    emit('select', { open });
}

export function setTab(tab) {
    state.tab = tab;
    writeHash();
    emit('select', { open: false });
}

export function setView(view) {
    if (!VIEWS.includes(view)) view = 'jobs';
    state.view = view;
    writeHash();
    emit('view');
}

export function setFilter({ query, stateFilter }) {
    if (query !== undefined) state.query = query;
    if (stateFilter !== undefined) state.stateFilter = stateFilter;
    writeHash();
    emit('filter');
}

// The address bar mirrors what is on screen, so a reload or a shared link
// returns to the same job, tab and filter.
export function writeHash() {
    const params = new URLSearchParams();
    if (state.view !== 'jobs') params.set('view', state.view);
    if (state.selectedId) params.set('job', state.selectedId);
    if (state.tab) params.set('tab', state.tab);
    if (state.query) params.set('q', state.query);
    if (state.stateFilter) params.set('state', state.stateFilter);
    const hash = params.toString();
    const url = hash ? `#${hash}` : location.pathname + location.search;
    if (location.hash.slice(1) !== hash) history.replaceState(null, '', url);
}

export function readHash() {
    const params = new URLSearchParams(location.hash.slice(1));
    state.view = VIEWS.includes(params.get('view')) ? params.get('view') : 'jobs';
    state.selectedId = params.get('job') || null;
    state.tab = TABS.includes(params.get('tab')) ? params.get('tab') : null;
    state.query = params.get('q') || '';
    state.stateFilter = params.get('state') || '';
}
