'use strict';
let jobs = [];
let statusFilter = 'ALL', sortKey = 'id', direction = -1, selectedId = null, selectedFile = 'script';
let page = 1, totalPages = 1;
let viewerJob = null, logStream = null, viewerRequest = 0, queueRequest = 0;
let queueController = null, scriptController = null, followLog = true;
let queueLoading = true, queueError = false, logText = '';
const $ = id => document.getElementById(id);
const escape = text => String(text).replace(/[&<>"']/g, char => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[char]));
const failedStates = new Set(['FAILED', 'TIMEOUT', 'OUT_OF_MEMORY', 'NODE_FAIL', 'BOOT_FAIL', 'DEADLINE', 'PREEMPTED', 'REVOKED']);
const runningStates = new Set(['RUNNING', 'COMPLETING', 'CONFIGURING', 'SUSPENDED', 'RESIZING', 'STAGE_OUT']);
const pendingStates = new Set(['PENDING', 'REQUEUED', 'REQUEUE_FED', 'REQUEUE_HOLD']);
function stateCategory(state) {
  if (failedStates.has(state)) return 'FAILED';
  if (runningStates.has(state)) return 'RUNNING';
  if (pendingStates.has(state)) return 'PENDING';
  return ['COMPLETED', 'CANCELLED'].includes(state) ? state : 'UNKNOWN';
}
function selectJobs(records, query, filter, from, to, key, order) {
  return records.filter(job => {
    const stateMatches = filter === 'ALL' || stateCategory(job.status) === filter;
    const dateMatches = (!from && !to) || (job.queued &&
      (!from || job.queued.slice(0, 10) >= from) && (!to || job.queued.slice(0, 10) <= to));
    return stateMatches && dateMatches && `${job.id} ${job.name}`.toLowerCase().includes(query);
  }).sort((a, b) => {
    if (!a[key] && !b[key]) return String(a.id).localeCompare(String(b.id), undefined, {numeric: true});
    if (!a[key]) return 1;
    if (!b[key]) return -1;
    const compared = String(a[key]).localeCompare(String(b[key]), undefined, {numeric: true});
    return compared ? compared * order : String(a.id).localeCompare(String(b.id), undefined, {numeric: true});
  });
}
function timestamp(value) {
  if (!value) return '<span class="not-yet" aria-label="Unavailable or not applicable">—</span>';
  return `<time datetime="${escape(value)}">${escape(value.slice(0,10))}<span>${escape(value.slice(11,19))}</span></time>`;
}
function renderFilters() {
  $('filters').innerHTML = ['ALL','RUNNING','PENDING','COMPLETED','FAILED','CANCELLED','UNKNOWN'].map(state => {
    const count = jobs.filter(j => state === 'ALL' || stateCategory(j.status) === state).length;
    const title = state === 'ALL' ? 'All jobs' : state === 'FAILED' ? 'Failed / timeout' : state[0] + state.slice(1).toLowerCase();
    return `<button data-state="${state}" aria-pressed="${statusFilter === state}">${title}<span class="count">${count}</span></button>`;
  }).join('');
}
function render() {
  const query = $('search').value.toLowerCase().trim();
  const rows = selectJobs(jobs, query, statusFilter, $('date-from').value, $('date-to').value, sortKey, direction);
  const pageSize = Number($('page-size').value);
  totalPages = Math.max(1, Math.ceil(rows.length / pageSize));
  page = Math.max(1, Math.min(page, totalPages));
  const offset = (page - 1) * pageSize;
  const visibleRows = rows.slice(offset, offset + pageSize);
  $('jobs').innerHTML = visibleRows.map(job => `<tr data-id="${escape(job.id)}" class="${selectedId === job.id ? 'selected' : ''}">
    <td class="id-cell"><button class="job-id" data-open="script" title="Inspect job ${escape(job.id)}">${escape(job.id)}</button></td>
    <td class="name-cell"><div class="job-name">${escape(job.name)}</div><div class="job-subtitle">${escape(job.partition || 'Partition unavailable')}${job.reason ? ' · ' + escape(job.reason) : ''}${failedStates.has(job.status) && job.exit_code ? ' · Exit ' + escape(job.exit_code) : ''}</div></td>
    <td class="state-cell"><span class="state ${stateCategory(job.status).toLowerCase()}">${escape(job.status)}</span></td>
    <td class="time-cell" data-label="Queued">${timestamp(job.queued)}</td>
    <td class="time-cell" data-label="Started">${timestamp(job.started)}</td>
    <td class="time-cell" data-label="Ended">${timestamp(job.ended)}</td>
    <td class="action-cell"><div class="row-actions">${['script','stdout','stderr'].map(file => `<button data-open="${file}" aria-label="View ${file} for job ${escape(job.id)}" class="${selectedId === job.id && selectedFile === file ? 'active' : ''}">${file === 'script' ? 'Script' : file === 'stdout' ? 'Stdout' : 'Stderr'}</button>`).join('')}</div></td>
  </tr>`).join('');
  $('empty').hidden = rows.length > 0;
  $('empty-text').textContent = queueLoading && !jobs.length ? 'Loading jobs…'
    : queueError && !jobs.length ? 'Job data unavailable. See the error above and retry.'
    : 'No jobs match these filters.';
  $('results').textContent = rows.length
    ? `${offset + 1}–${offset + visibleRows.length} of ${rows.length} matching jobs · ${jobs.length} in scope`
    : `0 matching jobs · ${jobs.length} in scope`;
  $('page-status').textContent = `Page ${page} of ${totalPages}`;
  $('first-page').disabled = $('previous-page').disabled = page === 1;
  $('next-page').disabled = $('last-page').disabled = page === totalPages;
  $('mobile-sort').value = sortKey;
  $('sort-direction').textContent = direction === 1 ? 'Ascending ↑' : 'Descending ↓';
  document.querySelectorAll('th[data-active]').forEach(th => th.removeAttribute('data-active'));
  document.querySelectorAll('th [data-sort]').forEach(button => {
    const active = button.dataset.sort === sortKey;
    button.parentElement.setAttribute('aria-sort', active ? direction === 1 ? 'ascending' : 'descending' : 'none');
    const titles = {id:'Job ID',name:'Job name',status:'Status',queued:'Queued',started:'Started',ended:'Ended'};
    button.textContent = titles[button.dataset.sort] + (active ? direction === 1 ? ' ↑' : ' ↓' : ' ↕');
    button.title = `Sort all matching jobs by ${titles[button.dataset.sort].toLowerCase()}`;
  });
  renderFilters();
}
function viewerMessage(message, error = false) {
  $('viewer-message').textContent = message;
  $('viewer-message').classList.toggle('error', error);
}
function stopViewerRequest() {
  viewerRequest++;
  if (logStream) logStream.close();
  logStream = null;
  if (scriptController) scriptController.abort();
  scriptController = null;
}
async function openViewer(id, file) {
  const job = jobs.find(j => j.id === id) || (viewerJob?.id === id ? viewerJob : null);
  if (!job) return;
  stopViewerRequest();
  const request = viewerRequest;
  viewerJob = job;
  selectedId = id; selectedFile = file;
  $('viewer').hidden = false; document.body.classList.add('viewer-open');
  $('viewer-id').textContent = `JOB ${job.id} / ${job.status}`;
  $('viewer-title').textContent = job.name;
  document.querySelectorAll('[data-file]').forEach(tab => {
    tab.setAttribute('aria-selected', String(tab.dataset.file === file));
    tab.tabIndex = tab.dataset.file === file ? 0 : -1;
  });
  $('file-content').setAttribute('aria-labelledby', `tab-${file}`);
  $('file-note').textContent = file === 'script' ? 'Submitted copy retained by Slurm' : `${file} · configured log pattern`;
  $('follow').hidden = file === 'script';
  $('retry-file').hidden = true;
  logText = '';
  $('file-content').textContent = '';
  $('file-content').scrollTop = 0;
  viewerMessage(file === 'script' ? 'Loading submitted script…' : 'Connecting to log…');
  render();
  $('viewer').scrollIntoView({block:'nearest'});
  $(`tab-${file}`).focus({preventScroll:true});
  if (file === 'script') {
    scriptController = new AbortController();
    try {
      const response = await fetch(`/api/submitted_script/${encodeURIComponent(id)}`, {signal: scriptController.signal});
      const data = await response.json();
      if (request !== viewerRequest) return;
      if (!response.ok) {
        const attempts = (data.attempts || []).map(attempt => `${attempt.source}: ${attempt.message}`);
        throw new Error([data.error || `Script request failed (HTTP ${response.status})`, ...attempts].join('\n'));
      }
      if (typeof data.content !== 'string') throw new Error('Invalid script response: no content returned.');
      $('file-content').textContent = data.content;
      const origin = data.source === 'accounting' ? 'Slurm accounting' : 'Slurm controller';
      $('file-note').textContent = `Source: ${origin}`;
      viewerMessage(`Submitted copy from ${origin}, not the current source file.${data.truncated
        ? ` Preview truncated at ${data.max_lines} lines or ${data.max_bytes} bytes.` : ''}`);
    } catch (error) {
      if (request !== viewerRequest || error.name === 'AbortError') return;
      console.error('Script retrieval failed:', error);
      viewerMessage(error.message, true);
      $('retry-file').hidden = false;
    }
    return;
  }
  const params = new URLSearchParams({log_key: job.log_key, kind: file});
  const stream = new EventSource(`/stream_log?${params}`);
  logStream = stream;
  stream.onmessage = event => {
    if (request !== viewerRequest) return;
    try {
      const data = JSON.parse(event.data);
      if (Object.hasOwn(data, 'snapshot')) logText = data.snapshot;
      else if (typeof data.append === 'string') logText += data.append;
      else throw new Error('Log stream returned an invalid message.');
      logText = logText.slice(-200000);
      $('file-content').textContent = logText;
      if (followLog) $('file-content').scrollTop = $('file-content').scrollHeight;
      viewerMessage(logText ? 'Live log · showing the latest output (up to 200 KB on connection; 200,000 characters retained).'
        : 'Log file is empty. Connected and waiting for output.');
    } catch (error) {
      console.error('Invalid log event:', error);
      stream.close();
      viewerMessage('Invalid log stream response. Retry to reconnect.', true);
      $('retry-file').hidden = false;
    }
  };
  stream.onerror = () => {
    if (request !== viewerRequest) return;
    stream.close();
    viewerMessage('Log unavailable or connection lost. The file may not exist yet, may be unreadable, or the server is offline. Retry to reconnect.', true);
    $('retry-file').hidden = false;
  };
}
function closeViewer() {
  const id = selectedId, file = selectedFile;
  stopViewerRequest();
  viewerJob = null;
  $('viewer').hidden = true; document.body.classList.remove('viewer-open'); selectedId = null; render();
  const target = document.querySelector(`tr[data-id="${id}"] .row-actions [data-open="${file}"]`);
  if (target) target.focus({preventScroll:true});
  else $('search').focus({preventScroll:true});
}
$('jobs').addEventListener('click', event => {
  const button = event.target.closest('[data-open]');
  if (button) openViewer(button.closest('tr').dataset.id, button.dataset.open);
});
$('filters').addEventListener('click', event => {
  const button = event.target.closest('[data-state]'); if (!button) return;
  statusFilter = button.dataset.state; resetPage();
});
function resetPage() {
  page = 1;
  render();
  document.querySelector('.table-scroll').scrollTop = 0;
}
$('search').addEventListener('input', resetPage);
$('date-from').addEventListener('change', resetPage);
$('date-to').addEventListener('change', resetPage);
$('history').addEventListener('change', () => { page = 1; refreshJobs(); });
$('page-size').addEventListener('change', resetPage);
$('clear').addEventListener('click', () => {
  statusFilter = 'ALL'; $('search').value = ''; $('date-from').value = ''; $('date-to').value = '';
  resetPage();
});
document.querySelectorAll('[data-sort]').forEach(button => button.addEventListener('click', () => {
  direction = sortKey === button.dataset.sort ? -direction : button.dataset.sort === 'name' || button.dataset.sort === 'status' ? 1 : -1;
  sortKey = button.dataset.sort; resetPage();
}));
$('mobile-sort').addEventListener('change', () => {
  sortKey = $('mobile-sort').value;
  direction = ['name', 'status'].includes(sortKey) ? 1 : -1;
  resetPage();
});
$('sort-direction').addEventListener('click', () => { direction = -direction; resetPage(); });
for (const action of ['first', 'previous', 'next', 'last']) {
  $(`${action}-page`).addEventListener('click', () => {
    page = action === 'first' ? 1 : action === 'last' ? totalPages : page + (action === 'next' ? 1 : -1);
    render();
    document.querySelector('.table-scroll').scrollTop = 0;
  });
}
document.querySelectorAll('[data-file]').forEach(button => button.addEventListener('click', () => openViewer(selectedId, button.dataset.file)));
document.querySelector('[role="tablist"]').addEventListener('keydown', event => {
  const keys = ['ArrowLeft','ArrowRight','Home','End']; if (!keys.includes(event.key)) return;
  event.preventDefault();
  const files = ['script','stdout','stderr'], index = files.indexOf(selectedFile);
  const next = event.key === 'Home' ? 0 : event.key === 'End' ? 2 : (index + (event.key === 'ArrowRight' ? 1 : 2)) % 3;
  openViewer(selectedId, files[next]);
});
$('close').addEventListener('click', closeViewer);
$('theme').addEventListener('click', () => {
  document.documentElement.dataset.theme = document.documentElement.dataset.theme === 'dark' ? 'light' : 'dark';
  localStorage.setItem('theme', document.documentElement.dataset.theme);
});
$('follow').addEventListener('click', () => {
  followLog = !followLog;
  $('follow').setAttribute('aria-pressed', String(followLog));
  if (followLog) $('file-content').scrollTop = $('file-content').scrollHeight;
});
$('retry-file').addEventListener('click', () => openViewer(selectedId, selectedFile));
document.addEventListener('keydown', event => {
  if (event.key === 'Escape' && !$('viewer').hidden) closeViewer();
  if (event.key === '/' && !['INPUT','TEXTAREA','SELECT'].includes(document.activeElement.tagName)) { event.preventDefault(); $('search').focus(); }
});
async function refreshJobs(automatic = false) {
  if (automatic && queueLoading) return;
  const request = ++queueRequest;
  if (queueController) queueController.abort();
  queueController = new AbortController();
  queueLoading = true;
  $('refresh').disabled = true;
  try {
    const response = await fetch(`/api/queue?days=${$('history').value}`, {signal: queueController.signal});
    if (!response.ok) throw new Error(`Job request failed (HTTP ${response.status})`);
    const data = await response.json();
    if (request !== queueRequest) return;
    if (!Array.isArray(data.jobs) || !Array.isArray(data.errors)) throw new Error('Invalid job data returned by server.');
    jobs = data.jobs;
    queueError = data.errors.length > 0;
    $('queue-error').textContent = data.errors.length ? `Incomplete job data: ${data.errors.join(' · ')}` : '';
    $('queue-error').hidden = !queueError;
    $('scope').textContent = `${data.user} · ${data.scope}`;
    $('updated').textContent = `Updated ${new Date().toISOString().slice(11, 19)} UTC`;
    const selected = jobs.find(job => job.id === selectedId);
    if (selected) {
      viewerJob = selected;
      $('viewer-id').textContent = `JOB ${selected.id} / ${selected.status}`;
      $('viewer-title').textContent = selected.name;
    }
  } catch (error) {
    if (request !== queueRequest || error.name === 'AbortError') return;
    console.error('Queue refresh failed:', error);
    queueError = true;
    $('queue-error').textContent = `${error.message}. ${jobs.length ? 'Showing previous data; it may be stale.' : 'No job data loaded.'} Use Refresh to retry.`;
    $('queue-error').hidden = false;
  } finally {
    if (request === queueRequest) {
      queueLoading = false;
      $('refresh').disabled = false;
      render();
    }
  }
}
$('refresh').addEventListener('click', () => refreshJobs());
render();
refreshJobs();
setInterval(() => { if (!document.hidden) refreshJobs(true); }, 20000);
document.addEventListener('visibilitychange', () => { if (!document.hidden) refreshJobs(true); });
window.addEventListener('pagehide', () => {
  stopViewerRequest();
  if (queueController) queueController.abort();
});
