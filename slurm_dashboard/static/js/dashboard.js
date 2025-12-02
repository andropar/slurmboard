const runningBody = document.getElementById('running-body');
const recentBody = document.getElementById('recent-body');
const logPanel = document.getElementById('log-content');
const logTitle = document.getElementById('log-title');
const searchBox = document.getElementById('search-box');
const scrollToggle = document.getElementById('scroll-toggle');
const themeToggle = document.getElementById('theme-toggle');
const logSearchInput = document.getElementById('log-search-input');
const logSearchCount = document.getElementById('log-search-count');
const logSearchPrev = document.getElementById('log-search-prev');
const logSearchNext = document.getElementById('log-search-next');
const logSearchClose = document.getElementById('log-search-close');

// Theme handling
function getPreferredTheme() {
    const stored = localStorage.getItem('theme');
    if (stored) return stored;
    return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
}

function setTheme(theme) {
    document.documentElement.setAttribute('data-theme', theme);
    localStorage.setItem('theme', theme);
}

function toggleTheme() {
    const current = document.documentElement.getAttribute('data-theme') || 'light';
    setTheme(current === 'dark' ? 'light' : 'dark');
}

// Initialize theme immediately to prevent flash
setTheme(getPreferredTheme());

// Listen for system theme changes
window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', (e) => {
    if (!localStorage.getItem('theme')) {
        setTheme(e.matches ? 'dark' : 'light');
    }
});

let logStream = null;
let autoScroll = true;
let currentLogKey = null;
let currentLogKind = 'stdout';
let allRunningJobs = [];
let allRecentJobs = [];
let searchQuery = '';
let sortState = { table: null, column: null, direction: 'asc' };
let expandedJobs = new Set();
let jobDetails = {};
let resourceHistory = {}; // Store resource samples over time for charts
let jobSubmitInfo = {}; // Cache for job submission info
let queueInfo = {}; // Cache for queue position and wait estimates
let insightsData = null; // Cache for job insights
let insightsCollapsed = localStorage.getItem('insightsCollapsed') === 'true';

// Log search state
let logSearchResults = [];
let logSearchCurrentIndex = -1;
let logSearchDebounceTimer = null;
let originalLogContent = '';

// Watch/notification state
let watchedJobs = new Set(JSON.parse(localStorage.getItem('watchedJobs') || '[]'));
let previousJobStates = {}; // Track previous states to detect changes

// Comparison state
let selectedForCompare = new Set();
let compareMode = false;
let comparePanes = []; // Array of {logKey, kind, stream} objects

// Annotation state
let annotations = JSON.parse(localStorage.getItem('logAnnotations') || '{}');
// Structure: { "logKey::kind": { lineNumber: { text: "note", createdAt: timestamp } } }

// Advanced filter state
let activeQuickFilters = new Set();
let advancedFilters = {
    state: '',
    partition: '',
    name: '',
    nameRegex: false,
    dateFrom: '',
    dateTo: '',
    runtimeMin: '',
    runtimeMax: '',
    exitCode: ''
};
let savedSearches = JSON.parse(localStorage.getItem('savedSearches') || '[]');
let advancedFiltersVisible = false;

// Batch selection state
let selectedRunningJobs = new Set();
let selectedRecentJobs = new Set();

// Log analysis state
let detectedErrors = [];
let dismissedErrors = new Set();
let logAnalysisEnabled = true;

// Error pattern definitions
const errorPatterns = [
    {
        type: 'oom',
        label: 'Out of Memory',
        icon: '🔴',
        patterns: [
            /Killed/i,
            /Out of memory/i,
            /oom-kill/i,
            /Cannot allocate memory/i,
            /MemoryError/i,
            /CUDA out of memory/i,
            /tried to allocate.*GiB/i
        ]
    },
    {
        type: 'traceback',
        label: 'Python Traceback',
        icon: '🔴',
        patterns: [
            /Traceback \(most recent call last\)/i
        ],
        multiline: true,
        endPattern: /^\S/  // Ends when non-whitespace at start
    },
    {
        type: 'cuda',
        label: 'CUDA Error',
        icon: '🔴',
        patterns: [
            /CUDA error/i,
            /cudaError/i,
            /device-side assert/i,
            /NCCL error/i,
            /cuDNN error/i
        ]
    },
    {
        type: 'slurm',
        label: 'Slurm Error',
        icon: '🟠',
        patterns: [
            /DUE TO TIME LIMIT/i,
            /CANCELLED/i,
            /NODE_FAIL/i,
            /slurmstepd.*error/i
        ]
    },
    {
        type: 'segfault',
        label: 'Segmentation Fault',
        icon: '🔴',
        patterns: [
            /Segmentation fault/i,
            /core dumped/i,
            /SIGSEGV/i
        ]
    },
    {
        type: 'permission',
        label: 'Permission Error',
        icon: '🟠',
        patterns: [
            /Permission denied/i,
            /Access denied/i,
            /Operation not permitted/i
        ]
    },
    {
        type: 'file',
        label: 'File Error',
        icon: '🟠',
        patterns: [
            /No such file or directory/i,
            /File not found/i,
            /FileNotFoundError/i,
            /IOError/i
        ]
    },
    {
        type: 'assertion',
        label: 'Assertion Error',
        icon: '🔴',
        patterns: [
            /AssertionError/i,
            /assert.*failed/i
        ]
    },
    {
        type: 'warning',
        label: 'Warning',
        icon: '🟡',
        patterns: [
            /\bwarning\b/i,
            /DeprecationWarning/i,
            /UserWarning/i,
            /FutureWarning/i
        ],
        collapseRepeats: true,
        maxShow: 3
    }
];

const isNearBottom = () => (logPanel.scrollHeight - logPanel.clientHeight - logPanel.scrollTop) <= 32;

function relativeTime(dateStr) {
    const date = new Date(dateStr);
    const now = new Date();
    const diffMs = now - date;
    const diffMins = Math.floor(diffMs / 60000);
    if (diffMins < 1) return 'just now';
    if (diffMins < 60) return `${diffMins}m ago`;
    const diffHours = Math.floor(diffMins / 60);
    if (diffHours < 24) return `${diffHours}h ago`;
    const diffDays = Math.floor(diffHours / 24);
    return `${diffDays}d ago`;
}

function copyToClipboard(text, btn) {
    navigator.clipboard.writeText(text).then(() => {
        const orig = btn.textContent;
        btn.textContent = '✓';
        setTimeout(() => btn.textContent = orig, 1000);
    });
}

function toggleAutoScroll() {
    autoScroll = !autoScroll;
    scrollToggle.classList.toggle('active', autoScroll);
    scrollToggle.textContent = autoScroll ? 'Auto-scroll' : 'Manual';
}

// Watch/notification functions
function saveWatchedJobs() {
    localStorage.setItem('watchedJobs', JSON.stringify([...watchedJobs]));
}

function isJobWatched(jobId) {
    return watchedJobs.has(jobId);
}

async function toggleWatchJob(jobId, jobName) {
    if (watchedJobs.has(jobId)) {
        watchedJobs.delete(jobId);
        saveWatchedJobs();
        renderRunning(filterJobs(allRunningJobs));
        return;
    }

    // Request notification permission if not granted
    if (Notification.permission === 'default') {
        const permission = await Notification.requestPermission();
        if (permission !== 'granted') {
            alert('Notifications are blocked. Enable them in browser settings to watch jobs.');
            return;
        }
    }

    if (Notification.permission !== 'granted') {
        alert('Notifications are blocked. Enable them in browser settings to watch jobs.');
        return;
    }

    watchedJobs.add(jobId);
    saveWatchedJobs();
    renderRunning(filterJobs(allRunningJobs));

    // Show confirmation
    new Notification('Job Watch Added', {
        body: `You'll be notified when job ${jobId} (${jobName}) completes.`,
        icon: '/static/favicon.ico',
        tag: `watch-confirm-${jobId}`
    });
}

function checkJobStateChanges(currentJobs) {
    const currentById = {};
    for (const job of currentJobs) {
        currentById[job.id] = job;
    }

    // Check for state changes in watched jobs
    for (const jobId of watchedJobs) {
        const prevState = previousJobStates[jobId];
        const currentJob = currentById[jobId];

        // Job was running, now not in running list - it completed or failed
        if (prevState && prevState.state && !currentJob) {
            const wasRunning = prevState.state.toLowerCase().includes('running');
            if (wasRunning) {
                sendJobNotification(jobId, prevState.name, 'completed', prevState.runtime);
                watchedJobs.delete(jobId);
                saveWatchedJobs();
            }
        }

        // Job state changed (e.g., PENDING -> RUNNING, or RUNNING -> COMPLETED)
        if (prevState && currentJob && prevState.state !== currentJob.state) {
            const newState = currentJob.state.toLowerCase();
            if (newState.includes('completed') || newState.includes('failed') ||
                newState.includes('cancelled') || newState.includes('timeout')) {
                sendJobNotification(jobId, currentJob.name, newState, currentJob.runtime);
                watchedJobs.delete(jobId);
                saveWatchedJobs();
            }
        }
    }

    // Update previous states
    for (const job of currentJobs) {
        previousJobStates[job.id] = { state: job.state, name: job.name, runtime: job.runtime };
    }
}

function sendJobNotification(jobId, jobName, state, runtime) {
    if (Notification.permission !== 'granted') return;

    const isFailure = state.includes('failed') || state.includes('cancelled') || state.includes('timeout');
    const title = isFailure ? `Job Failed: ${jobName}` : `Job Completed: ${jobName}`;
    const icon = isFailure ? '❌' : '✅';

    const notification = new Notification(title, {
        body: `Job ${jobId} ${state}${runtime ? ` after ${runtime}` : ''}`,
        icon: '/static/favicon.ico',
        tag: `job-${jobId}`,
        requireInteraction: isFailure // Keep failure notifications visible
    });

    notification.onclick = () => {
        window.focus();
        // Find and highlight the job in recent jobs
        const rows = document.querySelectorAll('tbody tr');
        rows.forEach(row => {
            if (row.dataset.logKey && row.dataset.logKey.includes(jobId)) {
                row.scrollIntoView({ behavior: 'smooth', block: 'center' });
                row.classList.add('notification-highlight');
                setTimeout(() => row.classList.remove('notification-highlight'), 2000);
            }
        });
        notification.close();
    };
}

// Comparison functions
function toggleSelectForCompare(logKey, event) {
    if (event) event.stopPropagation();

    if (selectedForCompare.has(logKey)) {
        selectedForCompare.delete(logKey);
    } else {
        if (selectedForCompare.size >= 4) {
            alert('Maximum 4 jobs can be compared at once');
            return;
        }
        selectedForCompare.add(logKey);
    }

    updateCompareButton();
    renderRunning(filterJobs(allRunningJobs));
    renderRecent(filterJobs(allRecentJobs));
}

function updateCompareButton() {
    const btn = document.getElementById('compare-btn');
    if (!btn) return;

    const count = selectedForCompare.size;
    if (count >= 2) {
        btn.style.display = 'inline-flex';
        btn.textContent = `Compare (${count})`;
    } else {
        btn.style.display = 'none';
    }
}

function startComparison() {
    if (selectedForCompare.size < 2) return;

    compareMode = true;
    comparePanes = [...selectedForCompare].map(logKey => ({
        logKey,
        kind: 'stdout',
        content: '',
        stream: null
    }));

    renderCompareView();

    // Start streaming for each pane
    comparePanes.forEach((pane, idx) => {
        startCompareStream(idx);
    });
}

function exitCompareMode() {
    compareMode = false;
    comparePanes.forEach(pane => {
        if (pane.stream) {
            pane.stream.close();
            pane.stream = null;
        }
    });
    comparePanes = [];
    selectedForCompare.clear();

    // Restore normal view
    document.getElementById('log-wrapper').style.display = 'flex';
    const compareView = document.getElementById('compare-view');
    if (compareView) compareView.remove();

    updateCompareButton();
    renderRunning(filterJobs(allRunningJobs));
    renderRecent(filterJobs(allRecentJobs));
}

function renderCompareView() {
    // Hide normal log panel
    document.getElementById('log-wrapper').style.display = 'none';

    // Create comparison view
    let compareView = document.getElementById('compare-view');
    if (!compareView) {
        compareView = document.createElement('div');
        compareView.id = 'compare-view';
        compareView.className = 'compare-view';
        document.querySelector('.layout').appendChild(compareView);
    }

    const paneCount = comparePanes.length;
    const gridCols = paneCount <= 2 ? paneCount : 2;
    compareView.style.gridTemplateColumns = `repeat(${gridCols}, 1fr)`;

    compareView.innerHTML = `
        <div class="compare-header">
            <span>Comparing ${paneCount} jobs</span>
            <div class="compare-controls">
                <label class="sync-scroll-label">
                    <input type="checkbox" id="sync-scroll-toggle" onchange="toggleSyncScroll()">
                    Sync scroll
                </label>
                <button class="compare-exit-btn" onclick="exitCompareMode()">Exit Compare</button>
            </div>
        </div>
        <div class="compare-panes" style="grid-template-columns: repeat(${gridCols}, 1fr);">
            ${comparePanes.map((pane, idx) => `
                <div class="compare-pane card" data-pane-idx="${idx}">
                    <div class="compare-pane-header">
                        <span class="compare-pane-title">${pane.logKey}</span>
                        <div class="compare-pane-controls">
                            <button class="${pane.kind === 'stdout' ? 'active' : ''}" onclick="switchCompareKind(${idx}, 'stdout')">stdout</button>
                            <button class="${pane.kind === 'stderr' ? 'active' : ''}" onclick="switchCompareKind(${idx}, 'stderr')">stderr</button>
                            <button class="compare-pane-close" onclick="removeComparePane(${idx})">&times;</button>
                        </div>
                    </div>
                    <pre class="compare-pane-content" id="compare-pane-${idx}" onscroll="handleCompareScroll(${idx})"></pre>
                </div>
            `).join('')}
        </div>
    `;
}

function startCompareStream(idx) {
    const pane = comparePanes[idx];
    if (pane.stream) {
        pane.stream.close();
    }

    const panel = document.getElementById(`compare-pane-${idx}`);
    if (panel) panel.textContent = '';
    pane.content = '';

    const params = new URLSearchParams({ log_key: pane.logKey, kind: pane.kind });
    pane.stream = new EventSource(`/stream_log?${params.toString()}`);

    pane.stream.onmessage = evt => {
        try {
            const payload = JSON.parse(evt.data);
            if (Object.prototype.hasOwnProperty.call(payload, 'snapshot')) {
                pane.content = payload.snapshot;
                if (panel) panel.textContent = pane.content;
            }
            if (payload.append) {
                pane.content += payload.append;
                if (panel) panel.textContent = pane.content;
            }
        } catch (e) {
            console.error(e);
        }
    };

    pane.stream.onerror = () => {
        if (pane.stream) {
            pane.stream.close();
            pane.stream = null;
        }
    };
}

function switchCompareKind(idx, kind) {
    if (comparePanes[idx].kind === kind) return;
    comparePanes[idx].kind = kind;
    renderCompareView();
    startCompareStream(idx);
}

function removeComparePane(idx) {
    if (comparePanes[idx].stream) {
        comparePanes[idx].stream.close();
    }
    const logKey = comparePanes[idx].logKey;
    selectedForCompare.delete(logKey);
    comparePanes.splice(idx, 1);

    if (comparePanes.length < 2) {
        exitCompareMode();
    } else {
        renderCompareView();
        comparePanes.forEach((pane, i) => {
            if (!pane.stream) startCompareStream(i);
        });
    }
}

let syncScrollEnabled = false;
let isScrolling = false;

function toggleSyncScroll() {
    syncScrollEnabled = document.getElementById('sync-scroll-toggle').checked;
}

function handleCompareScroll(sourceIdx) {
    if (!syncScrollEnabled || isScrolling) return;

    isScrolling = true;
    const sourcePane = document.getElementById(`compare-pane-${sourceIdx}`);
    if (!sourcePane) return;

    const scrollRatio = sourcePane.scrollTop / (sourcePane.scrollHeight - sourcePane.clientHeight);

    comparePanes.forEach((pane, idx) => {
        if (idx !== sourceIdx) {
            const targetPane = document.getElementById(`compare-pane-${idx}`);
            if (targetPane) {
                targetPane.scrollTop = scrollRatio * (targetPane.scrollHeight - targetPane.clientHeight);
            }
        }
    });

    setTimeout(() => { isScrolling = false; }, 50);
}

// Annotation functions
function getAnnotationKey() {
    if (!currentLogKey || !currentLogKind) return null;
    return `${currentLogKey}::${currentLogKind}`;
}

function saveAnnotations() {
    localStorage.setItem('logAnnotations', JSON.stringify(annotations));
}

function getAnnotationsForCurrentLog() {
    const key = getAnnotationKey();
    if (!key) return {};
    return annotations[key] || {};
}

function addAnnotation(lineNumber) {
    const key = getAnnotationKey();
    if (!key) return;

    const text = prompt('Add annotation for line ' + lineNumber + ':');
    if (!text || !text.trim()) return;

    if (!annotations[key]) annotations[key] = {};
    annotations[key][lineNumber] = {
        text: text.trim(),
        createdAt: Date.now()
    };
    saveAnnotations();
    renderLogWithAnnotations();
}

function editAnnotation(lineNumber) {
    const key = getAnnotationKey();
    if (!key || !annotations[key] || !annotations[key][lineNumber]) return;

    const current = annotations[key][lineNumber].text;
    const text = prompt('Edit annotation:', current);
    if (text === null) return; // Cancelled

    if (!text.trim()) {
        deleteAnnotation(lineNumber);
        return;
    }

    annotations[key][lineNumber].text = text.trim();
    saveAnnotations();
    renderLogWithAnnotations();
}

function deleteAnnotation(lineNumber) {
    const key = getAnnotationKey();
    if (!key || !annotations[key]) return;

    delete annotations[key][lineNumber];
    if (Object.keys(annotations[key]).length === 0) {
        delete annotations[key];
    }
    saveAnnotations();
    renderLogWithAnnotations();
}

function renderLogWithAnnotations() {
    if (!originalLogContent) return;

    const currentAnnotations = getAnnotationsForCurrentLog();
    const lines = originalLogContent.split('\n');
    const annotatedLineNumbers = Object.keys(currentAnnotations).map(n => parseInt(n));

    // Update annotation count display
    updateAnnotationCount(annotatedLineNumbers.length);

    // Analyze log for errors
    if (logAnalysisEnabled) {
        detectedErrors = analyzeLogForErrors(lines);
        renderErrorSummary();
    }

    let html = '';
    let inTraceback = false;

    lines.forEach((line, idx) => {
        const lineNum = idx + 1;
        const hasAnnotation = currentAnnotations[lineNum];
        let escapedLine = line.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

        // Check if this line has an error
        const lineError = detectedErrors.find(e => e.line === lineNum);
        const isErrorLine = lineError && !dismissedErrors.has(`${lineError.type}-${lineError.line}`);

        // Track traceback state
        if (/Traceback \(most recent call last\)/i.test(line)) {
            inTraceback = true;
        } else if (inTraceback && /^\S/.test(line) && !/^\s*(File|at|in)\s/.test(line)) {
            inTraceback = false;
        }

        // Apply syntax highlighting
        escapedLine = applySyntaxHighlighting(escapedLine, inTraceback);

        const classes = ['log-line'];
        if (hasAnnotation) classes.push('annotated');
        if (isErrorLine) classes.push('error-line', `error-${lineError.type}`);
        if (inTraceback) classes.push('traceback-line');

        html += `<div class="${classes.join(' ')}" data-line="${lineNum}" id="log-line-${lineNum}">`;
        html += `<span class="line-number" onclick="handleLineClick(${lineNum})">${lineNum}</span>`;
        if (hasAnnotation) {
            html += `<span class="annotation-marker" title="${hasAnnotation.text.replace(/"/g, '&quot;')}" onclick="showAnnotationMenu(${lineNum}, event)">●</span>`;
        }
        if (isErrorLine) {
            html += `<span class="error-indicator" title="${lineError.label}">!</span>`;
        }
        html += `<span class="line-content">${escapedLine}</span>`;
        html += '</div>';
    });

    logPanel.innerHTML = html;
}

function analyzeLogForErrors(lines) {
    const errors = [];
    const seenTypes = {};

    lines.forEach((line, idx) => {
        const lineNum = idx + 1;

        for (const pattern of errorPatterns) {
            for (const regex of pattern.patterns) {
                if (regex.test(line)) {
                    // Track count per type
                    seenTypes[pattern.type] = (seenTypes[pattern.type] || 0) + 1;

                    // For collapsible patterns, limit how many we track
                    if (pattern.collapseRepeats && seenTypes[pattern.type] > (pattern.maxShow || 3)) {
                        // Just increment count, don't add new entry
                        continue;
                    }

                    // Extract relevant portion of line for preview
                    const match = line.match(regex);
                    const preview = line.slice(0, 80).trim() + (line.length > 80 ? '...' : '');

                    errors.push({
                        type: pattern.type,
                        label: pattern.label,
                        icon: pattern.icon,
                        line: lineNum,
                        preview: preview,
                        match: match ? match[0] : '',
                        count: seenTypes[pattern.type]
                    });
                    break; // Only one pattern match per line
                }
            }
        }
    });

    // Update counts for collapsible patterns
    for (const error of errors) {
        const pattern = errorPatterns.find(p => p.type === error.type);
        if (pattern && pattern.collapseRepeats) {
            error.totalCount = seenTypes[error.type];
        }
    }

    return errors;
}

function applySyntaxHighlighting(line, inTraceback) {
    // Timestamps (ISO, common log formats)
    line = line.replace(/(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?)/g,
        '<span class="hl-timestamp">$1</span>');

    // Line numbers in tracebacks
    if (inTraceback) {
        line = line.replace(/(File ".*?", line \d+)/g, '<span class="hl-file">$1</span>');
    }

    // Error keywords
    line = line.replace(/\b(Error|Exception|Failed|Failure|FAILED|ERROR)\b/gi,
        '<span class="hl-error">$1</span>');

    // Warning keywords
    line = line.replace(/\b(Warning|WARN|WARNING)\b/gi,
        '<span class="hl-warning">$1</span>');

    // Success keywords
    line = line.replace(/\b(Success|OK|PASS|PASSED|Completed)\b/gi,
        '<span class="hl-success">$1</span>');

    // Numbers and metrics
    line = line.replace(/\b(\d+(?:\.\d+)?%)\b/g, '<span class="hl-metric">$1</span>');

    return line;
}

function renderErrorSummary() {
    // Remove existing summary
    const existing = document.getElementById('error-summary');
    if (existing) existing.remove();

    // Filter out dismissed errors
    const activeErrors = detectedErrors.filter(e =>
        !dismissedErrors.has(`${e.type}-${e.line}`)
    );

    if (activeErrors.length === 0) return;

    // Group by type
    const grouped = {};
    for (const error of activeErrors) {
        if (!grouped[error.type]) {
            grouped[error.type] = {
                ...error,
                errors: []
            };
        }
        grouped[error.type].errors.push(error);
    }

    const summary = document.createElement('div');
    summary.id = 'error-summary';
    summary.className = 'error-summary';

    let html = `
        <div class="error-summary-header">
            <span class="error-summary-title">${activeErrors.length} issue(s) detected</span>
            <button class="error-summary-dismiss" onclick="dismissAllErrors()" title="Dismiss all">×</button>
        </div>
        <div class="error-summary-list">
    `;

    for (const [type, group] of Object.entries(grouped)) {
        const count = group.errors.length;
        const firstError = group.errors[0];
        const totalCount = firstError.totalCount || count;

        html += `
            <div class="error-summary-item" data-type="${type}">
                <span class="error-icon">${group.icon}</span>
                <div class="error-info">
                    <span class="error-type">${group.label}</span>
                    ${totalCount > count ? `<span class="error-count">(×${totalCount})</span>` : count > 1 ? `<span class="error-count">(×${count})</span>` : ''}
                    <span class="error-location">Line ${firstError.line}</span>
                </div>
                <div class="error-actions">
                    <button class="error-jump-btn" onclick="jumpToLogLine(${firstError.line})" title="Jump to line">↓</button>
                    <button class="error-dismiss-btn" onclick="dismissErrorType('${type}')" title="Dismiss">×</button>
                </div>
            </div>
        `;
    }

    html += '</div>';
    summary.innerHTML = html;

    // Insert before log panel
    const logWrapper = document.getElementById('log-wrapper');
    logWrapper.insertBefore(summary, logPanel);
}

function jumpToLogLine(lineNum) {
    const lineEl = document.getElementById(`log-line-${lineNum}`);
    if (lineEl) {
        lineEl.scrollIntoView({ behavior: 'smooth', block: 'center' });
        lineEl.classList.add('highlight-flash');
        setTimeout(() => lineEl.classList.remove('highlight-flash'), 2000);
    }
}

function dismissErrorType(type) {
    const errors = detectedErrors.filter(e => e.type === type);
    for (const error of errors) {
        dismissedErrors.add(`${error.type}-${error.line}`);
    }
    renderLogWithAnnotations();
}

function dismissAllErrors() {
    for (const error of detectedErrors) {
        dismissedErrors.add(`${error.type}-${error.line}`);
    }
    renderLogWithAnnotations();
}

function handleLineClick(lineNumber) {
    const currentAnnotations = getAnnotationsForCurrentLog();
    if (currentAnnotations[lineNumber]) {
        showAnnotationMenu(lineNumber, event);
    } else {
        addAnnotation(lineNumber);
    }
}

function showAnnotationMenu(lineNumber, event) {
    event.stopPropagation();

    // Remove existing menu
    const existingMenu = document.querySelector('.annotation-menu');
    if (existingMenu) existingMenu.remove();

    const currentAnnotations = getAnnotationsForCurrentLog();
    const annotation = currentAnnotations[lineNumber];
    if (!annotation) return;

    const menu = document.createElement('div');
    menu.className = 'annotation-menu';
    menu.innerHTML = `
        <div class="annotation-menu-text">${annotation.text}</div>
        <div class="annotation-menu-actions">
            <button onclick="editAnnotation(${lineNumber})">Edit</button>
            <button onclick="deleteAnnotation(${lineNumber})">Delete</button>
        </div>
    `;

    // Position near the click
    menu.style.left = event.clientX + 'px';
    menu.style.top = event.clientY + 'px';

    document.body.appendChild(menu);

    // Close on click outside
    setTimeout(() => {
        document.addEventListener('click', function closeMenu(e) {
            if (!menu.contains(e.target)) {
                menu.remove();
                document.removeEventListener('click', closeMenu);
            }
        });
    }, 0);
}

function updateAnnotationCount(count) {
    const countEl = document.getElementById('annotation-count');
    if (countEl) {
        countEl.textContent = count > 0 ? `${count} annotation${count > 1 ? 's' : ''}` : '';
        countEl.style.display = count > 0 ? 'inline' : 'none';
    }
}

function navigateAnnotation(direction) {
    const currentAnnotations = getAnnotationsForCurrentLog();
    const lineNumbers = Object.keys(currentAnnotations).map(n => parseInt(n)).sort((a, b) => a - b);
    if (lineNumbers.length === 0) return;

    // Find current scroll position's line
    const logLines = logPanel.querySelectorAll('.log-line');
    let currentLine = 1;
    for (const line of logLines) {
        const rect = line.getBoundingClientRect();
        const panelRect = logPanel.getBoundingClientRect();
        if (rect.top >= panelRect.top) {
            currentLine = parseInt(line.dataset.line);
            break;
        }
    }

    let targetLine;
    if (direction === 'next') {
        targetLine = lineNumbers.find(n => n > currentLine) || lineNumbers[0];
    } else {
        const reversed = [...lineNumbers].reverse();
        targetLine = reversed.find(n => n < currentLine) || reversed[0];
    }

    scrollToLine(targetLine);
}

function scrollToLine(lineNumber) {
    const line = logPanel.querySelector(`.log-line[data-line="${lineNumber}"]`);
    if (line) {
        line.scrollIntoView({ behavior: 'smooth', block: 'center' });
        line.classList.add('highlight-flash');
        setTimeout(() => line.classList.remove('highlight-flash'), 1000);
    }
}

// Log search functions
async function searchLogContent(query) {
    if (!currentLogKey || !query.trim()) {
        clearLogSearch();
        return;
    }

    logSearchCount.textContent = 'Searching...';

    try {
        const params = new URLSearchParams({
            log_key: currentLogKey,
            kind: currentLogKind,
            q: query,
            context: '0'
        });
        const res = await fetch(`/api/search_log?${params.toString()}`);
        const data = await res.json();

        if (data.error) {
            logSearchCount.textContent = data.error;
            logSearchResults = [];
            return;
        }

        logSearchResults = data.matches;
        logSearchCurrentIndex = logSearchResults.length > 0 ? 0 : -1;

        if (data.total_matches === 0) {
            logSearchCount.textContent = 'No matches';
        } else if (data.truncated) {
            logSearchCount.textContent = `${data.total_matches}+ matches`;
        } else {
            logSearchCount.textContent = `${logSearchCurrentIndex + 1}/${data.total_matches}`;
        }

        highlightMatches(query);
        if (logSearchCurrentIndex >= 0) {
            scrollToMatch(logSearchCurrentIndex);
        }

        updateSearchButtons();
    } catch (err) {
        console.error('Search error:', err);
        logSearchCount.textContent = 'Error';
    }
}

function highlightMatches(query) {
    if (!query.trim()) {
        logPanel.textContent = originalLogContent;
        return;
    }

    // Escape HTML but preserve the text
    const escapeHtml = (text) => {
        return text.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    };

    // Create regex for highlighting
    let regex;
    try {
        regex = new RegExp(`(${query})`, 'gi');
    } catch (e) {
        // Invalid regex, escape it for literal search
        regex = new RegExp(`(${query.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')})`, 'gi');
    }

    const content = originalLogContent;
    const highlighted = escapeHtml(content).replace(regex, '<mark>$1</mark>');
    logPanel.innerHTML = highlighted;

    // Mark the current match
    updateCurrentMatchHighlight();
}

function updateCurrentMatchHighlight() {
    const marks = logPanel.querySelectorAll('mark');
    marks.forEach((mark, i) => {
        mark.classList.toggle('current', i === logSearchCurrentIndex);
    });
}

function scrollToMatch(index) {
    const marks = logPanel.querySelectorAll('mark');
    if (marks[index]) {
        marks[index].scrollIntoView({ behavior: 'smooth', block: 'center' });
    }
}

function goToNextMatch() {
    if (logSearchResults.length === 0) return;
    const marks = logPanel.querySelectorAll('mark');
    if (marks.length === 0) return;

    logSearchCurrentIndex = (logSearchCurrentIndex + 1) % marks.length;
    updateCurrentMatchHighlight();
    scrollToMatch(logSearchCurrentIndex);
    logSearchCount.textContent = `${logSearchCurrentIndex + 1}/${marks.length}`;
}

function goToPrevMatch() {
    if (logSearchResults.length === 0) return;
    const marks = logPanel.querySelectorAll('mark');
    if (marks.length === 0) return;

    logSearchCurrentIndex = (logSearchCurrentIndex - 1 + marks.length) % marks.length;
    updateCurrentMatchHighlight();
    scrollToMatch(logSearchCurrentIndex);
    logSearchCount.textContent = `${logSearchCurrentIndex + 1}/${marks.length}`;
}

function clearLogSearch() {
    logSearchInput.value = '';
    logSearchCount.textContent = '';
    logSearchResults = [];
    logSearchCurrentIndex = -1;
    if (originalLogContent) {
        logPanel.textContent = originalLogContent;
    }
    updateSearchButtons();
}

function updateSearchButtons() {
    const hasMatches = logSearchResults.length > 0;
    logSearchPrev.disabled = !hasMatches;
    logSearchNext.disabled = !hasMatches;
}

function handleLogSearchInput(e) {
    clearTimeout(logSearchDebounceTimer);
    const query = e.target.value;

    if (!query.trim()) {
        clearLogSearch();
        return;
    }

    logSearchDebounceTimer = setTimeout(() => {
        searchLogContent(query);
    }, 300);
}

async function fetchJobs() {
    try {
        const res = await fetch('/api/jobs');
        if (!res.ok) return;
        const data = await res.json();
        allRunningJobs = data.running;
        allRecentJobs = data.recent;

        // Check for state changes in watched jobs
        checkJobStateChanges(data.running);

        document.getElementById('stat-running').textContent = data.running.length;
        const lastUpdatedEl = document.getElementById('last-updated');
        if (lastUpdatedEl) {
            const now = new Date();
            lastUpdatedEl.textContent = now.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
        }

        renderRunning(filterJobs(allRunningJobs));
        renderRecent(filterJobs(allRecentJobs));

        // Load queue info if there are pending jobs
        const hasPending = allRunningJobs.some(j => j.state && j.state.toLowerCase().includes('pending'));
        if (hasPending) {
            loadQueueInfo();
        }
    } catch (err) {
        console.error(err);
    }
}

async function loadQueueInfo() {
    try {
        const res = await fetch('/api/queue_info');
        if (!res.ok) return;
        const data = await res.json();

        // Store queue info by job ID
        queueInfo = {};
        if (data.pending_jobs) {
            for (const job of data.pending_jobs) {
                queueInfo[job.job_id] = job;
            }
        }

        // Re-render running jobs to show queue info
        renderRunning(filterJobs(allRunningJobs));
    } catch (err) {
        console.error('Queue info fetch error:', err);
    }
}

async function toggleJobDetails(jobId) {
    const detailsRow = document.getElementById(`details-${jobId}`);
    const expandBtn = document.querySelector(`[data-job-id="${jobId}"]`);

    if (expandedJobs.has(jobId)) {
        expandedJobs.delete(jobId);
        if (detailsRow) detailsRow.remove();
        if (expandBtn) expandBtn.textContent = '▸';
    } else {
        expandedJobs.add(jobId);
        if (expandBtn) expandBtn.textContent = '▾';

        if (!jobDetails[jobId]) {
            try {
                const res = await fetch(`/api/job_details/${jobId}`);
                if (res.ok) {
                    jobDetails[jobId] = await res.json();
                }
            } catch (err) {
                console.error(err);
            }
        }

        renderRunning(filterJobs(allRunningJobs));
        renderRecent(filterJobs(allRecentJobs));

        // Load live resource data for running jobs
        const job = allRunningJobs.find(j => j.id === jobId);
        if (job && job.state && job.state.toLowerCase().includes('running')) {
            loadResourceData(jobId);
        }

        // Load submission info for resubmit feature
        if (!jobSubmitInfo[jobId]) {
            loadSubmitInfo(jobId);
        }
    }
}

function getEfficiencyClass(value) {
    const num = parseFloat(value);
    if (isNaN(num)) return 'eff-medium';
    if (num >= 70) return 'eff-good';
    if (num >= 40) return 'eff-medium';
    return 'eff-bad';
}

function renderDetailsRow(job, colspan) {
    const details = jobDetails[job.id];
    if (!details) return '';

    const isRunning = job.state && job.state.toLowerCase().includes('running');
    const submitInfo = jobSubmitInfo[job.id];

    return `
        <tr class="details-row" id="details-${job.id}">
            <td colspan="${colspan}">
                <div class="details-content">
                    <div class="detail-item">
                        <div class="detail-label">Exit Code</div>
                        <div class="detail-value">${details.exit_code || 'N/A'}</div>
                    </div>
                    <div class="detail-item">
                        <div class="detail-label">CPU Efficiency</div>
                        <div class="detail-value">${details.cpu_eff || 'N/A'}</div>
                        ${details.cpu_eff && details.cpu_eff !== 'N/A' ? `
                            <div class="efficiency-bar">
                                <div class="efficiency-fill ${getEfficiencyClass(details.cpu_eff)}" style="width: ${details.cpu_eff}"></div>
                            </div>
                        ` : ''}
                    </div>
                    <div class="detail-item">
                        <div class="detail-label">Memory Efficiency</div>
                        <div class="detail-value">${details.mem_eff || 'N/A'}</div>
                        ${details.mem_eff && details.mem_eff !== 'N/A' ? `
                            <div class="efficiency-bar">
                                <div class="efficiency-fill ${getEfficiencyClass(details.mem_eff)}" style="width: ${details.mem_eff}"></div>
                            </div>
                        ` : ''}
                    </div>
                    <div class="detail-item">
                        <div class="detail-label">State</div>
                        <div class="detail-value">${details.state || 'N/A'}</div>
                    </div>
                    <div class="detail-item">
                        <div class="detail-label">End Time</div>
                        <div class="detail-value">${details.end_time || 'N/A'}</div>
                    </div>
                    ${details.service_units !== null && details.service_units !== undefined ? `
                    <div class="detail-item cost-detail">
                        <div class="detail-label">Cost (SUs)</div>
                        <div class="detail-value cost-value">${formatSUs(details.service_units)}</div>
                    </div>
                    ` : ''}
                    ${isRunning ? `
                    <div class="detail-item resource-chart-item" id="resource-chart-${job.id}">
                        <div class="detail-label">Live Resources</div>
                        <div class="resource-loading">Loading...</div>
                    </div>
                    ` : ''}
                    <div class="detail-item resubmit-section" id="resubmit-section-${job.id}">
                        ${submitInfo ? renderResubmitSection(job.id, submitInfo) : `
                            <div class="detail-label">Resubmit</div>
                            <div class="resubmit-loading">Loading submission info...</div>
                        `}
                    </div>
                </div>
            </td>
        </tr>
    `;
}

function renderResubmitSection(jobId, info) {
    if (info.error) {
        return `
            <div class="detail-label">Resubmit</div>
            <div class="resubmit-unavailable">${info.error}</div>
        `;
    }

    return `
        <div class="detail-label">Resubmit Job</div>
        <div class="resubmit-info">
            ${info.work_dir ? `<div class="resubmit-field"><span class="resubmit-label">Work Dir:</span> <span class="resubmit-value">${info.work_dir}</span></div>` : ''}
            ${info.script_path ? `<div class="resubmit-field"><span class="resubmit-label">Script:</span> <span class="resubmit-value">${info.script_path}</span></div>` : ''}
            ${info.partition ? `<div class="resubmit-field"><span class="resubmit-label">Partition:</span> <span class="resubmit-value">${info.partition}</span></div>` : ''}
            ${info.timelimit ? `<div class="resubmit-field"><span class="resubmit-label">Time Limit:</span> <span class="resubmit-value">${info.timelimit}</span></div>` : ''}
            ${info.req_mem ? `<div class="resubmit-field"><span class="resubmit-label">Memory:</span> <span class="resubmit-value">${info.req_mem}</span></div>` : ''}
        </div>
        <div class="resubmit-actions">
            ${info.script_path ? `
                <button class="resubmit-btn" onclick="viewScript('${info.script_path.replace(/'/g, "\\'")}')">View Script</button>
                <button class="resubmit-btn primary" onclick="showResubmitDialog('${jobId}')">Resubmit</button>
            ` : `<span class="resubmit-unavailable">Script path not available</span>`}
        </div>
    `;
}

async function loadSubmitInfo(jobId) {
    try {
        const res = await fetch(`/api/job_submit_info/${jobId}`);
        const data = await res.json();
        jobSubmitInfo[jobId] = data;

        const section = document.getElementById(`resubmit-section-${jobId}`);
        if (section) {
            section.innerHTML = renderResubmitSection(jobId, data);
        }
    } catch (err) {
        console.error('Submit info fetch error:', err);
    }
}

async function viewScript(scriptPath) {
    try {
        const res = await fetch(`/api/script_content?path=${encodeURIComponent(scriptPath)}`);
        const data = await res.json();

        if (data.error) {
            alert('Could not load script: ' + data.error);
            return;
        }

        // Create a modal to display the script
        showScriptModal(scriptPath, data.content, data.truncated);
    } catch (err) {
        alert('Error loading script');
    }
}

function showScriptModal(path, content, truncated) {
    // Remove existing modal
    const existing = document.getElementById('script-modal');
    if (existing) existing.remove();

    const modal = document.createElement('div');
    modal.id = 'script-modal';
    modal.className = 'modal-overlay';
    modal.innerHTML = `
        <div class="modal-content">
            <div class="modal-header">
                <h3>Script: ${path.split('/').pop()}</h3>
                <button class="modal-close" onclick="closeScriptModal()">&times;</button>
            </div>
            <pre class="script-content">${escapeHtml(content)}</pre>
            ${truncated ? '<div class="script-truncated">Script truncated (max 200 lines)</div>' : ''}
        </div>
    `;

    document.body.appendChild(modal);

    // Close on click outside
    modal.addEventListener('click', (e) => {
        if (e.target === modal) closeScriptModal();
    });
}

function closeScriptModal() {
    const modal = document.getElementById('script-modal');
    if (modal) modal.remove();
}

function escapeHtml(text) {
    return text.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

function showResubmitDialog(jobId) {
    const info = jobSubmitInfo[jobId];
    if (!info || info.error || !info.script_path) {
        alert('Cannot resubmit: script path not available');
        return;
    }

    // Remove existing modal
    const existing = document.getElementById('resubmit-modal');
    if (existing) existing.remove();

    const modal = document.createElement('div');
    modal.id = 'resubmit-modal';
    modal.className = 'modal-overlay';
    modal.innerHTML = `
        <div class="modal-content resubmit-dialog">
            <div class="modal-header">
                <h3>Resubmit Job</h3>
                <button class="modal-close" onclick="closeResubmitDialog()">&times;</button>
            </div>
            <div class="resubmit-form">
                <div class="form-group">
                    <label>Script</label>
                    <input type="text" id="resubmit-script" value="${info.script_path}" readonly>
                </div>
                <div class="form-group">
                    <label>Working Directory</label>
                    <input type="text" id="resubmit-workdir" value="${info.work_dir || ''}" placeholder="Original directory">
                </div>
                <div class="form-row">
                    <div class="form-group">
                        <label>Partition</label>
                        <input type="text" id="resubmit-partition" value="${info.partition || ''}" placeholder="Default">
                    </div>
                    <div class="form-group">
                        <label>Time Limit</label>
                        <input type="text" id="resubmit-time" value="${info.timelimit || ''}" placeholder="e.g., 1:00:00">
                    </div>
                </div>
                <div class="form-row">
                    <div class="form-group">
                        <label>Memory</label>
                        <input type="text" id="resubmit-memory" value="${info.req_mem || ''}" placeholder="e.g., 4G">
                    </div>
                    <div class="form-group">
                        <label>CPUs</label>
                        <input type="number" id="resubmit-cpus" value="${info.req_cpus || ''}" placeholder="Default">
                    </div>
                </div>
            </div>
            <div class="modal-footer">
                <button class="btn-secondary" onclick="closeResubmitDialog()">Cancel</button>
                <button class="btn-primary" onclick="submitResubmit()">Submit Job</button>
            </div>
            <div id="resubmit-status"></div>
        </div>
    `;

    document.body.appendChild(modal);
}

function closeResubmitDialog() {
    const modal = document.getElementById('resubmit-modal');
    if (modal) modal.remove();
}

async function submitResubmit() {
    const scriptPath = document.getElementById('resubmit-script').value;
    const workDir = document.getElementById('resubmit-workdir').value;
    const partition = document.getElementById('resubmit-partition').value;
    const timeLimit = document.getElementById('resubmit-time').value;
    const memory = document.getElementById('resubmit-memory').value;
    const cpus = document.getElementById('resubmit-cpus').value;

    const statusEl = document.getElementById('resubmit-status');
    statusEl.textContent = 'Submitting...';
    statusEl.className = 'resubmit-status submitting';

    try {
        const res = await fetch('/api/resubmit', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                script_path: scriptPath,
                work_dir: workDir || null,
                partition: partition || null,
                time_limit: timeLimit || null,
                memory: memory || null,
                cpus: cpus ? parseInt(cpus) : null,
            }),
        });

        const data = await res.json();

        if (data.error) {
            statusEl.textContent = 'Error: ' + data.error;
            statusEl.className = 'resubmit-status error';
        } else {
            statusEl.innerHTML = `<strong>Success!</strong> New job ID: <span class="metric">${data.job_id}</span>`;
            statusEl.className = 'resubmit-status success';

            // Refresh job list after short delay
            setTimeout(() => {
                fetchJobs();
                closeResubmitDialog();
            }, 2000);
        }
    } catch (err) {
        statusEl.textContent = 'Error submitting job';
        statusEl.className = 'resubmit-status error';
    }
}

async function loadResourceData(jobId) {
    try {
        const res = await fetch(`/api/job_resources/${jobId}`);
        if (!res.ok) return;
        const data = await res.json();

        if (data.error) {
            const container = document.getElementById(`resource-chart-${jobId}`);
            if (container) {
                container.innerHTML = `
                    <div class="detail-label">Live Resources</div>
                    <div class="resource-unavailable">${data.error}</div>
                `;
            }
            return;
        }

        // Store in history for chart
        if (!resourceHistory[jobId]) {
            resourceHistory[jobId] = [];
        }
        resourceHistory[jobId].push({
            time: Date.now(),
            max_rss: data.max_rss,
            ave_rss: data.ave_rss,
        });
        // Keep last 30 samples (about 15 minutes at 30s intervals)
        if (resourceHistory[jobId].length > 30) {
            resourceHistory[jobId].shift();
        }

        renderResourceChart(jobId, data);
    } catch (err) {
        console.error('Resource fetch error:', err);
    }
}

function renderResourceChart(jobId, data) {
    const container = document.getElementById(`resource-chart-${jobId}`);
    if (!container) return;

    const history = resourceHistory[jobId] || [];

    // Parse memory values for display
    const maxRss = data.max_rss || 'N/A';
    const aveRss = data.ave_rss || 'N/A';

    // Generate mini chart SVG if we have history
    let chartHtml = '';
    if (history.length > 1) {
        chartHtml = generateMiniChart(history, jobId);
    }

    container.innerHTML = `
        <div class="detail-label">Live Resources</div>
        <div class="resource-metrics">
            <div class="resource-metric">
                <span class="resource-label">Max Memory:</span>
                <span class="resource-value">${maxRss}</span>
            </div>
            <div class="resource-metric">
                <span class="resource-label">Avg Memory:</span>
                <span class="resource-value">${aveRss}</span>
            </div>
            ${data.ntasks ? `
            <div class="resource-metric">
                <span class="resource-label">Tasks:</span>
                <span class="resource-value">${data.ntasks}</span>
            </div>
            ` : ''}
        </div>
        ${chartHtml}
    `;
}

function generateMiniChart(history, jobId) {
    const width = 200;
    const height = 40;
    const padding = 2;

    // Parse memory values to bytes for comparison
    const parseMemToBytes = (memStr) => {
        if (!memStr) return 0;
        const match = memStr.match(/([\d.]+)([KMGT]?)/i);
        if (!match) return 0;
        const value = parseFloat(match[1]);
        const unit = (match[2] || 'K').toUpperCase();
        const multipliers = { K: 1024, M: 1024**2, G: 1024**3, T: 1024**4 };
        return value * (multipliers[unit] || 1024);
    };

    const values = history.map(h => parseMemToBytes(h.max_rss));
    const maxVal = Math.max(...values, 1);

    // Generate path points
    const points = values.map((v, i) => {
        const x = padding + (i / (values.length - 1)) * (width - 2 * padding);
        const y = height - padding - (v / maxVal) * (height - 2 * padding);
        return `${x},${y}`;
    }).join(' ');

    // Generate area fill
    const areaPoints = [
        `${padding},${height - padding}`,
        ...values.map((v, i) => {
            const x = padding + (i / (values.length - 1)) * (width - 2 * padding);
            const y = height - padding - (v / maxVal) * (height - 2 * padding);
            return `${x},${y}`;
        }),
        `${width - padding},${height - padding}`
    ].join(' ');

    return `
        <div class="resource-chart-container">
            <svg class="resource-mini-chart" viewBox="0 0 ${width} ${height}">
                <polygon points="${areaPoints}" class="chart-area"/>
                <polyline points="${points}" class="chart-line"/>
            </svg>
            <div class="chart-label">Memory over time</div>
        </div>
    `;
}

function filterJobs(jobs) {
    let filtered = jobs;

    // Basic search query
    if (searchQuery) {
        const q = searchQuery.toLowerCase();
        filtered = filtered.filter(job =>
            (job.name && job.name.toLowerCase().includes(q)) ||
            (job.id && job.id.toString().includes(q)) ||
            (job.state && job.state.toLowerCase().includes(q))
        );
    }

    // Quick filters
    if (activeQuickFilters.size > 0) {
        filtered = filtered.filter(job => {
            for (const filter of activeQuickFilters) {
                if (!matchesQuickFilter(job, filter)) return false;
            }
            return true;
        });
    }

    // Advanced filters
    filtered = applyAdvancedFiltersToJobs(filtered);

    return filtered;
}

function matchesQuickFilter(job, filter) {
    const state = (job.state || '').toLowerCase();
    const now = new Date();
    const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());

    switch (filter) {
        case 'failed':
            // If job has no state (log-only), don't filter it out
            if (!job.state) return false;
            return state.includes('failed') || state.includes('cancelled') || state.includes('timeout');
        case 'today':
            const updated = job.updated || job.start_time;
            if (!updated) return true; // Include jobs without date info
            const jobDate = new Date(updated);
            return jobDate >= today;
        case 'gpu':
            // Check if job uses GPUs (based on partition name, GRES, or job name)
            const partition = (job.partition || '').toLowerCase();
            const name = (job.name || '').toLowerCase();
            return partition.includes('gpu') ||
                   (job.gres && job.gres.includes('gpu')) ||
                   name.includes('gpu');
        case 'long':
            // Jobs running > 1 hour; if no runtime info, don't filter out
            if (!job.runtime) return false;
            const runtime = parseRuntimeToMinutes(job.runtime);
            return runtime >= 60;
        case 'pending':
            if (!job.state) return false;
            return state.includes('pending') || state.includes('configuring');
        default:
            return true;
    }
}

function parseRuntimeToMinutes(runtime) {
    if (!runtime) return 0;
    // Parse formats like "1:23:45" (h:m:s), "23:45" (m:s), "1-02:03:04" (d-h:m:s)
    const parts = runtime.split('-');
    let days = 0;
    let timeStr = runtime;

    if (parts.length === 2) {
        days = parseInt(parts[0]) || 0;
        timeStr = parts[1];
    }

    const timeParts = timeStr.split(':').map(p => parseInt(p) || 0);
    let hours = 0, minutes = 0, seconds = 0;

    if (timeParts.length === 3) {
        [hours, minutes, seconds] = timeParts;
    } else if (timeParts.length === 2) {
        [minutes, seconds] = timeParts;
    }

    return (days * 24 * 60) + (hours * 60) + minutes + (seconds / 60);
}

function applyAdvancedFiltersToJobs(jobs) {
    return jobs.filter(job => {
        // State filter
        if (advancedFilters.state) {
            const jobState = (job.state || '').toLowerCase();
            const filterState = advancedFilters.state.toLowerCase();
            if (!jobState.includes(filterState)) return false;
        }

        // Partition filter
        if (advancedFilters.partition) {
            const jobPartition = (job.partition || '').toLowerCase();
            if (jobPartition !== advancedFilters.partition.toLowerCase()) return false;
        }

        // Name pattern filter
        if (advancedFilters.name) {
            const jobName = job.name || '';
            if (advancedFilters.nameRegex) {
                try {
                    const regex = new RegExp(advancedFilters.name, 'i');
                    if (!regex.test(jobName)) return false;
                } catch (e) {
                    // Invalid regex, fall back to simple includes
                    if (!jobName.toLowerCase().includes(advancedFilters.name.toLowerCase())) return false;
                }
            } else {
                // Support wildcards (* and ?)
                const pattern = advancedFilters.name.replace(/\*/g, '.*').replace(/\?/g, '.');
                try {
                    const regex = new RegExp('^' + pattern + '$', 'i');
                    if (!regex.test(jobName)) return false;
                } catch (e) {
                    if (!jobName.toLowerCase().includes(advancedFilters.name.toLowerCase())) return false;
                }
            }
        }

        // Date range filter
        if (advancedFilters.dateFrom || advancedFilters.dateTo) {
            const jobDate = job.updated || job.start_time;
            if (!jobDate) return false;
            const date = new Date(jobDate);

            if (advancedFilters.dateFrom) {
                const from = new Date(advancedFilters.dateFrom);
                if (date < from) return false;
            }
            if (advancedFilters.dateTo) {
                const to = new Date(advancedFilters.dateTo);
                to.setHours(23, 59, 59, 999);
                if (date > to) return false;
            }
        }

        // Runtime filter
        if (advancedFilters.runtimeMin || advancedFilters.runtimeMax) {
            const runtime = parseRuntimeToMinutes(job.runtime || '0:00:00');
            const minMinutes = parseFilterRuntime(advancedFilters.runtimeMin);
            const maxMinutes = parseFilterRuntime(advancedFilters.runtimeMax);

            if (minMinutes !== null && runtime < minMinutes) return false;
            if (maxMinutes !== null && runtime > maxMinutes) return false;
        }

        // Exit code filter
        if (advancedFilters.exitCode) {
            const exitCode = job.exit_code || (jobDetails[job.id] && jobDetails[job.id].exit_code);
            if (exitCode === undefined) return true; // Don't filter jobs without exit code info

            const filterCode = advancedFilters.exitCode.trim();
            if (filterCode.startsWith('!')) {
                // Not equal
                const code = filterCode.slice(1);
                if (exitCode === code) return false;
            } else {
                if (exitCode !== filterCode) return false;
            }
        }

        return true;
    });
}

function parseFilterRuntime(str) {
    if (!str) return null;
    str = str.toLowerCase().trim();

    // Parse formats like "1h", "30m", "2h30m", "1d", "90"
    let totalMinutes = 0;
    let match;

    // Days
    match = str.match(/(\d+)d/);
    if (match) totalMinutes += parseInt(match[1]) * 24 * 60;

    // Hours
    match = str.match(/(\d+)h/);
    if (match) totalMinutes += parseInt(match[1]) * 60;

    // Minutes
    match = str.match(/(\d+)m(?!s)/); // Avoid matching 'ms'
    if (match) totalMinutes += parseInt(match[1]);

    // If just a number, treat as minutes
    if (totalMinutes === 0 && /^\d+$/.test(str)) {
        totalMinutes = parseInt(str);
    }

    return totalMinutes > 0 ? totalMinutes : null;
}

function getStatusClass(state) {
    const s = state.toLowerCase();
    if (s.includes('running')) return 'status-running';
    if (s.includes('pending') || s.includes('configuring')) return 'status-pending';
    if (s.includes('completed')) return 'status-completed';
    if (s.includes('failed') || s.includes('cancelled') || s.includes('error')) return 'status-failed';
    if (s.includes('timeout')) return 'status-timeout';
    return 'status-pending';
}

function formatStateShort(state) {
    const s = state.toUpperCase();
    if (s.startsWith('PEND')) return 'PD';
    if (s.startsWith('RUN')) return 'R';
    if (s.startsWith('COMPLETED')) return 'CD';
    if (s.startsWith('CANCEL')) return 'CA';
    if (s.includes('TIMEOUT')) return 'TO';
    if (s.includes('CONFIG')) return 'CF';
    return s.split('+')[0].slice(0, 4);
}

function formatSUs(sus) {
    if (sus === undefined || sus === null) return '0';
    if (sus >= 1000000) return (sus / 1000000).toFixed(1) + 'M';
    if (sus >= 1000) return (sus / 1000).toFixed(1) + 'K';
    return sus.toFixed(1);
}

function renderRunning(rows) {
    if (!rows.length) {
        runningBody.innerHTML = '<tr><td colspan="5"><div class="empty-state">No running jobs.</div></td></tr>';
        updateBatchActionsVisibility('running');
        return;
    }
    runningBody.innerHTML = rows.map(job => {
        const watched = isJobWatched(job.id);
        const isPending = job.state && job.state.toLowerCase().includes('pending');
        const jobQueueInfo = queueInfo[job.id];
        const isSelected = selectedRunningJobs.has(job.id);

        // Build queue info display for pending jobs
        let queueDisplay = '';
        if (isPending && jobQueueInfo) {
            const conf = jobQueueInfo.confidence || 'low';
            const confClass = conf === 'high' ? 'confidence-high' : conf === 'medium' ? 'confidence-medium' : 'confidence-low';
            queueDisplay = `
                <div class="queue-info">
                    <span class="queue-position" title="Position in partition queue">
                        #${jobQueueInfo.position || '?'}/${jobQueueInfo.total_in_partition || '?'}
                    </span>
                    ${jobQueueInfo.estimated_wait ? `
                        <span class="queue-wait ${confClass}" title="Estimated wait time (${conf} confidence)">
                            ~${jobQueueInfo.estimated_wait}
                        </span>
                    ` : ''}
                </div>
            `;
        }

        return `
        <tr class="${currentLogKey === job.log_key ? 'active-log' : ''} ${watched ? 'watched-job' : ''} ${isSelected ? 'batch-selected' : ''}" data-log-key="${job.log_key}" data-job-id="${job.id}">
            <td class="select-col">
                <input type="checkbox" class="batch-select" ${isSelected ? 'checked' : ''} onchange="toggleJobSelection('running', '${job.id}', this.checked)">
            </td>
            <td class="job-cell col-main">
                <div class="job-main-top">
                    <span class="expand-btn" data-job-id="${job.id}" onclick="toggleJobDetails('${job.id}')">${expandedJobs.has(job.id) ? '▾' : '▸'}</span>
                    <span class="metric">${job.id}</span>
                    <button class="copy-btn" onclick="copyToClipboard('${job.id}', this)">⎘</button>
                    <button class="watch-btn ${watched ? 'watching' : ''}" onclick="toggleWatchJob('${job.id}', '${job.name.replace(/'/g, "\\'")}')" title="${watched ? 'Stop watching' : 'Watch for completion'}">
                        ${watched ? '★' : '☆'}
                    </button>
                </div>
                <div class="job-main-name" title="${job.name}">${job.name}</div>
                ${queueDisplay}
            </td>
            <td class="col-state"><span class="status-badge ${getStatusClass(job.state)}">${formatStateShort(job.state)}</span></td>
            <td class="col-runtime"><span class="metric">${job.runtime}</span></td>
            <td class="col-actions">
                <button onclick="openLog('${job.log_key}','stdout')">stdout</button>
                <button onclick="openLog('${job.log_key}','stderr')">stderr</button>
                <button onclick="cancelJob('${job.id}')" style="background: var(--status-failed-bg); color: var(--status-failed-text);">cancel</button>
            </td>
        </tr>
        ${expandedJobs.has(job.id) ? renderDetailsRow(job, 5) : ''}`;
    }).join('');
    updateBatchActionsVisibility('running');
}

function renderRecent(rows) {
    if (!rows.length) {
        recentBody.innerHTML = '<tr><td colspan="8"><div class="empty-state">No recent logs found.</div></td></tr>';
        updateBatchActionsVisibility('recent');
        return;
    }
    recentBody.innerHTML = rows.map(job => {
        const selectedCompare = selectedForCompare.has(job.log_key);
        const isSelected = selectedRecentJobs.has(job.id);
        return `
        <tr class="${currentLogKey === job.log_key ? 'active-log' : ''} ${selectedCompare ? 'selected-for-compare' : ''} ${isSelected ? 'batch-selected' : ''}" data-log-key="${job.log_key}" data-job-id="${job.id}">
            <td class="select-col">
                <input type="checkbox" class="batch-select" ${isSelected ? 'checked' : ''} onchange="toggleJobSelection('recent', '${job.id}', this.checked)">
            </td>
            <td style="font-size: 12px; color: var(--text-faint);" title="${job.updated}">${relativeTime(job.updated)}</td>
            <td class="job-cell">${job.name}</td>
            <td>
                <span class="expand-btn" data-job-id="${job.id}" onclick="toggleJobDetails('${job.id}')">${expandedJobs.has(job.id) ? '▾' : '▸'}</span>
                <span class="metric">${job.id}</span>
                <button class="copy-btn" onclick="copyToClipboard('${job.id}', this)">⎘</button>
            </td>
            <td><button onclick="openLog('${job.log_key}','stdout')">stdout</button></td>
            <td><button onclick="openLog('${job.log_key}','stderr')">stderr</button></td>
            <td><span class="metric">${job.size}</span></td>
        </tr>
        ${expandedJobs.has(job.id) ? renderDetailsRow(job, 8) : ''}`;
    }).join('');
    updateBatchActionsVisibility('recent');
}

async function cancelJob(jobId) {
    if (!confirm(`Cancel job ${jobId}?`)) return;
    try {
        const res = await fetch(`/api/cancel/${jobId}`, { method: 'POST' });
        if (res.ok) {
            await fetchJobs();
        } else {
            alert('Failed to cancel job');
        }
    } catch (err) {
        console.error(err);
        alert('Error canceling job');
    }
}

function openLog(logKey, kind) {
    currentLogKey = logKey;
    currentLogKind = kind;
    logTitle.textContent = `Logs: ${logKey} (${kind})`;
    logPanel.innerHTML = '';
    originalLogContent = '';
    clearLogSearch();
    if (logStream) logStream.close();

    // Update annotation count for new log
    const currentAnnotations = getAnnotationsForCurrentLog();
    updateAnnotationCount(Object.keys(currentAnnotations).length);

    document.querySelectorAll('tbody tr').forEach(row => {
        row.classList.toggle('active-log', row.dataset.logKey === logKey);
    });

    const params = new URLSearchParams({ log_key: logKey, kind });
    logStream = new EventSource(`/stream_log?${params.toString()}`);
    logStream.onmessage = evt => {
        try {
            const payload = JSON.parse(evt.data);
            let stick = autoScroll && isNearBottom();
            if (payload.reset) {
                logPanel.innerHTML = '';
                originalLogContent = '';
                logPanel.scrollTop = 0;
                stick = false;
            }
            if (Object.prototype.hasOwnProperty.call(payload, 'snapshot')) {
                originalLogContent = payload.snapshot;
                renderLogWithAnnotations();
                logPanel.scrollTop = 0;
                stick = false;
            }
            if (payload.append) {
                originalLogContent += payload.append;
                renderLogWithAnnotations();
            }
            if (stick && autoScroll) {
                logPanel.scrollTop = logPanel.scrollHeight;
            }
        } catch (e) {
            console.error(e);
        }
    };
    logStream.onerror = () => {
        if (logStream) {
            logStream.close();
            logStream = null;
        }
    };
}

function sortTable(tableId, column, data) {
    const isRecent = tableId === 'recent';
    if (sortState.table === tableId && sortState.column === column) {
        sortState.direction = sortState.direction === 'asc' ? 'desc' : 'asc';
    } else {
        sortState.table = tableId;
        sortState.column = column;
        sortState.direction = 'asc';
    }

    const sorted = [...data].sort((a, b) => {
        let valA = a[column] || '';
        let valB = b[column] || '';

        if (column === 'updated') {
            valA = new Date(a[column]);
            valB = new Date(b[column]);
        } else if (column === 'size') {
            valA = parseFloat(a.size_bytes || 0);
            valB = parseFloat(b.size_bytes || 0);
        } else {
            valA = valA.toString().toLowerCase();
            valB = valB.toString().toLowerCase();
        }

        if (valA < valB) return sortState.direction === 'asc' ? -1 : 1;
        if (valA > valB) return sortState.direction === 'asc' ? 1 : -1;
        return 0;
    });

    document.querySelectorAll(`#${tableId} th`).forEach(th => {
        th.classList.remove('sort-asc', 'sort-desc');
    });
    const th = document.querySelector(`#${tableId} th[data-sort="${column}"]`);
    if (th) th.classList.add(`sort-${sortState.direction}`);

    if (isRecent) {
        renderRecent(sorted);
    } else {
        renderRunning(sorted);
    }
}

// Event listeners
themeToggle.addEventListener('click', toggleTheme);

searchBox.addEventListener('input', (e) => {
    searchQuery = e.target.value;
    renderRunning(filterJobs(allRunningJobs));
    renderRecent(filterJobs(allRecentJobs));
});

// Log search event listeners
logSearchInput.addEventListener('input', handleLogSearchInput);
logSearchInput.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') {
        e.preventDefault();
        if (e.shiftKey) {
            goToPrevMatch();
        } else {
            goToNextMatch();
        }
    }
    if (e.key === 'Escape') {
        clearLogSearch();
        logSearchInput.blur();
    }
});
logSearchPrev.addEventListener('click', goToPrevMatch);
logSearchNext.addEventListener('click', goToNextMatch);
logSearchClose.addEventListener('click', clearLogSearch);

document.addEventListener('keydown', (e) => {
    if (e.key === '/' && e.target.tagName !== 'INPUT') {
        e.preventDefault();
        searchBox.focus();
    }
    // Ctrl+G or Cmd+G to focus log search
    if ((e.key === 'g' || e.key === 'G') && (e.ctrlKey || e.metaKey) && currentLogKey) {
        e.preventDefault();
        logSearchInput.focus();
    }
    // Ctrl+F or Cmd+F in log panel area focuses log search
    if ((e.key === 'f' || e.key === 'F') && (e.ctrlKey || e.metaKey)) {
        const logWrapper = document.getElementById('log-wrapper');
        if (logWrapper && logWrapper.contains(document.activeElement)) {
            e.preventDefault();
            logSearchInput.focus();
        }
    }
    if (e.key === 'Escape') {
        searchBox.blur();
        logSearchInput.blur();
        if (logSearchInput.value) {
            clearLogSearch();
        } else if (logStream) {
            logStream.close();
            logStream = null;
            currentLogKey = null;
            originalLogContent = '';
            logTitle.textContent = 'Logs';
            logPanel.textContent = '';
            document.querySelectorAll('tbody tr').forEach(row => {
                row.classList.remove('active-log');
            });
        }
    }
    if (e.key === 'r' && e.target.tagName !== 'INPUT') {
        e.preventDefault();
        fetchJobs();
    }
});

document.querySelectorAll('#running-table th.sortable').forEach(th => {
    th.addEventListener('click', () => {
        const column = th.dataset.sort;
        sortTable('running-table', column, filterJobs(allRunningJobs));
    });
});

document.querySelectorAll('#recent-table th.sortable').forEach(th => {
    th.addEventListener('click', () => {
        const column = th.dataset.sort;
        sortTable('recent-table', column, filterJobs(allRecentJobs));
    });
});

// Timeline view state
let currentView = 'table';
let timelineData = [];
let timelineZoom = 'day'; // 'hour', 'day', 'week'
let filteredTimelineData = [];
let timelineZoomFactor = 1; // 1 = 100%, 2 = 200%, etc.
let timelinePanOffset = 0; // Pan offset in percentage of total width
let timelineIsPanning = false;
let timelinePanStart = 0;
let timelineGrouped = {}; // Grouped job arrays

// DAG view state
let dagData = { nodes: [], edges: [], pipelines: [] };
let selectedPipeline = null;

// Heatmap view state
let heatmapData = null;
let currentHeatmapView = 'activity'; // 'activity', 'success', 'pattern'

function setView(view) {
    currentView = view;
    document.getElementById('view-table-btn').classList.toggle('active', view === 'table');
    document.getElementById('view-timeline-btn').classList.toggle('active', view === 'timeline');
    document.getElementById('view-dag-btn').classList.toggle('active', view === 'dag');
    document.getElementById('view-heatmap-btn').classList.toggle('active', view === 'heatmap');

    document.querySelector('.layout').style.display = view === 'table' ? 'grid' : 'none';
    document.getElementById('timeline-view').style.display = view === 'timeline' ? 'flex' : 'none';
    document.getElementById('dag-view').style.display = view === 'dag' ? 'flex' : 'none';
    document.getElementById('heatmap-view').style.display = view === 'heatmap' ? 'flex' : 'none';

    if (view === 'timeline') {
        loadTimelineData();
    } else if (view === 'dag') {
        loadDagData();
    } else if (view === 'heatmap') {
        loadHeatmapData();
    }
}

async function loadTimelineData() {
    const days = document.getElementById('timeline-days').value;
    const container = document.getElementById('timeline-container');
    container.innerHTML = '<div class="timeline-loading">Loading timeline...</div>';

    try {
        const res = await fetch(`/api/job_history?days=${days}`);
        if (!res.ok) throw new Error('Failed to load');
        const data = await res.json();
        timelineData = data.jobs;
        filterTimeline();
    } catch (err) {
        console.error('Timeline load error:', err);
        container.innerHTML = '<div class="timeline-empty"><div class="timeline-empty-icon">⚠️</div><div class="timeline-empty-text">Failed to load job history</div></div>';
    }
}

function filterTimeline() {
    const stateFilter = document.getElementById('timeline-state-filter').value;
    const nameFilter = document.getElementById('timeline-name-filter').value.toLowerCase();
    const groupArrays = document.getElementById('timeline-group-arrays')?.checked || false;

    filteredTimelineData = timelineData.filter(job => {
        if (stateFilter !== 'all' && job.state_category !== stateFilter) {
            return false;
        }
        if (nameFilter && !job.name.toLowerCase().includes(nameFilter)) {
            return false;
        }
        return true;
    });

    // Group job arrays if enabled
    if (groupArrays) {
        groupTimelineJobs();
    } else {
        timelineGrouped = {};
    }

    renderTimeline();
}

function groupTimelineJobs() {
    timelineGrouped = {};
    const processedIds = new Set();

    for (const job of filteredTimelineData) {
        // Check if job is part of an array (format: 12345_0, 12345_1, etc.)
        const match = job.id.match(/^(\d+)_(\d+)$/);
        if (match) {
            const baseId = match[0].split('_')[0];
            if (!timelineGrouped[baseId]) {
                timelineGrouped[baseId] = {
                    baseId,
                    name: job.name.replace(/_\d+$/, ''),
                    jobs: [],
                    expanded: false,
                };
            }
            timelineGrouped[baseId].jobs.push(job);
            processedIds.add(job.id);
        }
    }

    // Calculate aggregate stats for groups
    for (const group of Object.values(timelineGrouped)) {
        const starts = group.jobs.map(j => new Date(j.start).getTime());
        const ends = group.jobs.map(j => j.end ? new Date(j.end).getTime() : Date.now());
        group.start = new Date(Math.min(...starts));
        group.end = new Date(Math.max(...ends));
        group.completed = group.jobs.filter(j => j.state_category === 'completed').length;
        group.total = group.jobs.length;
        group.state_category = group.completed === group.total ? 'completed' :
            group.jobs.some(j => j.state_category === 'running') ? 'running' :
            group.jobs.some(j => j.state_category === 'failed') ? 'failed' : 'pending';
    }
}

function toggleTimelineGroup(baseId) {
    if (timelineGrouped[baseId]) {
        timelineGrouped[baseId].expanded = !timelineGrouped[baseId].expanded;
        renderTimeline();
    }
}

function setTimelineZoom(zoom) {
    timelineZoom = zoom;
    timelineZoomFactor = 1;
    timelinePanOffset = 0;
    document.querySelectorAll('.zoom-btn').forEach(btn => {
        btn.classList.toggle('active', btn.textContent.toLowerCase() === zoom);
    });
    updateZoomDisplay();
    renderTimeline();
}

function timelineZoomIn() {
    timelineZoomFactor = Math.min(timelineZoomFactor * 1.5, 10);
    updateZoomDisplay();
    renderTimeline();
    updateMinimap();
}

function timelineZoomOut() {
    timelineZoomFactor = Math.max(timelineZoomFactor / 1.5, 0.5);
    // Clamp pan offset when zooming out
    const maxPan = Math.max(0, 100 - (100 / timelineZoomFactor));
    timelinePanOffset = Math.min(timelinePanOffset, maxPan);
    updateZoomDisplay();
    renderTimeline();
    updateMinimap();
}

function timelineReset() {
    timelineZoomFactor = 1;
    timelinePanOffset = 0;
    updateZoomDisplay();
    renderTimeline();
    updateMinimap();
}

function updateZoomDisplay() {
    const zoomLabel = document.getElementById('timeline-zoom-level');
    if (zoomLabel) {
        zoomLabel.textContent = Math.round(timelineZoomFactor * 100) + '%';
    }
}

function renderTimeline() {
    const container = document.getElementById('timeline-container');

    if (filteredTimelineData.length === 0) {
        container.innerHTML = '<div class="timeline-empty"><div class="timeline-empty-icon">📊</div><div class="timeline-empty-text">No jobs found for the selected filters</div></div>';
        return;
    }

    // Calculate time range
    const now = new Date();
    let baseTimeRangeMs, tickCount, tickFormat;

    switch (timelineZoom) {
        case 'hour':
            baseTimeRangeMs = 24 * 60 * 60 * 1000; // 24 hours
            tickCount = 24;
            tickFormat = (date) => date.getHours() + ':00';
            break;
        case 'week':
            baseTimeRangeMs = 7 * 24 * 60 * 60 * 1000; // 7 days
            tickCount = 7;
            tickFormat = (date) => date.toLocaleDateString([], { weekday: 'short', month: 'short', day: 'numeric' });
            break;
        case 'day':
        default:
            const days = parseInt(document.getElementById('timeline-days').value) || 7;
            baseTimeRangeMs = days * 24 * 60 * 60 * 1000;
            tickCount = Math.min(days, 14);
            tickFormat = (date) => date.toLocaleDateString([], { month: 'short', day: 'numeric' });
            break;
    }

    // Apply zoom factor to visible range
    const visibleRangeMs = baseTimeRangeMs / timelineZoomFactor;
    const totalWidth = 100 * timelineZoomFactor;

    // Calculate visible window based on pan offset
    const baseStart = now.getTime() - baseTimeRangeMs;
    const viewportStartOffset = (timelinePanOffset / 100) * baseTimeRangeMs;

    const startTime = new Date(baseStart + viewportStartOffset);
    const endTime = new Date(startTime.getTime() + visibleRangeMs);

    // Generate tick labels with zoom-adjusted count
    const adjustedTickCount = Math.max(4, Math.round(tickCount / timelineZoomFactor));
    const ticks = [];
    const tickInterval = visibleRangeMs / adjustedTickCount;
    for (let i = 0; i < adjustedTickCount; i++) {
        const tickTime = new Date(startTime.getTime() + (i + 0.5) * tickInterval);
        ticks.push({
            label: tickFormat(tickTime),
            time: tickTime,
            isNow: false
        });
    }

    // Build HTML
    let html = `
        <div class="timeline-chart zoomable" style="--timeline-ticks: ${adjustedTickCount};" data-zoom="${timelineZoomFactor}">
            <div class="timeline-header-row">
                <div class="timeline-name-col">Job Name</div>
                <div class="timeline-grid-header">
                    ${ticks.map(t => `<div class="timeline-tick">${t.label}</div>`).join('')}
                </div>
            </div>
    `;

    // Get grouping state
    const groupArrays = document.getElementById('timeline-group-arrays')?.checked || false;
    const processedGroupIds = new Set();

    // Render each job (with optional grouping)
    for (const job of filteredTimelineData) {
        // Check if this job is part of a group
        if (groupArrays) {
            const match = job.id.match(/^(\d+)_\d+$/);
            if (match) {
                const baseId = match[1];
                if (processedGroupIds.has(baseId)) continue; // Skip already rendered group jobs
                processedGroupIds.add(baseId);

                const group = timelineGrouped[baseId];
                if (group) {
                    html += renderTimelineGroup(group, startTime, endTime, visibleRangeMs);
                    continue;
                }
            }
        }

        // Regular job rendering
        html += renderTimelineJob(job, startTime, endTime, visibleRangeMs, '');
    }

    // Add now marker
    const nowPercent = ((now.getTime() - startTime.getTime()) / visibleRangeMs) * 100;
    if (nowPercent >= 0 && nowPercent <= 100) {
        html += `<div class="timeline-now-marker" style="left: calc(180px + ${nowPercent}% * (100% - 180px) / 100);"></div>`;
    }

    html += '</div>';
    container.innerHTML = html;

    // Add mouse wheel zoom handler
    const chart = container.querySelector('.timeline-chart');
    if (chart) {
        chart.addEventListener('wheel', handleTimelineWheel, { passive: false });
        chart.addEventListener('mousedown', handleTimelinePanStart);
    }

    // Update minimap
    updateMinimap();
}

function renderTimelineJob(job, startTime, endTime, visibleRangeMs, indent) {
    const now = new Date();
    const jobStart = new Date(job.start);
    const jobEnd = job.end ? new Date(job.end) : now;

    // Skip jobs outside visible range
    if (jobEnd < startTime || jobStart > endTime) return '';

    // Calculate bar position and width
    const clampedStart = Math.max(jobStart.getTime(), startTime.getTime());
    const clampedEnd = Math.min(jobEnd.getTime(), endTime.getTime());

    const leftPercent = ((clampedStart - startTime.getTime()) / visibleRangeMs) * 100;
    const widthPercent = ((clampedEnd - clampedStart) / visibleRangeMs) * 100;

    const jobJson = JSON.stringify(job).replace(/"/g, '&quot;');

    return `
        <div class="timeline-row ${indent ? 'timeline-row-child' : ''}">
            <div class="timeline-job-name" onclick="openLog('${job.log_key}', 'stdout')" title="${job.name}">
                ${indent}${job.name}
                <span class="job-id">#${job.id}</span>
            </div>
            <div class="timeline-grid">
                <div class="timeline-bar ${job.state_category}"
                     style="left: ${leftPercent}%; width: ${Math.max(widthPercent, 0.5)}%;"
                     onclick="openLog('${job.log_key}', 'stdout')"
                     onmouseenter="showTimelineTooltip(event, ${jobJson})"
                     onmouseleave="hideTimelineTooltip()"
                     title="${job.name} - ${job.state} - ${job.elapsed}">
                    ${widthPercent > 5 ? job.elapsed : ''}
                </div>
            </div>
        </div>
    `;
}

function renderTimelineGroup(group, startTime, endTime, visibleRangeMs) {
    const now = new Date();
    const groupStart = group.start;
    const groupEnd = group.end;

    // Skip groups outside visible range
    if (groupEnd < startTime || groupStart > endTime) return '';

    // Calculate bar position and width
    const clampedStart = Math.max(groupStart.getTime(), startTime.getTime());
    const clampedEnd = Math.min(groupEnd.getTime(), endTime.getTime());

    const leftPercent = ((clampedStart - startTime.getTime()) / visibleRangeMs) * 100;
    const widthPercent = ((clampedEnd - clampedStart) / visibleRangeMs) * 100;

    const expandIcon = group.expanded ? '▼' : '▶';
    const groupInfo = `${group.completed}/${group.total} completed`;

    let html = `
        <div class="timeline-row timeline-row-group">
            <div class="timeline-job-name timeline-group-name" onclick="toggleTimelineGroup('${group.baseId}')" title="${group.name}">
                <span class="timeline-group-toggle">${expandIcon}</span>
                ${group.name}
                <span class="timeline-group-count">[${group.total}]</span>
            </div>
            <div class="timeline-grid">
                <div class="timeline-bar timeline-bar-group ${group.state_category}"
                     style="left: ${leftPercent}%; width: ${Math.max(widthPercent, 0.5)}%;"
                     title="${group.name} - ${groupInfo}">
                    ${widthPercent > 8 ? groupInfo : ''}
                </div>
            </div>
        </div>
    `;

    // Render child jobs if expanded
    if (group.expanded) {
        for (const job of group.jobs) {
            html += renderTimelineJob(job, startTime, endTime, visibleRangeMs, '└ ');
        }
    }

    return html;
}

function handleTimelineWheel(e) {
    // Only zoom if Ctrl/Cmd key is held, otherwise allow normal scrolling
    if (!e.ctrlKey && !e.metaKey) {
        return; // Let the browser handle normal scrolling
    }

    e.preventDefault();
    const delta = e.deltaY > 0 ? -1 : 1;

    if (delta > 0) {
        timelineZoomIn();
    } else {
        timelineZoomOut();
    }
}

function handleTimelinePanStart(e) {
    if (e.button !== 0) return; // Only left mouse button

    const target = e.target;
    // Don't start pan if clicking on a bar or name
    if (target.closest('.timeline-bar') || target.closest('.timeline-job-name')) return;

    timelineIsPanning = true;
    timelinePanStart = e.clientX;
    document.addEventListener('mousemove', handleTimelinePanMove);
    document.addEventListener('mouseup', handleTimelinePanEnd);
    e.target.style.cursor = 'grabbing';
}

function handleTimelinePanMove(e) {
    if (!timelineIsPanning) return;

    const container = document.getElementById('timeline-container');
    const gridWidth = container.offsetWidth - 180; // Subtract name column width
    const deltaX = e.clientX - timelinePanStart;
    const deltaPct = (deltaX / gridWidth) * 100 * timelineZoomFactor;

    // Pan in opposite direction of drag
    const newOffset = timelinePanOffset - deltaPct;
    const maxPan = Math.max(0, 100 - (100 / timelineZoomFactor));
    timelinePanOffset = Math.max(0, Math.min(newOffset, maxPan));

    timelinePanStart = e.clientX;
    renderTimeline();
}

function handleTimelinePanEnd() {
    timelineIsPanning = false;
    document.removeEventListener('mousemove', handleTimelinePanMove);
    document.removeEventListener('mouseup', handleTimelinePanEnd);
    const container = document.getElementById('timeline-container');
    if (container) container.style.cursor = '';
}

function updateMinimap() {
    const minimap = document.getElementById('timeline-minimap');
    const viewport = document.getElementById('minimap-viewport');
    if (!minimap || !viewport) return;

    // Calculate viewport size and position
    const viewportWidth = 100 / timelineZoomFactor;
    const viewportLeft = timelinePanOffset;

    viewport.style.width = viewportWidth + '%';
    viewport.style.left = viewportLeft + '%';

    // Show/hide minimap based on zoom level
    minimap.style.display = timelineZoomFactor > 1 ? 'block' : 'none';
}

function showTimelineTooltip(event, job) {
    hideTimelineTooltip();

    const tooltip = document.createElement('div');
    tooltip.className = 'timeline-tooltip';
    tooltip.id = 'timeline-tooltip';
    tooltip.innerHTML = `
        <div class="timeline-tooltip-header">${job.name}</div>
        <div class="timeline-tooltip-row">
            <span class="timeline-tooltip-label">Job ID:</span>
            <span class="timeline-tooltip-value">${job.id}</span>
        </div>
        <div class="timeline-tooltip-row">
            <span class="timeline-tooltip-label">State:</span>
            <span class="timeline-tooltip-value">${job.state}</span>
        </div>
        <div class="timeline-tooltip-row">
            <span class="timeline-tooltip-label">Start:</span>
            <span class="timeline-tooltip-value">${job.start}</span>
        </div>
        <div class="timeline-tooltip-row">
            <span class="timeline-tooltip-label">End:</span>
            <span class="timeline-tooltip-value">${job.end || 'Running'}</span>
        </div>
        <div class="timeline-tooltip-row">
            <span class="timeline-tooltip-label">Duration:</span>
            <span class="timeline-tooltip-value">${job.elapsed}</span>
        </div>
        ${job.partition ? `
        <div class="timeline-tooltip-row">
            <span class="timeline-tooltip-label">Partition:</span>
            <span class="timeline-tooltip-value">${job.partition}</span>
        </div>
        ` : ''}
    `;

    // Position tooltip
    tooltip.style.left = (event.clientX + 10) + 'px';
    tooltip.style.top = (event.clientY + 10) + 'px';

    document.body.appendChild(tooltip);

    // Adjust if off-screen
    const rect = tooltip.getBoundingClientRect();
    if (rect.right > window.innerWidth) {
        tooltip.style.left = (event.clientX - rect.width - 10) + 'px';
    }
    if (rect.bottom > window.innerHeight) {
        tooltip.style.top = (event.clientY - rect.height - 10) + 'px';
    }
}

function hideTimelineTooltip() {
    const tooltip = document.getElementById('timeline-tooltip');
    if (tooltip) tooltip.remove();
}

// Periodically update resource data for expanded running jobs
function updateExpandedJobResources() {
    for (const jobId of expandedJobs) {
        const job = allRunningJobs.find(j => j.id === jobId);
        if (job && job.state && job.state.toLowerCase().includes('running')) {
            loadResourceData(jobId);
        }
    }
}

// DAG View Functions
async function loadDagData() {
    const pipelineList = document.getElementById('dag-pipeline-list');
    const statusEl = document.getElementById('dag-status');

    pipelineList.innerHTML = '<div class="dag-loading">Loading pipelines...</div>';
    statusEl.textContent = '';

    try {
        const res = await fetch('/api/job_dependencies');
        if (!res.ok) throw new Error('Failed to load');
        dagData = await res.json();

        renderPipelineList();
        statusEl.textContent = `${dagData.nodes.length} jobs, ${dagData.pipelines.length} pipelines`;
    } catch (err) {
        console.error('DAG load error:', err);
        pipelineList.innerHTML = '<div class="dag-error">Failed to load dependency data</div>';
    }
}

function renderPipelineList() {
    const pipelineList = document.getElementById('dag-pipeline-list');

    if (dagData.pipelines.length === 0) {
        pipelineList.innerHTML = `
            <div class="dag-no-pipelines">
                <p>No job pipelines found</p>
                <p class="dag-hint">Pipelines are created when jobs have dependencies like <code>--dependency=afterok:123</code></p>
            </div>
        `;
        return;
    }

    pipelineList.innerHTML = dagData.pipelines.map((pipeline, idx) => `
        <div class="dag-pipeline-item ${selectedPipeline === idx ? 'selected' : ''}" onclick="selectPipeline(${idx})">
            <div class="dag-pipeline-name">${pipeline.name}</div>
            <div class="dag-pipeline-meta">
                <span class="dag-pipeline-jobs">${pipeline.total} jobs</span>
                <span class="dag-pipeline-progress">
                    <span class="dag-progress-bar">
                        <span class="dag-progress-fill" style="width: ${pipeline.progress}%"></span>
                    </span>
                    <span class="dag-progress-text">${pipeline.progress}%</span>
                </span>
            </div>
        </div>
    `).join('');
}

function selectPipeline(idx) {
    selectedPipeline = idx;
    renderPipelineList();
    renderDagGraph(idx);
}

function renderDagGraph(pipelineIdx) {
    const mainArea = document.getElementById('dag-main');
    const pipeline = dagData.pipelines[pipelineIdx];

    if (!pipeline) {
        mainArea.innerHTML = '<div class="dag-empty"><div class="dag-empty-text">Pipeline not found</div></div>';
        return;
    }

    // Get nodes and edges for this pipeline
    const pipelineJobIds = new Set(pipeline.job_ids);
    const nodes = dagData.nodes.filter(n => pipelineJobIds.has(n.id));
    const edges = dagData.edges.filter(e => pipelineJobIds.has(e.from) && pipelineJobIds.has(e.to));

    // Build a simple DAG visualization using CSS/HTML
    // We'll use a topological sort to arrange nodes in layers

    const layers = topologicalLayers(nodes, edges);

    let html = `
        <div class="dag-graph">
            <div class="dag-graph-header">
                <h3>${pipeline.name}</h3>
                <span class="dag-graph-progress">${pipeline.completed}/${pipeline.total} completed (${pipeline.progress}%)</span>
            </div>
            <div class="dag-graph-container">
    `;

    // Create node lookup
    const nodeMap = {};
    nodes.forEach(n => nodeMap[n.id] = n);

    // Render each layer
    layers.forEach((layer, layerIdx) => {
        html += `<div class="dag-layer" data-layer="${layerIdx}">`;
        layer.forEach(jobId => {
            const node = nodeMap[jobId];
            if (!node) return;

            const stateClass = node.state_category || 'pending';
            html += `
                <div class="dag-node ${stateClass}" data-job-id="${node.id}" onclick="showDagNodeDetails('${node.id}')" title="${node.name} (${node.state})">
                    <div class="dag-node-name">${node.name}</div>
                    <div class="dag-node-id">#${node.id}</div>
                    <div class="dag-node-state">${node.state}</div>
                </div>
            `;
        });
        html += '</div>';

        // Add connector arrows between layers
        if (layerIdx < layers.length - 1) {
            html += '<div class="dag-layer-connector"><span class="dag-arrow">→</span></div>';
        }
    });

    html += `
            </div>
        </div>
    `;

    // Add edge info section
    if (edges.length > 0) {
        html += `
            <div class="dag-edges-info">
                <h4>Dependencies</h4>
                <ul class="dag-edge-list">
                    ${edges.map(e => {
                        const fromNode = nodeMap[e.from];
                        const toNode = nodeMap[e.to];
                        return `<li><span class="dag-edge-from">${fromNode?.name || e.from}</span> → <span class="dag-edge-to">${toNode?.name || e.to}</span> <span class="dag-edge-type">(${e.type})</span></li>`;
                    }).join('')}
                </ul>
            </div>
        `;
    }

    mainArea.innerHTML = html;
}

function topologicalLayers(nodes, edges) {
    // Build adjacency list and in-degree map
    const inDegree = {};
    const adj = {};
    const nodeIds = new Set(nodes.map(n => n.id));

    nodeIds.forEach(id => {
        inDegree[id] = 0;
        adj[id] = [];
    });

    edges.forEach(e => {
        if (nodeIds.has(e.from) && nodeIds.has(e.to)) {
            adj[e.from].push(e.to);
            inDegree[e.to]++;
        }
    });

    // Kahn's algorithm with layer tracking
    const layers = [];
    let currentLayer = [];

    // Find all nodes with no incoming edges
    nodeIds.forEach(id => {
        if (inDegree[id] === 0) {
            currentLayer.push(id);
        }
    });

    while (currentLayer.length > 0) {
        layers.push([...currentLayer]);
        const nextLayer = [];

        currentLayer.forEach(nodeId => {
            adj[nodeId].forEach(neighbor => {
                inDegree[neighbor]--;
                if (inDegree[neighbor] === 0) {
                    nextLayer.push(neighbor);
                }
            });
        });

        currentLayer = nextLayer;
    }

    // Add any remaining nodes (cycles or disconnected)
    const placed = new Set(layers.flat());
    const remaining = [...nodeIds].filter(id => !placed.has(id));
    if (remaining.length > 0) {
        layers.push(remaining);
    }

    return layers;
}

function showDagNodeDetails(jobId) {
    const node = dagData.nodes.find(n => n.id === jobId);
    if (!node) return;

    // Remove existing tooltip
    const existing = document.getElementById('dag-tooltip');
    if (existing) existing.remove();

    const tooltip = document.createElement('div');
    tooltip.className = 'dag-tooltip';
    tooltip.id = 'dag-tooltip';
    tooltip.innerHTML = `
        <div class="dag-tooltip-header">
            <span class="dag-tooltip-name">${node.name}</span>
            <button class="dag-tooltip-close" onclick="hideDagTooltip()">&times;</button>
        </div>
        <div class="dag-tooltip-body">
            <div class="dag-tooltip-row">
                <span class="dag-tooltip-label">Job ID:</span>
                <span class="dag-tooltip-value">${node.id}</span>
            </div>
            <div class="dag-tooltip-row">
                <span class="dag-tooltip-label">State:</span>
                <span class="dag-tooltip-value dag-state-${node.state_category}">${node.state}</span>
            </div>
            <div class="dag-tooltip-row">
                <span class="dag-tooltip-label">Partition:</span>
                <span class="dag-tooltip-value">${node.partition || 'N/A'}</span>
            </div>
            ${node.start_time ? `
            <div class="dag-tooltip-row">
                <span class="dag-tooltip-label">Start:</span>
                <span class="dag-tooltip-value">${node.start_time}</span>
            </div>
            ` : ''}
            ${node.end_time ? `
            <div class="dag-tooltip-row">
                <span class="dag-tooltip-label">End:</span>
                <span class="dag-tooltip-value">${node.end_time}</span>
            </div>
            ` : ''}
        </div>
        <div class="dag-tooltip-actions">
            <button onclick="openLog('${node.name}::${node.id}', 'stdout'); hideDagTooltip();">View Logs</button>
        </div>
    `;

    document.body.appendChild(tooltip);

    // Position near center of screen
    tooltip.style.left = '50%';
    tooltip.style.top = '50%';
    tooltip.style.transform = 'translate(-50%, -50%)';
}

function hideDagTooltip() {
    const tooltip = document.getElementById('dag-tooltip');
    if (tooltip) tooltip.remove();
}

// Insights Panel Functions
let costData = null;

async function loadInsights() {
    const content = document.getElementById('insights-content');
    if (!content) return;

    try {
        // Load both insights and cost data in parallel
        const [insightsRes, costRes] = await Promise.all([
            fetch('/api/insights?days=30'),
            fetch('/api/cost?days=30')
        ]);

        if (insightsRes.ok) {
            insightsData = await insightsRes.json();
        }
        if (costRes.ok) {
            costData = await costRes.json();
        }

        renderInsights();
    } catch (err) {
        console.error('Insights load error:', err);
        content.innerHTML = '<div class="insights-error">Could not load insights</div>';
    }
}

function renderInsights() {
    const content = document.getElementById('insights-content');
    const panel = document.getElementById('insights-panel');

    if (!content || !insightsData) return;

    // Handle collapsed/minimized state
    if (insightsCollapsed || panel.classList.contains('widget-minimized')) {
        return;
    }

    // Check if we have any data
    if (!insightsData.job_stats || insightsData.job_stats.total_jobs === 0) {
        content.innerHTML = '<div class="insights-empty">No job history found for analysis</div>';
        return;
    }

    let html = '<div class="insights-grid">';

    // Efficiency Score Card
    if (insightsData.efficiency_score) {
        const eff = insightsData.efficiency_score;
        const gradeClass = eff.grade === 'A' ? 'grade-a' : eff.grade === 'B' ? 'grade-b' : eff.grade === 'C' ? 'grade-c' : 'grade-d';

        // Build explanation based on efficiency metrics
        let explanation = '';
        const memEff = eff.memory_efficiency || 0;
        const timeEff = eff.time_efficiency || 0;

        if (eff.grade === 'A') {
            explanation = 'Great resource utilization';
        } else if (eff.grade === 'B') {
            explanation = 'Good efficiency, minor optimization possible';
        } else if (eff.grade === 'C') {
            if (memEff < 50) explanation = 'Consider requesting less memory';
            else if (timeEff < 50) explanation = 'Jobs finishing much faster than time limit';
            else explanation = 'Some resource optimization recommended';
        } else {
            if (memEff < 30) explanation = 'Jobs using <30% of requested memory';
            else if (timeEff < 30) explanation = 'Jobs using <30% of time limit';
            else explanation = 'Significant resource overallocation detected';
        }

        html += `
            <div class="insight-card efficiency-card">
                <div class="insight-icon">📊</div>
                <div class="insight-content">
                    <div class="insight-title">Efficiency Score</div>
                    <div class="efficiency-score ${gradeClass}">
                        <span class="efficiency-grade">${eff.grade}</span>
                        <span class="efficiency-label">${eff.label}</span>
                    </div>
                    <div class="efficiency-explanation">${explanation}</div>
                    <div class="efficiency-details">
                        ${eff.memory_efficiency ? `<span title="Actual memory used vs requested">Memory: ${eff.memory_efficiency}%</span>` : ''}
                        ${eff.time_efficiency ? `<span title="Actual runtime vs time limit">Time: ${eff.time_efficiency}%</span>` : ''}
                    </div>
                </div>
            </div>
        `;
    }

    // Job Stats Card
    if (insightsData.job_stats) {
        const stats = insightsData.job_stats;
        html += `
            <div class="insight-card stats-card">
                <div class="insight-icon">📈</div>
                <div class="insight-content">
                    <div class="insight-title">Last 30 Days</div>
                    <div class="stats-grid">
                        <div class="stat-item">
                            <span class="stat-value">${stats.total_jobs}</span>
                            <span class="stat-label">Total Jobs</span>
                        </div>
                        <div class="stat-item success">
                            <span class="stat-value">${stats.success_rate}%</span>
                            <span class="stat-label">Success Rate</span>
                        </div>
                        <div class="stat-item ${stats.failed > 0 ? 'warning' : ''}">
                            <span class="stat-value">${stats.failed}</span>
                            <span class="stat-label">Failed</span>
                        </div>
                        <div class="stat-item ${stats.timeout > 0 ? 'warning' : ''}">
                            <span class="stat-value">${stats.timeout}</span>
                            <span class="stat-label">Timeout</span>
                        </div>
                    </div>
                </div>
            </div>
        `;
    }

    // Cost/Usage Card
    if (costData && costData.total_sus !== undefined) {
        const dailyAvg = costData.daily_avg || 0;
        const projected = costData.projected_total || costData.total_sus;
        html += `
            <div class="insight-card cost-card">
                <div class="insight-icon">💰</div>
                <div class="insight-content">
                    <div class="insight-title">Cost & Usage (30 days)</div>
                    <div class="cost-grid">
                        <div class="cost-item">
                            <span class="cost-value">${formatSUs(costData.total_sus)}</span>
                            <span class="cost-label">Total SUs</span>
                        </div>
                        <div class="cost-item">
                            <span class="cost-value">${formatSUs(dailyAvg)}</span>
                            <span class="cost-label">Daily Avg</span>
                        </div>
                    </div>
                    ${costData.by_partition && Object.keys(costData.by_partition).length > 0 ? `
                        <div class="cost-breakdown">
                            ${Object.entries(costData.by_partition).slice(0, 3).map(([part, sus]) =>
                                `<span class="cost-partition"><span class="partition-name">${part}</span>: ${formatSUs(sus)}</span>`
                            ).join('')}
                        </div>
                    ` : ''}
                </div>
            </div>
        `;
    }

    // Memory Recommendation Card
    if (insightsData.memory_insights && insightsData.memory_insights.recommendation) {
        const mem = insightsData.memory_insights;
        html += `
            <div class="insight-card recommendation-card">
                <div class="insight-icon">💾</div>
                <div class="insight-content">
                    <div class="insight-title">Memory Recommendation</div>
                    <div class="recommendation-text">${mem.recommendation}</div>
                    <div class="recommendation-meta">Based on ${mem.sample_count} completed jobs</div>
                </div>
            </div>
        `;
    }

    // Time Recommendation Card
    if (insightsData.time_insights && insightsData.time_insights.recommendation) {
        const time = insightsData.time_insights;
        html += `
            <div class="insight-card recommendation-card">
                <div class="insight-icon">⏱️</div>
                <div class="insight-content">
                    <div class="insight-title">Time Limit Recommendation</div>
                    <div class="recommendation-text">${time.recommendation}</div>
                    <div class="recommendation-meta">Based on ${time.sample_count} completed jobs</div>
                </div>
            </div>
        `;
    }

    // Failure Patterns
    if (insightsData.failure_patterns && insightsData.failure_patterns.length > 0) {
        for (const pattern of insightsData.failure_patterns.slice(0, 2)) {
            html += `
                <div class="insight-card warning-card">
                    <div class="insight-icon">⚠️</div>
                    <div class="insight-content">
                        <div class="insight-title">Failure Pattern Detected</div>
                        <div class="warning-text">${pattern.message}</div>
                    </div>
                </div>
            `;
        }
    }

    html += '</div>';

    // If no insights to show
    if (html === '<div class="insights-grid"></div>') {
        html = '<div class="insights-empty">No recommendations at this time. Keep submitting jobs to get personalized insights!</div>';
    }

    content.innerHTML = html;
}

function toggleInsightsPanel() {
    insightsCollapsed = !insightsCollapsed;
    localStorage.setItem('insightsCollapsed', insightsCollapsed);
    renderInsights();
}

// Load prediction for a running job
async function loadJobPrediction(jobId) {
    try {
        const res = await fetch(`/api/predict/${jobId}`);
        if (!res.ok) return null;
        return await res.json();
    } catch (err) {
        console.error('Prediction load error:', err);
        return null;
    }
}

// Heatmap Functions
async function loadHeatmapData() {
    const days = document.getElementById('heatmap-days').value;

    // Show loading state in all containers
    const containers = ['heatmap-activity-container', 'heatmap-success-container', 'heatmap-pattern-container'];
    containers.forEach(id => {
        const el = document.getElementById(id);
        if (el) el.innerHTML = '<div class="heatmap-loading">Loading...</div>';
    });

    try {
        const res = await fetch(`/api/heatmap?days=${days}`);
        if (!res.ok) throw new Error('Failed to load');
        heatmapData = await res.json();
        renderAllHeatmaps();
    } catch (err) {
        console.error('Heatmap load error:', err);
        containers.forEach(id => {
            const el = document.getElementById(id);
            if (el) el.innerHTML = '<div class="heatmap-empty">Failed to load data</div>';
        });
    }
}

function renderAllHeatmaps() {
    if (!heatmapData || heatmapData.error) {
        return;
    }

    // Update stats
    const daily = heatmapData.daily || [];
    const activeDays = daily.filter(d => d.total > 0).length;
    const totalCompleted = daily.reduce((sum, d) => sum + (d.completed || 0), 0);
    const totalJobs = heatmapData.total_jobs || 0;
    const successRate = totalJobs > 0 ? Math.round((totalCompleted / totalJobs) * 100) : 0;

    document.getElementById('stat-total-jobs').textContent = totalJobs.toLocaleString();
    document.getElementById('stat-active-days').textContent = activeDays;
    document.getElementById('stat-avg-daily').textContent = activeDays > 0 ? Math.round(totalJobs / activeDays) : 0;
    document.getElementById('stat-success-rate').textContent = successRate + '%';

    // Render all three views
    renderActivityCalendar(document.getElementById('heatmap-activity-container'), false);
    renderActivityCalendar(document.getElementById('heatmap-success-container'), true);
    renderHourlyPatternGrid(document.getElementById('heatmap-pattern-container'));
}

// Legacy function for compatibility
function renderHeatmap() {
    renderAllHeatmaps();
}

function renderActivityCalendar(container, isSuccessView) {
    if (!container) return;
    const daily = heatmapData.daily || [];

    if (daily.length === 0) {
        container.innerHTML = '<div class="heatmap-empty">No activity data available</div>';
        return;
    }

    // Group by weeks (GitHub-style calendar)
    const weeks = [];
    let currentWeek = [];

    for (const day of daily) {
        const date = new Date(day.date);
        const dayOfWeek = date.getDay(); // 0 = Sunday

        if (dayOfWeek === 0 && currentWeek.length > 0) {
            weeks.push(currentWeek);
            currentWeek = [];
        }
        currentWeek.push(day);
    }
    if (currentWeek.length > 0) {
        weeks.push(currentWeek);
    }

    // Calculate max for scaling
    const maxValue = isSuccessView ? 100 : heatmapData.max_daily;

    // Day labels
    const dayLabels = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];

    // Build HTML with new calendar classes
    let html = `
        <div class="activity-calendar">
            <div class="calendar-day-labels">
                ${dayLabels.map((d, i) => `<div class="calendar-day-label">${i % 2 === 1 ? d : ''}</div>`).join('')}
            </div>
            <div class="calendar-grid">
    `;

    // Month labels
    let lastMonth = -1;
    const months = [];
    for (let i = 0; i < weeks.length; i++) {
        if (weeks[i].length > 0) {
            const month = new Date(weeks[i][0].date).getMonth();
            if (month !== lastMonth) {
                months.push({ week: i, month: new Date(weeks[i][0].date).toLocaleDateString('en-US', { month: 'short' }) });
                lastMonth = month;
            }
        }
    }

    html += '<div class="calendar-month-labels">';
    let monthIdx = 0;
    let lastLabelEnd = 0;
    for (let i = 0; i < weeks.length; i++) {
        if (monthIdx < months.length && months[monthIdx].week === i) {
            // Calculate spacing to push labels to correct positions
            const spacing = (i - lastLabelEnd) * 16;
            html += `<div class="calendar-month-label" style="margin-left: ${spacing}px;">${months[monthIdx].month}</div>`;
            lastLabelEnd = i + 3; // Approximate label width
            monthIdx++;
        }
    }
    html += '</div>';

    html += '<div class="calendar-weeks">';

    for (const week of weeks) {
        html += '<div class="calendar-week">';

        // Pad start of week if needed
        if (week.length > 0) {
            const firstDayOfWeek = new Date(week[0].date).getDay();
            for (let i = 0; i < firstDayOfWeek; i++) {
                html += '<div class="calendar-cell empty"></div>';
            }
        }

        for (const day of week) {
            let level, value, tooltip;
            let hasFailures = false;

            if (isSuccessView) {
                value = day.success_rate || 0;
                hasFailures = day.failed > 0;
                // Higher value = more success = higher level (green)
                if (day.total === 0) {
                    level = 0;
                } else if (value >= 90) {
                    level = 4;
                } else if (value >= 70) {
                    level = 3;
                } else if (value >= 50) {
                    level = 2;
                } else if (value >= 30) {
                    level = 1;
                } else {
                    level = 0;
                }
                tooltip = `${day.date}: ${Math.round(value)}% success rate (${day.completed}/${day.total} completed)`;
            } else {
                value = day.total;
                if (value === 0) {
                    level = 0;
                } else if (value <= maxValue * 0.25) {
                    level = 1;
                } else if (value <= maxValue * 0.5) {
                    level = 2;
                } else if (value <= maxValue * 0.75) {
                    level = 3;
                } else {
                    level = 4;
                }
                tooltip = `${day.date}: ${day.total} jobs (${day.completed} completed, ${day.failed} failed, ${day.cancelled} cancelled)`;
            }

            const successClass = isSuccessView ? 'success' : '';
            const failureClass = hasFailures && isSuccessView ? 'has-failures' : '';

            html += `<div class="calendar-cell ${successClass} ${failureClass}"
                         data-level="${level}"
                         title="${tooltip}"
                         onclick="filterJobsByDate('${day.date}')"></div>`;
        }

        html += '</div>';
    }

    html += '</div></div></div>';

    // Add legend
    const legendColor = isSuccessView ? 'success' : '';
    html += `
        <div class="heatmap-panel-legend">
            <span class="legend-label">Less</span>
            <span class="legend-cell ${legendColor}" data-level="0"></span>
            <span class="legend-cell ${legendColor}" data-level="1"></span>
            <span class="legend-cell ${legendColor}" data-level="2"></span>
            <span class="legend-cell ${legendColor}" data-level="3"></span>
            <span class="legend-cell ${legendColor}" data-level="4"></span>
            <span class="legend-label">More</span>
        </div>
    `;

    container.innerHTML = html;
}

function renderHourlyPatternGrid(container) {
    if (!container) return;
    const hourly = heatmapData.hourly || [];

    if (hourly.length === 0) {
        container.innerHTML = '<div class="heatmap-empty">No pattern data available</div>';
        return;
    }

    const maxCount = heatmapData.max_hourly || 1;
    const dayLabels = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];

    // Hour labels at top
    let html = `<div class="weekly-pattern-grid">
        <div class="pattern-hour-labels">`;

    for (let h = 0; h < 24; h++) {
        if (h % 3 === 0) {
            const label = h === 0 ? '12a' : h < 12 ? `${h}a` : h === 12 ? '12p' : `${h-12}p`;
            html += `<span class="pattern-hour-label">${label}</span>`;
        } else {
            html += `<span class="pattern-hour-label"></span>`;
        }
    }
    html += `</div><div class="pattern-rows">`;

    for (let dow = 0; dow < 7; dow++) {
        html += `<div class="pattern-row">
            <div class="pattern-day-name">${dayLabels[dow]}</div>
            <div class="pattern-cells">`;

        for (let hour = 0; hour < 24; hour++) {
            const data = hourly.find(h => h.day === dow && h.hour === hour) || { count: 0 };
            const count = data.count;

            let level;
            if (count === 0) {
                level = 0;
            } else if (count <= maxCount * 0.25) {
                level = 1;
            } else if (count <= maxCount * 0.5) {
                level = 2;
            } else if (count <= maxCount * 0.75) {
                level = 3;
            } else {
                level = 4;
            }

            const hourStr = hour.toString().padStart(2, '0');
            const tooltip = `${dayLabels[dow]} ${hourStr}:00: ${count} jobs`;

            html += `<div class="pattern-cell" data-level="${level}" title="${tooltip}"></div>`;
        }

        html += '</div></div>';
    }

    html += '</div></div>';

    // Add legend
    html += `
        <div class="heatmap-panel-legend">
            <span class="legend-label">Less</span>
            <span class="legend-cell pattern" data-level="0"></span>
            <span class="legend-cell pattern" data-level="1"></span>
            <span class="legend-cell pattern" data-level="2"></span>
            <span class="legend-cell pattern" data-level="3"></span>
            <span class="legend-cell pattern" data-level="4"></span>
            <span class="legend-label">More</span>
        </div>
    `;

    container.innerHTML = html;
}

function filterJobsByDate(dateStr) {
    // Switch to table view and filter by date
    setView('table');
    searchBox.value = dateStr;
    searchQuery = dateStr;
    renderRunning(filterJobs(allRunningJobs));
    renderRecent(filterJobs(allRecentJobs));
}

// Quick Submit Modal Functions
let availablePartitions = [];
let jobTemplates = [];

async function loadPartitions() {
    try {
        const res = await fetch('/api/partitions');
        if (!res.ok) return;
        const data = await res.json();
        availablePartitions = data.partitions || [];
        populatePartitionSelect();
    } catch (err) {
        console.error('Partition load error:', err);
    }
}

function populatePartitionSelect() {
    const select = document.getElementById('submit-partition');
    if (!select) return;

    select.innerHTML = '<option value="">Default</option>';
    availablePartitions.forEach(p => {
        const opt = document.createElement('option');
        opt.value = p.name;
        opt.textContent = p.name;
        if (p.default) opt.textContent += ' (default)';
        select.appendChild(opt);
    });
}

async function loadTemplates() {
    try {
        const res = await fetch('/api/templates');
        if (!res.ok) return;
        const data = await res.json();
        jobTemplates = data.templates || [];
        populateTemplateSelect();
        renderTemplateList();
    } catch (err) {
        console.error('Template load error:', err);
    }
}

function populateTemplateSelect() {
    const select = document.getElementById('submit-template-select');
    if (!select) return;

    select.innerHTML = '<option value="">-- Choose a template --</option>';
    jobTemplates.forEach(t => {
        const opt = document.createElement('option');
        opt.value = t.id;
        opt.textContent = t.name;
        select.appendChild(opt);
    });
}

function openSubmitModal() {
    document.getElementById('submit-modal').style.display = 'flex';
    loadPartitions();
    loadTemplates();

    // Clear form
    document.getElementById('submit-script-path').value = '';
    document.getElementById('submit-script-content').value = '';
    document.getElementById('submit-job-name').value = '';
    document.getElementById('submit-time').value = '';
    document.getElementById('submit-memory').value = '';
    document.getElementById('submit-cpus').value = '';
    document.getElementById('submit-gpus').value = '';
    document.getElementById('submit-workdir').value = '';
    document.getElementById('submit-dependency').value = '';

    switchSubmitTab('script');
}

function closeSubmitModal() {
    document.getElementById('submit-modal').style.display = 'none';
}

function switchSubmitTab(tab) {
    document.querySelectorAll('.submit-tab').forEach(t => {
        t.classList.toggle('active', t.dataset.tab === tab);
    });
    document.getElementById('submit-tab-script').style.display = tab === 'script' ? 'block' : 'none';
    document.getElementById('submit-tab-template').style.display = tab === 'template' ? 'block' : 'none';
}

function loadTemplate() {
    const select = document.getElementById('submit-template-select');
    const templateId = select.value;

    if (!templateId) {
        document.getElementById('template-preview').style.display = 'none';
        return;
    }

    const template = jobTemplates.find(t => t.id === templateId);
    if (!template) return;

    // Show preview
    const preview = document.getElementById('template-preview');
    const previewContent = document.getElementById('template-preview-content');
    preview.style.display = 'block';
    previewContent.textContent = template.script_content || '(No script content)';

    // Populate form fields
    if (template.job_name) document.getElementById('submit-job-name').value = template.job_name;
    if (template.partition) document.getElementById('submit-partition').value = template.partition;
    if (template.time_limit) document.getElementById('submit-time').value = template.time_limit;
    if (template.memory) document.getElementById('submit-memory').value = template.memory;
    if (template.cpus) document.getElementById('submit-cpus').value = template.cpus;
    if (template.gpus) document.getElementById('submit-gpus').value = template.gpus;
    if (template.work_dir) document.getElementById('submit-workdir').value = template.work_dir;
}

async function submitJob() {
    const scriptPath = document.getElementById('submit-script-path').value.trim();
    const scriptContent = document.getElementById('submit-script-content').value.trim();
    const activeTab = document.querySelector('.submit-tab.active').dataset.tab;

    // Get template content if using template tab
    let finalScriptContent = scriptContent;
    if (activeTab === 'template') {
        const templateId = document.getElementById('submit-template-select').value;
        const template = jobTemplates.find(t => t.id === templateId);
        if (template && template.script_content) {
            finalScriptContent = template.script_content;
        }
    }

    if (!scriptPath && !finalScriptContent) {
        alert('Please provide a script path or script content');
        return;
    }

    const jobData = {
        script_path: scriptPath || null,
        script_content: !scriptPath && finalScriptContent ? finalScriptContent : null,
        job_name: document.getElementById('submit-job-name').value.trim() || null,
        partition: document.getElementById('submit-partition').value || null,
        time_limit: document.getElementById('submit-time').value.trim() || null,
        memory: document.getElementById('submit-memory').value.trim() || null,
        cpus: document.getElementById('submit-cpus').value ? parseInt(document.getElementById('submit-cpus').value) : null,
        gpus: document.getElementById('submit-gpus').value ? parseInt(document.getElementById('submit-gpus').value) : null,
        work_dir: document.getElementById('submit-workdir').value.trim() || null,
        dependency: document.getElementById('submit-dependency').value.trim() || null,
    };

    try {
        const res = await fetch('/api/submit', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(jobData)
        });

        const data = await res.json();

        if (data.error) {
            alert('Error submitting job: ' + data.error);
            return;
        }

        alert(`Job submitted successfully!\nJob ID: ${data.job_id}`);
        closeSubmitModal();
        fetchJobs(); // Refresh job list
    } catch (err) {
        console.error('Submit error:', err);
        alert('Error submitting job');
    }
}

function saveAsTemplate() {
    document.getElementById('save-template-modal').style.display = 'flex';
    document.getElementById('template-name').value = '';
    document.getElementById('template-description').value = '';
}

function closeSaveTemplateModal() {
    document.getElementById('save-template-modal').style.display = 'none';
}

async function confirmSaveTemplate() {
    const name = document.getElementById('template-name').value.trim();
    const description = document.getElementById('template-description').value.trim();

    if (!name) {
        alert('Please enter a template name');
        return;
    }

    const templateData = {
        name,
        description,
        script_content: document.getElementById('submit-script-content').value.trim(),
        job_name: document.getElementById('submit-job-name').value.trim(),
        partition: document.getElementById('submit-partition').value,
        time_limit: document.getElementById('submit-time').value.trim(),
        memory: document.getElementById('submit-memory').value.trim(),
        cpus: document.getElementById('submit-cpus').value ? parseInt(document.getElementById('submit-cpus').value) : null,
        gpus: document.getElementById('submit-gpus').value ? parseInt(document.getElementById('submit-gpus').value) : null,
        work_dir: document.getElementById('submit-workdir').value.trim(),
    };

    try {
        const res = await fetch('/api/templates', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(templateData)
        });

        const data = await res.json();

        if (data.error) {
            alert('Error saving template: ' + data.error);
            return;
        }

        alert('Template saved successfully!');
        closeSaveTemplateModal();
        loadTemplates();
    } catch (err) {
        console.error('Template save error:', err);
        alert('Error saving template');
    }
}

function openTemplateModal() {
    document.getElementById('template-modal').style.display = 'flex';
    loadTemplates();
}

function closeTemplateModal() {
    document.getElementById('template-modal').style.display = 'none';
}

function renderTemplateList() {
    const list = document.getElementById('template-list');
    if (!list) return;

    if (jobTemplates.length === 0) {
        list.innerHTML = '<div class="template-empty">No templates saved yet</div>';
        return;
    }

    list.innerHTML = jobTemplates.map(t => `
        <div class="template-item">
            <div class="template-item-info">
                <div class="template-item-name">${t.name}</div>
                ${t.description ? `<div class="template-item-desc">${t.description}</div>` : ''}
                <div class="template-item-meta">
                    ${t.partition ? `<span>Partition: ${t.partition}</span>` : ''}
                    ${t.time_limit ? `<span>Time: ${t.time_limit}</span>` : ''}
                    ${t.memory ? `<span>Memory: ${t.memory}</span>` : ''}
                </div>
            </div>
            <div class="template-item-actions">
                <button class="btn-small" onclick="useTemplate('${t.id}')">Use</button>
                <button class="btn-small btn-danger" onclick="deleteTemplate('${t.id}')">Delete</button>
            </div>
        </div>
    `).join('');
}

function useTemplate(templateId) {
    closeTemplateModal();
    openSubmitModal();

    setTimeout(() => {
        switchSubmitTab('template');
        document.getElementById('submit-template-select').value = templateId;
        loadTemplate();
    }, 100);
}

async function deleteTemplate(templateId) {
    if (!confirm('Are you sure you want to delete this template?')) return;

    try {
        const res = await fetch(`/api/templates/${templateId}`, {
            method: 'DELETE'
        });

        const data = await res.json();

        if (data.error) {
            alert('Error deleting template: ' + data.error);
            return;
        }

        loadTemplates();
    } catch (err) {
        console.error('Template delete error:', err);
        alert('Error deleting template');
    }
}

// Keyboard shortcut for quick submit
document.addEventListener('keydown', (e) => {
    // Ctrl+N or Cmd+N to open quick submit
    if ((e.key === 'n' || e.key === 'N') && (e.ctrlKey || e.metaKey)) {
        e.preventDefault();
        openSubmitModal();
    }
});

// Close modals on escape key
document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
        if (document.getElementById('submit-modal').style.display !== 'none') {
            closeSubmitModal();
        }
        if (document.getElementById('template-modal').style.display !== 'none') {
            closeTemplateModal();
        }
        if (document.getElementById('save-template-modal').style.display !== 'none') {
            closeSaveTemplateModal();
        }
    }
});

// ==========================================================================
// Batch Operations Functions
// ==========================================================================

function toggleJobSelection(tableType, jobId, isSelected) {
    const selectedSet = tableType === 'running' ? selectedRunningJobs : selectedRecentJobs;

    if (isSelected) {
        selectedSet.add(jobId);
    } else {
        selectedSet.delete(jobId);
    }

    updateBatchActionsVisibility(tableType);
    updateSelectAllCheckbox(tableType);
}

function toggleSelectAll(tableType, isSelected) {
    const selectedSet = tableType === 'running' ? selectedRunningJobs : selectedRecentJobs;
    const jobs = tableType === 'running' ? filterJobs(allRunningJobs) : filterJobs(allRecentJobs);

    selectedSet.clear();
    if (isSelected) {
        jobs.forEach(job => selectedSet.add(job.id));
    }

    // Re-render to update checkboxes
    if (tableType === 'running') {
        renderRunning(jobs);
    } else {
        renderRecent(jobs);
    }
}

function updateSelectAllCheckbox(tableType) {
    const selectedSet = tableType === 'running' ? selectedRunningJobs : selectedRecentJobs;
    const jobs = tableType === 'running' ? filterJobs(allRunningJobs) : filterJobs(allRecentJobs);
    const selectAllCheckbox = document.getElementById(`select-all-${tableType}`);

    if (selectAllCheckbox) {
        selectAllCheckbox.checked = jobs.length > 0 && selectedSet.size === jobs.length;
        selectAllCheckbox.indeterminate = selectedSet.size > 0 && selectedSet.size < jobs.length;
    }
}

function updateBatchActionsVisibility(tableType) {
    const selectedSet = tableType === 'running' ? selectedRunningJobs : selectedRecentJobs;
    const actionsEl = document.getElementById(`${tableType}-batch-actions`);
    const countEl = document.getElementById(`${tableType}-selection-count`);

    if (actionsEl) {
        actionsEl.style.display = selectedSet.size > 0 ? 'flex' : 'none';
    }
    if (countEl) {
        countEl.textContent = `${selectedSet.size} selected`;
    }
}

function clearSelection(tableType) {
    const selectedSet = tableType === 'running' ? selectedRunningJobs : selectedRecentJobs;
    selectedSet.clear();

    const selectAllCheckbox = document.getElementById(`select-all-${tableType}`);
    if (selectAllCheckbox) selectAllCheckbox.checked = false;

    // Re-render to update checkboxes
    if (tableType === 'running') {
        renderRunning(filterJobs(allRunningJobs));
    } else {
        renderRecent(filterJobs(allRecentJobs));
    }
}

async function batchCancel(tableType) {
    const selectedSet = tableType === 'running' ? selectedRunningJobs : selectedRecentJobs;
    const jobIds = [...selectedSet];

    if (jobIds.length === 0) return;

    const jobList = jobIds.slice(0, 10).join(', ') + (jobIds.length > 10 ? '...' : '');
    if (!confirm(`Cancel ${jobIds.length} job(s)?\n\nJob IDs: ${jobList}`)) return;

    let succeeded = 0;
    let failed = 0;

    for (const jobId of jobIds) {
        try {
            const res = await fetch(`/api/cancel/${jobId}`, { method: 'POST' });
            if (res.ok) {
                succeeded++;
                selectedSet.delete(jobId);
            } else {
                failed++;
            }
        } catch (err) {
            failed++;
        }
    }

    // Refresh jobs list
    fetchJobs();

    if (failed > 0) {
        alert(`Cancelled ${succeeded} job(s). ${failed} failed.`);
    } else {
        alert(`Successfully cancelled ${succeeded} job(s).`);
    }
}

async function batchResubmit(tableType) {
    const selectedSet = tableType === 'running' ? selectedRunningJobs : selectedRecentJobs;
    const jobIds = [...selectedSet];

    if (jobIds.length === 0) return;

    const jobList = jobIds.slice(0, 10).join(', ') + (jobIds.length > 10 ? '...' : '');
    if (!confirm(`Resubmit ${jobIds.length} job(s) with original parameters?\n\nJob IDs: ${jobList}`)) return;

    let succeeded = 0;
    let failed = 0;
    let skipped = 0;

    for (const jobId of jobIds) {
        // First get submission info
        try {
            const infoRes = await fetch(`/api/job_submit_info/${jobId}`);
            if (!infoRes.ok) {
                skipped++;
                continue;
            }

            const info = await infoRes.json();
            if (!info.script_path) {
                skipped++;
                continue;
            }

            // Resubmit with original parameters
            const res = await fetch('/api/resubmit', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    script_path: info.script_path,
                    work_dir: info.work_dir,
                    partition: info.partition,
                    time_limit: info.timelimit,
                    memory: info.req_mem,
                    cpus: info.req_cpus
                })
            });

            if (res.ok) {
                succeeded++;
            } else {
                failed++;
            }
        } catch (err) {
            failed++;
        }
    }

    // Refresh jobs list
    fetchJobs();

    let msg = `Resubmitted ${succeeded} job(s).`;
    if (failed > 0) msg += ` ${failed} failed.`;
    if (skipped > 0) msg += ` ${skipped} skipped (no script info).`;
    alert(msg);
}

function batchExport(tableType) {
    const selectedSet = tableType === 'running' ? selectedRunningJobs : selectedRecentJobs;
    const allJobs = tableType === 'running' ? allRunningJobs : allRecentJobs;
    const jobIds = [...selectedSet];

    if (jobIds.length === 0) return;

    const jobs = allJobs.filter(job => jobIds.includes(job.id));

    // Build export data
    const exportData = jobs.map(job => ({
        id: job.id,
        name: job.name,
        state: job.state,
        runtime: job.runtime,
        partition: job.partition,
        updated: job.updated || job.start_time,
        log_key: job.log_key
    }));

    // Show format selection
    const format = prompt('Export format:\n1. CSV\n2. JSON\n\nEnter 1 or 2:', '1');

    if (format === '1') {
        downloadCSV(exportData, `jobs_export_${new Date().toISOString().slice(0, 10)}.csv`);
    } else if (format === '2') {
        downloadJSON(exportData, `jobs_export_${new Date().toISOString().slice(0, 10)}.json`);
    }
}

function downloadCSV(data, filename) {
    if (data.length === 0) return;

    const headers = Object.keys(data[0]);
    const csvContent = [
        headers.join(','),
        ...data.map(row => headers.map(h => {
            const val = row[h] || '';
            // Escape quotes and wrap in quotes if contains comma
            if (typeof val === 'string' && (val.includes(',') || val.includes('"'))) {
                return `"${val.replace(/"/g, '""')}"`;
            }
            return val;
        }).join(','))
    ].join('\n');

    downloadFile(csvContent, filename, 'text/csv');
}

function downloadJSON(data, filename) {
    const jsonContent = JSON.stringify(data, null, 2);
    downloadFile(jsonContent, filename, 'application/json');
}

function downloadFile(content, filename, mimeType) {
    const blob = new Blob([content], { type: mimeType });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
}

// ==========================================================================
// Advanced Filter Functions
// ==========================================================================

function toggleAdvancedFilters() {
    advancedFiltersVisible = !advancedFiltersVisible;
    const panel = document.getElementById('advanced-filters');
    const toggle = document.getElementById('filter-toggle');

    if (advancedFiltersVisible) {
        panel.style.display = 'block';
        toggle.classList.add('active');
        loadPartitionsForFilter();
    } else {
        panel.style.display = 'none';
        toggle.classList.remove('active');
    }
}

function toggleQuickFilter(filter) {
    const chip = document.querySelector(`.filter-chip[data-filter="${filter}"]`);

    if (activeQuickFilters.has(filter)) {
        activeQuickFilters.delete(filter);
        chip.classList.remove('active');
    } else {
        activeQuickFilters.add(filter);
        chip.classList.add('active');
    }

    updateActiveFiltersDisplay();
    refreshJobTables();
    updateUrlWithFilters();
}

function applyAdvancedFilters() {
    // Read values from form
    advancedFilters.state = document.getElementById('filter-state').value;
    advancedFilters.partition = document.getElementById('filter-partition').value;
    advancedFilters.name = document.getElementById('filter-name').value;
    advancedFilters.nameRegex = document.getElementById('filter-name-regex').checked;
    advancedFilters.dateFrom = document.getElementById('filter-date-from').value;
    advancedFilters.dateTo = document.getElementById('filter-date-to').value;
    advancedFilters.runtimeMin = document.getElementById('filter-runtime-min').value;
    advancedFilters.runtimeMax = document.getElementById('filter-runtime-max').value;
    advancedFilters.exitCode = document.getElementById('filter-exit-code').value;

    updateActiveFiltersDisplay();
    refreshJobTables();
    updateUrlWithFilters();
}

function clearAllFilters() {
    // Clear quick filters
    activeQuickFilters.clear();
    document.querySelectorAll('.filter-chip').forEach(chip => chip.classList.remove('active'));

    // Clear advanced filters
    advancedFilters = {
        state: '',
        partition: '',
        name: '',
        nameRegex: false,
        dateFrom: '',
        dateTo: '',
        runtimeMin: '',
        runtimeMax: '',
        exitCode: ''
    };

    // Reset form inputs
    document.getElementById('filter-state').value = '';
    document.getElementById('filter-partition').value = '';
    document.getElementById('filter-name').value = '';
    document.getElementById('filter-name-regex').checked = false;
    document.getElementById('filter-date-from').value = '';
    document.getElementById('filter-date-to').value = '';
    document.getElementById('filter-runtime-min').value = '';
    document.getElementById('filter-runtime-max').value = '';
    document.getElementById('filter-exit-code').value = '';

    // Clear search box
    searchQuery = '';
    searchBox.value = '';

    updateActiveFiltersDisplay();
    refreshJobTables();
    updateUrlWithFilters();
}

function updateActiveFiltersDisplay() {
    const container = document.getElementById('active-filters');
    const countEl = document.getElementById('filter-count');
    let chips = [];

    // Quick filters
    for (const filter of activeQuickFilters) {
        chips.push(`<span class="active-filter-chip" onclick="toggleQuickFilter('${filter}')">${filter} &times;</span>`);
    }

    // Advanced filters
    if (advancedFilters.state) chips.push(`<span class="active-filter-chip" onclick="clearAdvancedFilter('state')">State: ${advancedFilters.state} &times;</span>`);
    if (advancedFilters.partition) chips.push(`<span class="active-filter-chip" onclick="clearAdvancedFilter('partition')">Partition: ${advancedFilters.partition} &times;</span>`);
    if (advancedFilters.name) chips.push(`<span class="active-filter-chip" onclick="clearAdvancedFilter('name')">Name: ${advancedFilters.name} &times;</span>`);
    if (advancedFilters.dateFrom || advancedFilters.dateTo) {
        const range = [advancedFilters.dateFrom, advancedFilters.dateTo].filter(Boolean).join(' - ');
        chips.push(`<span class="active-filter-chip" onclick="clearAdvancedFilter('date')">Date: ${range} &times;</span>`);
    }
    if (advancedFilters.runtimeMin || advancedFilters.runtimeMax) {
        const range = [advancedFilters.runtimeMin, advancedFilters.runtimeMax].filter(Boolean).join(' - ');
        chips.push(`<span class="active-filter-chip" onclick="clearAdvancedFilter('runtime')">Runtime: ${range} &times;</span>`);
    }
    if (advancedFilters.exitCode) chips.push(`<span class="active-filter-chip" onclick="clearAdvancedFilter('exitCode')">Exit: ${advancedFilters.exitCode} &times;</span>`);

    container.innerHTML = chips.join('');

    // Update count
    const totalFiltered = filterJobs([...allRunningJobs, ...allRecentJobs]).length;
    const total = allRunningJobs.length + allRecentJobs.length;

    if (chips.length > 0 || searchQuery) {
        countEl.textContent = `Showing ${totalFiltered} of ${total} jobs`;
    } else {
        countEl.textContent = 'Showing all jobs';
    }
}

function clearAdvancedFilter(field) {
    switch (field) {
        case 'state':
            advancedFilters.state = '';
            document.getElementById('filter-state').value = '';
            break;
        case 'partition':
            advancedFilters.partition = '';
            document.getElementById('filter-partition').value = '';
            break;
        case 'name':
            advancedFilters.name = '';
            advancedFilters.nameRegex = false;
            document.getElementById('filter-name').value = '';
            document.getElementById('filter-name-regex').checked = false;
            break;
        case 'date':
            advancedFilters.dateFrom = '';
            advancedFilters.dateTo = '';
            document.getElementById('filter-date-from').value = '';
            document.getElementById('filter-date-to').value = '';
            break;
        case 'runtime':
            advancedFilters.runtimeMin = '';
            advancedFilters.runtimeMax = '';
            document.getElementById('filter-runtime-min').value = '';
            document.getElementById('filter-runtime-max').value = '';
            break;
        case 'exitCode':
            advancedFilters.exitCode = '';
            document.getElementById('filter-exit-code').value = '';
            break;
    }

    updateActiveFiltersDisplay();
    refreshJobTables();
    updateUrlWithFilters();
}

function refreshJobTables() {
    renderRunning(filterJobs(allRunningJobs));
    renderRecent(filterJobs(allRecentJobs));
}

async function loadPartitionsForFilter() {
    const select = document.getElementById('filter-partition');
    if (select.options.length > 1) return; // Already loaded

    try {
        const res = await fetch('/api/partitions');
        if (!res.ok) return;
        const data = await res.json();

        for (const part of data.partitions || []) {
            const opt = document.createElement('option');
            opt.value = part.name;
            opt.textContent = part.name;
            select.appendChild(opt);
        }
    } catch (err) {
        console.error('Partition load error:', err);
    }
}

// ==========================================================================
// Saved Searches Functions
// ==========================================================================

function loadSavedSearches() {
    const select = document.getElementById('saved-searches');
    select.innerHTML = '<option value="">Saved searches...</option>';

    for (const search of savedSearches) {
        const opt = document.createElement('option');
        opt.value = search.id;
        opt.textContent = search.name;
        select.appendChild(opt);
    }
}

function applySavedSearch() {
    const select = document.getElementById('saved-searches');
    const searchId = select.value;
    if (!searchId) return;

    const search = savedSearches.find(s => s.id === searchId);
    if (!search) return;

    // Clear existing filters
    activeQuickFilters.clear();
    document.querySelectorAll('.filter-chip').forEach(chip => chip.classList.remove('active'));

    // Apply saved search
    if (search.query) {
        searchQuery = search.query;
        searchBox.value = search.query;
    }

    if (search.quickFilters) {
        for (const filter of search.quickFilters) {
            activeQuickFilters.add(filter);
            const chip = document.querySelector(`.filter-chip[data-filter="${filter}"]`);
            if (chip) chip.classList.add('active');
        }
    }

    if (search.advanced) {
        advancedFilters = { ...advancedFilters, ...search.advanced };
        // Update form inputs
        document.getElementById('filter-state').value = advancedFilters.state || '';
        document.getElementById('filter-partition').value = advancedFilters.partition || '';
        document.getElementById('filter-name').value = advancedFilters.name || '';
        document.getElementById('filter-name-regex').checked = advancedFilters.nameRegex || false;
        document.getElementById('filter-date-from').value = advancedFilters.dateFrom || '';
        document.getElementById('filter-date-to').value = advancedFilters.dateTo || '';
        document.getElementById('filter-runtime-min').value = advancedFilters.runtimeMin || '';
        document.getElementById('filter-runtime-max').value = advancedFilters.runtimeMax || '';
        document.getElementById('filter-exit-code').value = advancedFilters.exitCode || '';
    }

    updateActiveFiltersDisplay();
    refreshJobTables();
    updateUrlWithFilters();

    // Reset dropdown
    select.value = '';
}

function saveCurrentSearch() {
    const hasFilters = searchQuery ||
        activeQuickFilters.size > 0 ||
        Object.values(advancedFilters).some(v => v);

    if (!hasFilters) {
        alert('No filters to save. Apply some filters first.');
        return;
    }

    const name = prompt('Enter a name for this search:');
    if (!name) return;

    const search = {
        id: Date.now().toString(),
        name: name,
        query: searchQuery,
        quickFilters: [...activeQuickFilters],
        advanced: { ...advancedFilters },
        createdAt: new Date().toISOString()
    };

    savedSearches.push(search);
    localStorage.setItem('savedSearches', JSON.stringify(savedSearches));
    loadSavedSearches();
}

function deleteSavedSearch(searchId) {
    savedSearches = savedSearches.filter(s => s.id !== searchId);
    localStorage.setItem('savedSearches', JSON.stringify(savedSearches));
    loadSavedSearches();
}

// ==========================================================================
// URL Filter Encoding
// ==========================================================================

function updateUrlWithFilters() {
    const params = new URLSearchParams();

    if (searchQuery) params.set('q', searchQuery);
    if (activeQuickFilters.size > 0) params.set('quick', [...activeQuickFilters].join(','));
    if (advancedFilters.state) params.set('state', advancedFilters.state);
    if (advancedFilters.partition) params.set('partition', advancedFilters.partition);
    if (advancedFilters.name) params.set('name', advancedFilters.name);
    if (advancedFilters.nameRegex) params.set('regex', '1');
    if (advancedFilters.dateFrom) params.set('from', advancedFilters.dateFrom);
    if (advancedFilters.dateTo) params.set('to', advancedFilters.dateTo);
    if (advancedFilters.runtimeMin) params.set('rtmin', advancedFilters.runtimeMin);
    if (advancedFilters.runtimeMax) params.set('rtmax', advancedFilters.runtimeMax);
    if (advancedFilters.exitCode) params.set('exit', advancedFilters.exitCode);

    const newUrl = params.toString()
        ? `${window.location.pathname}?${params.toString()}`
        : window.location.pathname;

    window.history.replaceState({}, '', newUrl);
}

function loadFiltersFromUrl() {
    const params = new URLSearchParams(window.location.search);

    if (params.has('q')) {
        searchQuery = params.get('q');
        searchBox.value = searchQuery;
    }

    if (params.has('quick')) {
        const quickFilters = params.get('quick').split(',');
        for (const filter of quickFilters) {
            if (filter) {
                activeQuickFilters.add(filter);
                const chip = document.querySelector(`.filter-chip[data-filter="${filter}"]`);
                if (chip) chip.classList.add('active');
            }
        }
    }

    if (params.has('state')) {
        advancedFilters.state = params.get('state');
        document.getElementById('filter-state').value = advancedFilters.state;
    }
    if (params.has('partition')) {
        advancedFilters.partition = params.get('partition');
        document.getElementById('filter-partition').value = advancedFilters.partition;
    }
    if (params.has('name')) {
        advancedFilters.name = params.get('name');
        document.getElementById('filter-name').value = advancedFilters.name;
    }
    if (params.has('regex')) {
        advancedFilters.nameRegex = true;
        document.getElementById('filter-name-regex').checked = true;
    }
    if (params.has('from')) {
        advancedFilters.dateFrom = params.get('from');
        document.getElementById('filter-date-from').value = advancedFilters.dateFrom;
    }
    if (params.has('to')) {
        advancedFilters.dateTo = params.get('to');
        document.getElementById('filter-date-to').value = advancedFilters.dateTo;
    }
    if (params.has('rtmin')) {
        advancedFilters.runtimeMin = params.get('rtmin');
        document.getElementById('filter-runtime-min').value = advancedFilters.runtimeMin;
    }
    if (params.has('rtmax')) {
        advancedFilters.runtimeMax = params.get('rtmax');
        document.getElementById('filter-runtime-max').value = advancedFilters.runtimeMax;
    }
    if (params.has('exit')) {
        advancedFilters.exitCode = params.get('exit');
        document.getElementById('filter-exit-code').value = advancedFilters.exitCode;
    }

    // Show advanced filters panel if any advanced filters are set
    const hasAdvanced = advancedFilters.state || advancedFilters.partition ||
        advancedFilters.name || advancedFilters.dateFrom || advancedFilters.dateTo ||
        advancedFilters.runtimeMin || advancedFilters.runtimeMax || advancedFilters.exitCode;

    if (hasAdvanced) {
        advancedFiltersVisible = true;
        document.getElementById('advanced-filters').style.display = 'block';
        document.getElementById('filter-toggle').classList.add('active');
    }

    updateActiveFiltersDisplay();
}

// ============================================================================
// Layout Management
// ============================================================================

// Layout presets define which widgets are visible and their sizes
const layoutPresets = {
    default: {
        name: 'Default',
        widgets: {
            insights: { visible: true, minimized: false },
            running: { visible: true, minimized: false },
            recent: { visible: true, minimized: false },
            logs: { visible: true, minimized: false }
        },
        gridTemplate: null // Use default CSS grid
    },
    monitoring: {
        name: 'Monitoring',
        widgets: {
            insights: { visible: true, minimized: true },
            running: { visible: true, minimized: false },
            recent: { visible: true, minimized: false },
            logs: { visible: true, minimized: true }
        },
        gridTemplate: null
    },
    debugging: {
        name: 'Debugging',
        widgets: {
            insights: { visible: false, minimized: false },
            running: { visible: true, minimized: true },
            recent: { visible: true, minimized: true },
            logs: { visible: true, minimized: false }
        },
        gridTemplate: null
    },
    compact: {
        name: 'Compact',
        widgets: {
            insights: { visible: false, minimized: false },
            running: { visible: true, minimized: false },
            recent: { visible: true, minimized: false },
            logs: { visible: true, minimized: true }
        },
        gridTemplate: null
    }
};

// Current layout state
let currentLayout = localStorage.getItem('dashboardLayout') || 'default';
let minimizedWidgets = new Set(JSON.parse(localStorage.getItem('minimizedWidgets') || '[]'));

function setLayout(layoutName) {
    if (!layoutPresets[layoutName]) return;

    currentLayout = layoutName;
    localStorage.setItem('dashboardLayout', layoutName);

    const layout = document.getElementById('dashboard-layout');
    if (layout) {
        layout.setAttribute('data-layout', layoutName);
    }

    // Update layout buttons
    document.querySelectorAll('.layout-btn').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.layout === layoutName);
    });

    // Apply widget visibility and minimized states from preset
    const preset = layoutPresets[layoutName];
    for (const [widgetId, config] of Object.entries(preset.widgets)) {
        const panel = document.getElementById(`${widgetId}-panel`);
        if (!panel) continue;

        // Set visibility
        if (config.visible === false) {
            panel.classList.add('widget-hidden');
        } else {
            panel.classList.remove('widget-hidden');
        }

        // Set minimized state
        if (config.minimized) {
            panel.classList.add('widget-minimized');
            minimizedWidgets.add(`${widgetId}-panel`);
            const btn = panel.querySelector('.widget-minimize');
            if (btn) btn.textContent = '+';
        } else {
            panel.classList.remove('widget-minimized');
            minimizedWidgets.delete(`${widgetId}-panel`);
            const btn = panel.querySelector('.widget-minimize');
            if (btn) btn.textContent = '−';
        }
    }

    saveMinimizedWidgets();
}

function toggleWidgetMinimize(panelId) {
    const panel = document.getElementById(panelId);
    if (!panel) return;

    const isMinimized = panel.classList.toggle('widget-minimized');
    const btn = panel.querySelector('.widget-minimize');

    if (isMinimized) {
        minimizedWidgets.add(panelId);
        if (btn) btn.textContent = '+';
    } else {
        minimizedWidgets.delete(panelId);
        if (btn) btn.textContent = '−';
    }

    saveMinimizedWidgets();
}

function saveMinimizedWidgets() {
    localStorage.setItem('minimizedWidgets', JSON.stringify([...minimizedWidgets]));
}

function restoreLayoutState() {
    // Restore layout
    const savedLayout = localStorage.getItem('dashboardLayout') || 'default';
    const layout = document.getElementById('dashboard-layout');
    if (layout) {
        layout.setAttribute('data-layout', savedLayout);
    }

    // Update layout buttons
    document.querySelectorAll('.layout-btn').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.layout === savedLayout);
    });

    // Restore minimized widgets
    const savedMinimized = JSON.parse(localStorage.getItem('minimizedWidgets') || '[]');
    minimizedWidgets = new Set(savedMinimized);

    for (const panelId of minimizedWidgets) {
        const panel = document.getElementById(panelId);
        if (panel) {
            panel.classList.add('widget-minimized');
            const btn = panel.querySelector('.widget-minimize');
            if (btn) btn.textContent = '+';
        }
    }

    // Apply visibility from preset if not default
    if (savedLayout !== 'default' && layoutPresets[savedLayout]) {
        const preset = layoutPresets[savedLayout];
        for (const [widgetId, config] of Object.entries(preset.widgets)) {
            const panel = document.getElementById(`${widgetId}-panel`);
            if (panel && config.visible === false) {
                panel.classList.add('widget-hidden');
            }
        }
    }
}

// Initialize layout on load
restoreLayoutState();

// Initialize
fetchJobs();
loadInsights(); // Load insights on startup
loadSavedSearches(); // Load saved searches
loadFiltersFromUrl(); // Load filters from URL
setInterval(fetchJobs, 8000);
setInterval(updateExpandedJobResources, 30000); // Update resources every 30 seconds
setInterval(loadInsights, 300000); // Refresh insights every 5 minutes
