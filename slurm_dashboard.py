#!/usr/bin/env python3
import argparse
import json
import re
import subprocess
import time
from functools import lru_cache
from pathlib import Path
from typing import Optional

from flask import (
    Flask,
    Response,
    jsonify,
    render_template_string,
    request,
    stream_with_context,
)

INDEX_HTML = """
<!doctype html>
<html lang=\"en\">
<head>
    <meta charset=\"utf-8\">
    <title>Slurm Dashboard</title>
    <style>
        * { box-sizing: border-box; }
        body { 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            margin: 0; 
            padding: 24px; 
            background:
                radial-gradient(circle at top left, rgba(129, 140, 248, 0.18), transparent 55%),
                radial-gradient(circle at bottom right, rgba(244, 114, 182, 0.16), transparent 55%),
                linear-gradient(135deg, #edf2f7 0%, #f7fafc 40%, #ffffff 100%);
            color: #1a202c; 
            height: 100vh;
            display: flex;
            flex-direction: column;
            align-items: stretch;
            justify-content: flex-start;
            overflow: hidden;
        }
        .header-row {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 12px;
            flex-shrink: 0;
            gap: 12px;
        }
        .stat-badge {
            background: #edf2f7;
            color: #1a202c;
            padding: 4px 10px;
            border-radius: 999px;
            font-size: 11px;
            font-weight: 600;
            display: inline-flex;
            align-items: center;
            gap: 6px;
            border: 1px solid #e2e8f0;
        }
        .stat-num {
            background: #ffffff;
            padding: 2px 8px;
            border-radius: 999px;
            font-family: monospace;
            color: #4c51bf;
        }
        .search-box {
            background: rgba(255,255,255,0.9);
            border: 1px solid #cbd5e0;
            border-radius: 999px;
            padding: 8px 14px;
            font-size: 14px;
            width: 260px;
            outline: none;
            transition: all 0.15s ease;
        }
        .search-box:focus {
            background: #ffffff;
            border-color: #667eea;
            box-shadow: 0 0 0 3px rgba(102,126,234,0.35);
        }
        h2 { 
            margin: 0 0 16px 0; 
            color: #2d3748; 
            font-size: 18px; 
            font-weight: 600; 
        }
        .card {
            background: #ffffff;
            border-radius: 14px;
            box-shadow: 0 10px 25px rgba(15, 23, 42, 0.08);
            padding: 20px;
            border: 1px solid #e2e8f0;
            transition: box-shadow 0.18s ease, transform 0.1s ease, border-color 0.15s ease;
        }
        .card:hover { 
            box-shadow: 0 16px 30px rgba(15, 23, 42, 0.12);
            transform: translateY(-1px);
            border-color: #cbd5e0;
        }
        table { 
            border-collapse: collapse; 
            width: 100%; 
            margin: 0;
            table-layout: fixed;
        }
        th, td { 
            padding: 8px 10px; 
            text-align: left; 
            font-size: 12px; 
        }
        th { 
            background: #f7fafc; 
            color: #4a5568; 
            font-weight: 600; 
            text-transform: uppercase; 
            letter-spacing: 0.05em; 
            font-size: 11px;
            border-bottom: 2px solid #e2e8f0;
            cursor: pointer;
            user-select: none;
            position: relative;
        }
        th:hover { background: #edf2f7; }
        th.sortable::after {
            content: '⇅';
            position: absolute;
            right: 8px;
            opacity: 0.3;
            font-size: 10px;
        }
        th.sort-asc::after { content: '↑'; opacity: 1; }
        th.sort-desc::after { content: '↓'; opacity: 1; }
        td { 
            border-bottom: 1px solid #e2e8f0; 
            color: #2d3748;
        }
        tbody tr { transition: background-color 0.12s ease; }
        tbody tr:hover { background: #f7fafc; }
        tbody tr.active-log { background: #ebf4ff; }
        tbody tr.active-log:hover { background: #dbeafe; }
        tbody tr:last-child td { border-bottom: none; }
        .expand-btn {
            background: #e2e8f0;
            color: #4a5568;
            padding: 2px 6px;
            font-size: 11px;
            cursor: pointer;
            border-radius: 3px;
            margin-left: 4px;
            display: inline-block;
            min-width: 16px;
            text-align: center;
        }
        .expand-btn:hover { background: #cbd5e0; }
        .details-row {
            background: #f7fafc !important;
            border-top: none !important;
        }
        .details-row td {
            padding: 12px 14px;
            border-bottom: 1px solid #e2e8f0;
        }
        .details-content {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 12px;
            font-size: 12px;
        }
        .detail-item {
            display: flex;
            flex-direction: column;
            gap: 4px;
        }
        .detail-label {
            color: #718096;
            font-weight: 600;
            text-transform: uppercase;
            font-size: 10px;
            letter-spacing: 0.05em;
        }
        .detail-value {
            color: #2d3748;
            font-family: monospace;
        }
        .efficiency-bar {
            height: 6px;
            background: #e2e8f0;
            border-radius: 3px;
            overflow: hidden;
            margin-top: 4px;
        }
        .efficiency-fill {
            height: 100%;
            transition: width 0.3s ease;
        }
        .eff-good { background: #48bb78; }
        .eff-medium { background: #ed8936; }
        .eff-bad { background: #f56565; }
        button { 
            padding: 6px 12px; 
            font-size: 12px; 
            cursor: pointer; 
            background: linear-gradient(135deg, #4f46e5, #6366f1);
            color: #fff;
            border: none;
            border-radius: 6px;
            font-weight: 500;
            transition: all 0.2s ease;
            margin-right: 4px;
        }
        button:hover { 
            background: linear-gradient(135deg, #4338ca, #4f46e5);
            transform: translateY(-1px);
            box-shadow: 0 2px 4px rgba(102, 126, 234, 0.4);
        }
        button:active { transform: translateY(0); }
        button.copy-btn {
            background: #48bb78;
            padding: 4px 8px;
            font-size: 11px;
        }
        button.copy-btn:hover { background: #38a169; }
        .status-badge {
            display: inline-block;
            padding: 2px 8px;
            border-radius: 999px;
            font-size: 10px;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.06em;
        }
        .status-running { background: #c6f6d5; color: #22543d; }
        .status-pending { background: #feebc8; color: #7c2d12; }
        .status-completed { background: #bee3f8; color: #2c5282; }
        .status-failed { background: #fed7d7; color: #742a2a; }
        .status-timeout { background: #fbd38d; color: #744210; }
        #log-panel { 
            background: #1a202c; 
            color: #68d391; 
            padding: 16px; 
            border-radius: 8px; 
            flex: 1;
            min-height: 0;
            overflow: auto; 
            white-space: pre-wrap; 
            font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace; 
            font-size: 13px;
            line-height: 1.5;
            box-shadow: inset 0 2px 4px rgba(0,0,0,0.2);
        }
        #log-panel::-webkit-scrollbar { width: 8px; }
        #log-panel::-webkit-scrollbar-track { background: #2d3748; }
        #log-panel::-webkit-scrollbar-thumb { background: #4a5568; border-radius: 4px; }
        #log-panel::-webkit-scrollbar-thumb:hover { background: #718096; }
        #log-wrapper { 
            display: flex; 
            flex-direction: column; 
            min-height: 0;
            overflow: hidden;
        }
        .log-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 12px;
        }
        #log-title { 
            color: #2d3748; 
            font-size: 14px; 
            font-weight: 600;
            margin: 0;
        }
        .scroll-toggle {
            background: #e2e8f0;
            color: #4a5568;
            padding: 4px 10px;
            font-size: 11px;
            border-radius: 4px;
            cursor: pointer;
            border: none;
            font-weight: 600;
            margin: 0;
        }
        .scroll-toggle.active {
            background: #667eea;
            color: #fff;
        }
        .layout { 
            display: grid; 
            grid-template-columns: minmax(0, 1.05fr) minmax(0, 1.75fr); 
            grid-gap: 24px; 
            flex: 1;
            min-height: 0;
        }
        .table-column { 
            display: flex; 
            flex-direction: column; 
            gap: 16px; 
            min-height: 0; 
            overflow: hidden;
        }
        .running-wrapper {
            display: flex;
            flex-direction: column;
            min-height: 0;
            flex: 1;
        }
        .running-table-scroll {
            flex: 1;
            min-height: 0;
            overflow-y: auto;
            border-radius: 8px;
            border: 1px solid #e2e8f0;
            background: #fff;
        }
        .running-table-scroll table { margin: 0; }
        .running-table-scroll tbody tr:last-child td { border-bottom: none; }
        .running-table-scroll::-webkit-scrollbar { width: 8px; }
        .running-table-scroll::-webkit-scrollbar-track { background: #f1f1f1; }
        .running-table-scroll::-webkit-scrollbar-thumb { background: #cbd5e0; border-radius: 4px; }
        .running-table-scroll::-webkit-scrollbar-thumb:hover { background: #a0aec0; }
        .recent-wrapper { 
            flex: 1; 
            min-height: 0; 
            display: flex; 
            flex-direction: column; 
            overflow: hidden;
        }
        .table-scroll { 
            flex: 1; 
            min-height: 0; 
            overflow-y: auto; 
            border-radius: 8px; 
            background: #fff; 
            border: 1px solid #e2e8f0;
        }
        .table-scroll::-webkit-scrollbar { width: 8px; }
        .table-scroll::-webkit-scrollbar-track { background: #f1f1f1; }
        .table-scroll::-webkit-scrollbar-thumb { background: #cbd5e0; border-radius: 4px; }
        .table-scroll::-webkit-scrollbar-thumb:hover { background: #a0aec0; }
        .table-scroll table { margin: 0; }
        @media (max-width: 1100px) {
            body { height: auto; min-height: 100vh; overflow: auto; }
            .layout { 
                grid-template-columns: 1fr; 
                flex: none;
                overflow: visible;
            }
            .table-column { 
                min-height: auto; 
                overflow: visible;
            }
            .recent-wrapper { 
                max-height: 50vh; 
            }
            #log-wrapper {
                overflow: visible;
            }
            #log-panel { 
                height: 50vh; 
                min-height: 280px; 
                flex: none;
            }
        }
        .job-col { width: 220px; max-width: 260px; }
        .job-cell { 
            font-weight: 500;
            overflow: hidden;
        }
        .job-main-top {
            display: flex;
            align-items: center;
            gap: 6px;
            margin-bottom: 2px;
        }
        .job-main-name {
            font-size: 11px;
            color: #4a5568;
            overflow: hidden;
            white-space: nowrap;
            text-overflow: ellipsis;
        }
        .empty-state {
            padding: 32px;
            text-align: center;
            color: #718096;
            font-size: 14px;
        }
        .metric { 
            display: inline-block; 
            padding: 3px 8px; 
            background: #edf2f7; 
            border-radius: 999px; 
            font-family: monospace; 
            font-size: 12px;
            color: #4a5568;
        }
        .header-meta {
            display: flex;
            align-items: baseline;
            gap: 6px;
            font-size: 11px;
            color: #718096;
        }
        .last-updated-label {
            text-transform: uppercase;
            letter-spacing: .08em;
            font-weight: 600;
            opacity: 0.7;
        }
        .last-updated-value {
            font-family: monospace;
            color: #2d3748;
        }
        @media (max-width: 900px) {
            .col-runtime {
                display: none;
            }
        }
    </style>
</head>
<body>
    <div class="header-row">
        <div class="header-meta">
            <span class="last-updated-label">Updated</span>
            <span class="last-updated-value" id="last-updated">–</span>
        </div>
        <input type="text" class="search-box" id="search-box" placeholder="Search jobs... (/)">
    </div>
    <div class="layout">
        <div class="table-column">
            <div class="card running-wrapper">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;">
                    <h2 style="margin-bottom: 0;">Running Jobs</h2>
                    <div class="stat-badge">
                        <span>Running</span>
                        <span class="stat-num" id="stat-running">0</span>
                    </div>
                </div>
                <div class="running-table-scroll">
                    <table id="running-table">
                        <thead>
                            <tr>
                                <th class="job-col sortable col-main" data-sort="name">Job</th>
                                <th class="sortable col-state" data-sort="state">State</th>
                                <th class="sortable col-runtime" data-sort="runtime">Runtime</th>
                                <th class="col-actions">Actions</th>
                            </tr>
                        </thead>
                        <tbody id="running-body"><tr><td colspan="4"><div class="empty-state">Loading…</div></td></tr></tbody>
                    </table>
                </div>
            </div>
            <div class="recent-wrapper">
                <div class="card" style="flex: 1; display: flex; flex-direction: column; min-height: 0;">
                    <h2>Recent Jobs</h2>
                    <div class="table-scroll">
                        <table id="recent-table">
                            <thead>
                                <tr>
                                    <th class="sortable" data-sort="updated">Updated</th>
                                    <th class="job-col sortable" data-sort="name">Name</th>
                                    <th>ID</th>
                                    <th>Stdout</th>
                                    <th>Stderr</th>
                                    <th class="sortable" data-sort="size">Size</th>
                                </tr>
                            </thead>
                            <tbody id="recent-body"><tr><td colspan="6"><div class="empty-state">Loading…</div></td></tr></tbody>
                        </table>
                    </div>
                </div>
            </div>
        </div>
        <div class="card" id="log-wrapper">
            <div class="log-header">
                <h2 id="log-title">Logs</h2>
                <button class="scroll-toggle active" id="scroll-toggle" onclick="toggleAutoScroll()">Auto-scroll</button>
            </div>
            <pre id="log-panel"></pre>
        </div>
    </div>
    <script>
        const runningBody = document.getElementById('running-body');
        const recentBody = document.getElementById('recent-body');
        const logPanel = document.getElementById('log-panel');
        const logTitle = document.getElementById('log-title');
        const searchBox = document.getElementById('search-box');
        const scrollToggle = document.getElementById('scroll-toggle');
        let logStream = null;
        let autoScroll = true;
        let currentLogKey = null;
        let allRunningJobs = [];
        let allRecentJobs = [];
        let searchQuery = '';
        let sortState = { table: null, column: null, direction: 'asc' };
        let expandedJobs = new Set();
        let jobDetails = {};
        
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

        async function fetchJobs() {
            try {
                const res = await fetch('/api/jobs');
                if (!res.ok) return;
                const data = await res.json();
                allRunningJobs = data.running;
                allRecentJobs = data.recent;
                
                document.getElementById('stat-running').textContent = data.running.length;
                const lastUpdatedEl = document.getElementById('last-updated');
                if (lastUpdatedEl) {
                    const now = new Date();
                    lastUpdatedEl.textContent = now.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
                }
                
                renderRunning(filterJobs(allRunningJobs));
                renderRecent(filterJobs(allRecentJobs));
            } catch (err) {
                console.error(err);
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
                        </div>
                    </td>
                </tr>
            `;
        }

        function filterJobs(jobs) {
            if (!searchQuery) return jobs;
            const q = searchQuery.toLowerCase();
            return jobs.filter(job => 
                (job.name && job.name.toLowerCase().includes(q)) ||
                (job.id && job.id.toString().includes(q)) ||
                (job.state && job.state.toLowerCase().includes(q))
            );
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

        function renderRunning(rows) {
            if (!rows.length) {
                runningBody.innerHTML = '<tr><td colspan="4"><div class="empty-state">No running jobs.</div></td></tr>';
                return;
            }
            runningBody.innerHTML = rows.map(job => `
                <tr class="${currentLogKey === job.log_key ? 'active-log' : ''}" data-log-key="${job.log_key}">
                    <td class="job-cell col-main">
                        <div class="job-main-top">
                            <span class="expand-btn" data-job-id="${job.id}" onclick="toggleJobDetails('${job.id}')">${expandedJobs.has(job.id) ? '▾' : '▸'}</span>
                            <span class="metric">${job.id}</span>
                            <button class="copy-btn" onclick="copyToClipboard('${job.id}', this)">⎘</button>
                        </div>
                        <div class="job-main-name" title="${job.name}">${job.name}</div>
                    </td>
                    <td class="col-state"><span class="status-badge ${getStatusClass(job.state)}">${formatStateShort(job.state)}</span></td>
                    <td class="col-runtime"><span class="metric">${job.runtime}</span></td>
                    <td class="col-actions">
                        <button onclick="openLog('${job.log_key}','stdout')">stdout</button>
                        <button onclick="openLog('${job.log_key}','stderr')">stderr</button>
                        <button onclick="cancelJob('${job.id}')" style="background: #f56565;">cancel</button>
                    </td>
                </tr>
                ${expandedJobs.has(job.id) ? renderDetailsRow(job, 4) : ''}`).join('');
        }

        function renderRecent(rows) {
            if (!rows.length) {
                recentBody.innerHTML = '<tr><td colspan="6"><div class="empty-state">No recent logs found.</div></td></tr>';
                return;
            }
            recentBody.innerHTML = rows.map(job => `
                <tr class="${currentLogKey === job.log_key ? 'active-log' : ''}" data-log-key="${job.log_key}">
                    <td style="font-size: 12px; color: #718096;" title="${job.updated}">${relativeTime(job.updated)}</td>
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
                ${expandedJobs.has(job.id) ? renderDetailsRow(job, 6) : ''}`).join('');
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
            logTitle.textContent = `Logs: ${logKey} (${kind})`;
            logPanel.textContent = '';
            if (logStream) logStream.close();
            
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
                        logPanel.textContent = '';
                        logPanel.scrollTop = 0;
                        stick = false;
                    }
                    if (Object.prototype.hasOwnProperty.call(payload, 'snapshot')) {
                        logPanel.textContent = payload.snapshot;
                        logPanel.scrollTop = 0;
                        stick = false;
                    }
                    if (payload.append) {
                        logPanel.textContent += payload.append;
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

        searchBox.addEventListener('input', (e) => {
            searchQuery = e.target.value;
            renderRunning(filterJobs(allRunningJobs));
            renderRecent(filterJobs(allRecentJobs));
        });

        document.addEventListener('keydown', (e) => {
            if (e.key === '/' && e.target.tagName !== 'INPUT') {
                e.preventDefault();
                searchBox.focus();
            }
            if (e.key === 'Escape') {
                searchBox.blur();
                if (logStream) {
                    logStream.close();
                    logStream = null;
                    currentLogKey = null;
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

        fetchJobs();
        setInterval(fetchJobs, 8000);
    </script>
</body>
</html>
"""


def parse_time_to_seconds(time_str: str) -> float:
    """Parse slurm time format (HH:MM:SS or DD-HH:MM:SS) to seconds."""
    if not time_str or time_str == "00:00:00":
        return 0.0

    days = 0
    if "-" in time_str:
        day_part, time_part = time_str.split("-", 1)
        days = int(day_part)
        time_str = time_part

    parts = time_str.split(":")
    if len(parts) == 3:
        hours, mins, secs = parts
        return days * 86400 + int(hours) * 3600 + int(mins) * 60 + float(secs)
    return 0.0


def parse_memory(mem_str: str) -> float:
    """Parse slurm memory format (e.g., '4Gn', '1024M') to bytes."""
    if not mem_str:
        return 0.0

    mem_str = mem_str.strip().rstrip("n").rstrip("c")
    match = re.match(r"([\d.]+)([KMGT]?)", mem_str.upper())
    if not match:
        return 0.0

    value, unit = match.groups()
    value = float(value)

    multipliers = {"K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4, "": 1024**2}
    return value * multipliers.get(unit, 1)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--log-root", type=Path, default=Path("/u/rothj/slurm-logs"))
    parser.add_argument("--user", default="rothj")
    parser.add_argument("--refresh-cache", type=int, default=20)
    return parser.parse_args()


def run_app(args: argparse.Namespace) -> None:
    app = Flask(__name__)
    log_root = args.log_root.expanduser().resolve()
    log_root.mkdir(parents=True, exist_ok=True)

    def safe_log_path(log_key: str, kind: str) -> Optional[Path]:
        if "::" not in log_key:
            return None
        script_name, job_id = log_key.split("::", 1)
        if not job_id.isdigit():
            return None
        folder = (log_root / script_name).resolve()
        if not folder.exists():
            return None
        try:
            folder.relative_to(log_root)
        except ValueError:
            return None
        suffix = "out" if kind == "stdout" else "err"
        target = (folder / f"job.{suffix}.{job_id}").resolve()
        try:
            target.relative_to(log_root)
        except ValueError:
            return None
        if not target.exists():
            return None
        return target

    @lru_cache(maxsize=1)
    def cached_recent(timestamp_bucket: int) -> list[dict]:
        return collect_recent_jobs(log_root)

    def running_jobs() -> list[dict]:
        fmt = "%i|%j|%T|%M|%l|%D|%R"
        try:
            proc = subprocess.run(
                ["squeue", "-u", args.user, "--noheader", f"--format={fmt}"],
                capture_output=True,
                text=True,
                timeout=5,
                check=False,
            )
        except FileNotFoundError:
            return []
        if proc.returncode != 0:
            return []
        rows = []
        for line in proc.stdout.strip().splitlines():
            parts = line.split("|", maxsplit=6)
            if len(parts) != 7:
                continue
            job_id, name, state, runtime, limit, nodes, reason = parts
            log_key = derive_log_key(name, job_id)
            rows.append(
                {
                    "id": job_id,
                    "name": name,
                    "state": state,
                    "runtime": runtime,
                    "limit": limit,
                    "nodes": nodes,
                    "reason": reason,
                    "log_key": log_key,
                }
            )
        return rows

    def derive_log_key(job_name: str, job_id: str) -> str:
        folder = log_root / job_name
        if folder.exists():
            return f"{job_name}::{job_id}"
        candidates = [path for path in log_root.glob(f"{job_name}*") if path.is_dir()]
        if candidates:
            candidates.sort(key=lambda path: path.stat().st_mtime, reverse=True)
            return f"{candidates[0].name}::{job_id}"
        return f"{job_name}::{job_id}"

    def collect_recent_jobs(root: Path, limit: int = 200) -> list[dict]:
        entries = []
        for script_dir in root.iterdir():
            if not script_dir.is_dir():
                continue
            for stdout in script_dir.glob("job.out.*"):
                job_id = stdout.name.split(".")[-1]
                if not job_id.isdigit():
                    continue
                stderr = stdout.with_name(f"job.err.{job_id}")
                updated = time.strftime(
                    "%Y-%m-%d %H:%M:%S", time.localtime(stdout.stat().st_mtime)
                )
                size_bytes = stdout.stat().st_size + (
                    stderr.stat().st_size if stderr.exists() else 0
                )
                entries.append(
                    {
                        "updated": updated,
                        "name": script_dir.name,
                        "id": job_id,
                        "log_key": f"{script_dir.name}::{job_id}",
                        "size": human_size(size_bytes),
                        "size_bytes": size_bytes,
                    }
                )
        entries.sort(key=lambda row: row["updated"], reverse=True)
        return entries[:limit]

    def get_job_details(job_id: str, user: str) -> dict:
        fmt = "JobID,JobName,State,ExitCode,End,CPUTimeRAW,TotalCPU,ReqMem,MaxRSS"
        try:
            proc = subprocess.run(
                [
                    "sacct",
                    "-j",
                    job_id,
                    "-u",
                    user,
                    "--noheader",
                    f"--format={fmt}",
                    "-X",
                ],
                capture_output=True,
                text=True,
                timeout=5,
                check=False,
            )
        except (FileNotFoundError, subprocess.TimeoutExpired):
            return {}

        if proc.returncode != 0:
            return {}

        for line in proc.stdout.strip().splitlines():
            parts = [p.strip() for p in line.split("|")]
            if len(parts) != 9:
                continue

            (
                _,
                job_name,
                state,
                exit_code,
                end_time,
                cpu_time_raw,
                total_cpu,
                req_mem,
                max_rss,
            ) = parts

            cpu_eff = "N/A"
            mem_eff = "N/A"

            try:
                if cpu_time_raw and total_cpu and cpu_time_raw != "0":
                    cpu_seconds = parse_time_to_seconds(total_cpu)
                    cpu_alloc = int(cpu_time_raw)
                    if cpu_alloc > 0:
                        cpu_eff = f"{min(100, (cpu_seconds / cpu_alloc) * 100):.1f}%"
            except (ValueError, ZeroDivisionError):
                pass

            try:
                if req_mem and max_rss:
                    req_bytes = parse_memory(req_mem)
                    used_bytes = parse_memory(max_rss)
                    if req_bytes > 0:
                        mem_eff = f"{min(100, (used_bytes / req_bytes) * 100):.1f}%"
            except (ValueError, ZeroDivisionError):
                pass

            return {
                "state": state,
                "exit_code": exit_code.split(":")[0] if ":" in exit_code else exit_code,
                "cpu_eff": cpu_eff,
                "mem_eff": mem_eff,
                "end_time": end_time if end_time != "Unknown" else "N/A",
            }

        return {}

    @app.route("/")
    def index() -> str:
        return render_template_string(INDEX_HTML)

    @app.route("/api/jobs")
    def api_jobs() -> Response:
        now_bucket = int(time.time() // args.refresh_cache)
        recent = cached_recent(now_bucket)
        return jsonify({"running": running_jobs(), "recent": recent})

    @app.route("/api/job_details/<job_id>")
    def api_job_details(job_id: str) -> Response:
        if not job_id.isdigit():
            return jsonify({"error": "Invalid job ID"}), 400
        details = get_job_details(job_id, args.user)
        return jsonify(details)

    @app.route("/api/cancel/<job_id>", methods=["POST"])
    def api_cancel(job_id: str) -> Response:
        if not job_id.isdigit():
            return jsonify({"error": "Invalid job ID"}), 400
        try:
            proc = subprocess.run(
                ["scancel", job_id],
                capture_output=True,
                text=True,
                timeout=5,
                check=False,
            )
            if proc.returncode == 0:
                return jsonify({"success": True})
            return jsonify({"error": proc.stderr or "Failed to cancel"}), 500
        except FileNotFoundError:
            return jsonify({"error": "scancel command not found"}), 500
        except subprocess.TimeoutExpired:
            return jsonify({"error": "Command timeout"}), 500

    @app.route("/stream_log")
    def stream_log() -> Response:
        log_key = request.args.get("log_key", "")
        kind = request.args.get("kind", "stdout")
        if kind not in {"stdout", "stderr"}:
            return Response("Invalid kind", status=400)
        path = safe_log_path(log_key, kind)
        if path is None:
            return Response("Log not found", status=404)

        def event_stream(target: Path):
            max_bytes = 200_000
            try:
                with target.open("r") as handle:
                    handle.seek(0, 2)
                    file_size = handle.tell()
                    start = max(file_size - max_bytes, 0)
                    handle.seek(start)
                    if start > 0:
                        handle.readline()
                    snapshot = handle.read()
                    position = handle.tell()
                    yield f"data: {json.dumps({'snapshot': snapshot})}\n\n"
                    while True:
                        handle.seek(position)
                        chunk = handle.readline()
                        if chunk:
                            position = handle.tell()
                            yield f"data: {json.dumps({'append': chunk})}\n\n"
                        else:
                            time.sleep(1)
            except GeneratorExit:
                return

        headers = {"Content-Type": "text/event-stream", "Cache-Control": "no-cache"}
        return Response(stream_with_context(event_stream(path)), headers=headers)

    def human_size(size: int) -> str:
        units = ["B", "KB", "MB", "GB", "TB"]
        value = float(size)
        for unit in units:
            if value < 1024 or unit == units[-1]:
                return f"{value:.1f}{unit}"
            value /= 1024
        return f"{value:.1f}PB"

    app.run(host=args.host, port=args.port, debug=False)


def main() -> None:
    args = parse_args()
    run_app(args)


if __name__ == "__main__":
    main()
