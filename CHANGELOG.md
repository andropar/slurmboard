# Changelog

## 0.2.0

The data layer was rebuilt so that Slurm, not the log directory, is the source of truth.

### Fixed
- Log pane stayed blank when a log contained a warning or error (#2). The same missing element also broke comparison view and Ctrl+F.
- Jobs without log files matching `--log-pattern` were invisible. Jobs are now listed from `squeue` and `sacct`.
- Array tasks were merged into one row; each task is now listed with its own logs.
- A failing or slow Slurm command looked like "no jobs". Errors are shown in a banner and the last data is kept.
- Memory efficiency was always N/A (it was read from the job line, which carries no memory usage) and CPU efficiency was 0 for jobs with under an hour of CPU time.
- Partition dropdowns were empty; pending-job queue details never rendered; live resources never loaded for plain batch jobs.
- Job names and other text were inserted into the page unescaped.
- Timestamps were interpreted in the browser's time zone instead of the cluster's.
- Idle log streams kept a server thread forever after the tab was closed.
- Import failed on Python 3.8.

### Added
- Access token authentication and cross-site request protection (see "Security" in the README).
- `--all-users` to show every user's jobs, read-only for jobs that are not yours (#1).
- `--history-days`, `--token`, `--no-auth`, `--verbose`.
- Logs are located from Slurm's own records, so most setups need no `--log-pattern`.
- Batch scripts are shown from the copy Slurm kept at submission, falling back to the file on disk.
- Resubmit from finished jobs; resubmitting works for jobs whose script file is gone.
- Templates are saved to `~/.config/slurm-dashboard/templates.json` instead of being lost on restart.
- Test suite running against a fake Slurm CLI.

### Changed
- **New interface.** The frontend was rewritten as a master-detail app: one job list on the left, a page per job on the right with tabs for output, errors, script and details. White page, hairline rules, one type family, colour only for job state; light and dark; works on a phone.
- Array tasks are folded into one row per array. Finished jobs are grouped by day. States are written out ("Out of memory", not "OOM"), and pending reasons are explained in plain words.
- Running jobs show runtime against the time limit as a progress bar, which turns orange near the limit.
- The log viewer strips terminal colour codes, collapses carriage-return progress bars to their latest state, shows real line numbers, pauses following when you scroll up, and can load or download the whole file. A failed job opens on its error output.
- Search understands `name:`, `partition:`, `state:`, `user:`, `exit:` and `node:` filters. The current job, tab and filter live in the address bar.
- Keyboard shortcuts (`?` lists them), batch selection with shift-click, a confirmation dialog for cancel, and short notices instead of blocking alerts.
- Timeline shows one lane per job name instead of one row per job. Pipelines draws every dependency chain as a graph. Activity and Insights are one page with jobs per day, efficiency, and per-job-name and per-partition tables.
- The list can go back 30 or 90 days on request. Long history windows are cached longer and topped up with the last day, so they stay cheap for the accounting database.
- One `squeue` and one `sacct` result are cached and shared by all views and tabs; polling pauses while the tab is hidden.
- Script text is submitted through stdin; no temporary files.
- API: `/api/script_content?path=` is replaced by `/api/job_script/<job_id>`; `/api/resubmit` takes `job_id` instead of `script_path`; `/api/cost` is now `/api/usage`; `log_key` is the job ID. New: `/api/job/<job_id>` (summary, submission and log files in one call), `/api/log_file/<job_id>` (download), `/api/jobs?days=`, `/stream_log?full=1`.

### Removed
- Completion-time prediction, estimated queue wait and the "service units" cost figure. They were heuristics presented as facts. Pending jobs now show the reason and planned start time that Slurm itself reports.
- The A-D efficiency grade; the underlying percentages are shown instead.
- Log annotations, side-by-side log comparison, saved searches (bookmark the URL instead), layout presets, the hour-of-week heatmap and column sorting.
- Jobs that only exist as log files are no longer mixed into the list. They are listed only when the accounting database is unavailable.
