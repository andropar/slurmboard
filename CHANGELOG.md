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
- One `squeue` and one `sacct` result are cached and shared by all views and tabs; polling pauses while the tab is hidden.
- Script text is submitted through stdin; no temporary files.
- API: `/api/script_content?path=` is replaced by `/api/job_script/<job_id>`; `/api/resubmit` takes `job_id` instead of `script_path`; `/api/cost` is now `/api/usage`; `log_key` is the job ID.

### Removed
- Completion-time prediction, estimated queue wait and the "service units" cost figure. They were heuristics presented as facts. Pending jobs now show the reason and planned start time that Slurm itself reports.
- The A-D efficiency grade; the underlying percentages are shown instead.
