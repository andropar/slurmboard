# Slurm Dashboard

[![PyPI version](https://img.shields.io/pypi/v/slurm-dashboard.svg)](https://pypi.org/project/slurm-dashboard/)
[![Python versions](https://img.shields.io/pypi/pyversions/slurm-dashboard.svg)](https://pypi.org/project/slurm-dashboard/)

A lightweight, job-first dashboard for Slurm clusters. See each job's ID, full
name, status, queued/start/end times, submitted script, stdout, and stderr in one
table. No expansion is needed to read long names.

## At a Glance

Slurm Dashboard is a small Flask app and CLI for researchers and ML engineers
running batches of experiments. The default screen focuses on the queue;
secondary analytics and job-management features remain under **Advanced tools**.

- Tracks live and historical jobs through Slurm's `squeue` and `sacct` commands
- Streams stdout/stderr from configurable cluster log layouts
- Sorts all six data columns and paginates after searching/filtering
- Uses blue for running jobs, red for failures, and green for completed jobs
- Ships as a PyPI package with a `slurm-dashboard` command-line entry point

The project is intentionally lightweight: no database, no cluster daemon, and no scheduler-side service. It can run on a login node behind an SSH tunnel and read the same Slurm state a user can already access from the shell.

## Installation

```bash
pip install slurm-dashboard
```

## Features

- **Real-time job monitoring** - Live log streaming with auto-refresh
- **Job table** - Full names, IDs, statuses, and explicit UTC lifecycle timestamps
- **Sorting and pagination** - Click column headers to reverse sort; 100 rows per
  page by default, with 10/25/50/100 options and First/Previous/Next/Last controls
- **Search and filters** - Search ID/name, filter status and queued dates; all
  filters and sorts apply to the entire loaded scope, not just the current page
- **Submitted scripts and logs** - Direct Script/Stdout/Stderr actions; live log
  following, explicit unavailable/error messages, and retry controls
- **Full-space queue** - Jobs fill the available window. Opening a file shows
  jobs on the left (60%) and the file on the right (40%); drag the divider or
  focus it and use arrow keys to resize. Escape closes the file and restores
  the full job list. The split is kept until reload; phones show files full-screen.
- **Advanced tools** - The previous dashboard, including submission, cancellation,
  templates, batch operations, resource analysis, timeline and pipeline views,
  remains available at `/advanced`. Logs there open in a pop-up (close with the
  close button, Escape, or a backdrop click), so job lists keep the full width.
- **Dark mode** - Easy on the eyes during those late-night debugging sessions

## Previous dashboard screenshots

These screenshots show the older dashboard, now under **Advanced tools**, not
the default job table.

<table>
<tr>
<td width="50%">

**Job List & Logs**

<img src="https://raw.githubusercontent.com/andropar/slurmboard/main/docs/images/jobs-logs.png" alt="Job List" width="100%">

</td>
<td width="50%">

**Insights Dashboard**

<img src="https://raw.githubusercontent.com/andropar/slurmboard/main/docs/images/insights.png" alt="Insights" width="100%">

</td>
</tr>
<tr>
<td width="50%">

**Heatmap View**

<img src="https://raw.githubusercontent.com/andropar/slurmboard/main/docs/images/heatmap.png" alt="Heatmap" width="100%">

</td>
<td width="50%">

</td>
</tr>
</table>

## Quick Start

```bash
# Basic usage - uses current user and default log location
slurm-dashboard

# Specify log directory
slurm-dashboard --log-root /path/to/your/logs

# Full options
slurm-dashboard --log-root ~/slurm-logs --user myusername --port 8080
```

Then open `http://localhost:5000` in your browser.

## Configuration

### Command Line Options

| Option | Default | Description |
|--------|---------|-------------|
| `--host` | `127.0.0.1` | Host to bind to |
| `--port` | `5000` | Port to bind to |
| `--log-root` | `~/slurm-logs` | Root directory for Slurm log files |
| `--log-pattern` | `{name}/job.{stream}.{id}` | Log file path pattern |
| `--user` | Current user | Slurm username to filter jobs |
| `--refresh-cache` | `20` | Cache refresh interval in seconds |

### Log File Patterns

The dashboard needs to know where your job logs are stored and how they're named. Use `--log-pattern` to match your cluster's configuration.

**Pattern variables:**
- `{name}` - Job name (from `--job-name` or script name)
- `{id}` - Slurm job ID
- `{stream}` - `out` for stdout, `err` for stderr

#### Common Cluster Configurations

**Default Slurm (flat directory):**
```bash
# Logs like: /home/user/slurm-12345.out, slurm-12345.err
slurm-dashboard --log-root ~ --log-pattern "slurm-{id}.{stream}"
```

**Subdirectory per job name (default):**
```bash
# Logs like: ~/slurm-logs/train_model/job.out.12345
slurm-dashboard --log-root ~/slurm-logs --log-pattern "{name}/job.{stream}.{id}"
```

**Nested by job ID:**
```bash
# Logs like: ~/logs/experiment/12345/stdout, ~/logs/experiment/12345/stderr
slurm-dashboard --log-root ~/logs --log-pattern "{name}/{id}/std{stream}"
```

**Project-based structure:**
```bash
# Logs like: /scratch/myproject/logs/job_12345.out
slurm-dashboard --log-root /scratch/myproject/logs --log-pattern "job_{id}.{stream}"
```

**Date-based directories (use wildcards in log-root):**
```bash
# For logs like: ~/logs/2024-01/train_12345.out
# Run from within the date directory or use a symlink
slurm-dashboard --log-root ~/logs/2024-01 --log-pattern "{name}_{id}.{stream}"
```

### Setting Up Your Slurm Scripts

To work with the dashboard, configure your Slurm scripts to write logs to a consistent location:

```bash
#!/bin/bash
#SBATCH --job-name=train_model
#SBATCH --output=/home/%u/slurm-logs/%x/job.out.%j
#SBATCH --error=/home/%u/slurm-logs/%x/job.err.%j

# Your job commands here
python train.py
```

**Slurm filename variables:**
- `%u` - Username
- `%x` - Job name
- `%j` - Job ID
- `%A` - Array job ID
- `%a` - Array task ID

### Array Jobs

For array jobs, you might want to include the array task ID:

```bash
#SBATCH --output=/home/%u/slurm-logs/%x/job.out.%A_%a
#SBATCH --error=/home/%u/slurm-logs/%x/job.err.%A_%a
```

Then use:
```bash
slurm-dashboard --log-pattern "{name}/job.{stream}.{id}"
# The dashboard handles array job IDs like 12345_0, 12345_1 automatically
```

## Running on a Cluster

### SSH Tunnel (Recommended)

If you're running the dashboard on a login node:

```bash
# On the cluster
slurm-dashboard --port 5000

# On your local machine (in another terminal)
ssh -L 5000:localhost:5000 user@cluster.example.com

# Then open http://localhost:5000 in your browser
```

### Binding to All Interfaces

To access from other machines on the same network (less secure):

```bash
slurm-dashboard --host 0.0.0.0 --port 5000
```

**Note:** Only do this on trusted networks. The dashboard can cancel and submit jobs.

### Running in Background

```bash
# Using nohup
nohup slurm-dashboard --log-root ~/slurm-logs > dashboard.log 2>&1 &

# Using screen
screen -S dashboard
slurm-dashboard --log-root ~/slurm-logs
# Ctrl+A, D to detach

# Using tmux
tmux new -s dashboard
slurm-dashboard --log-root ~/slurm-logs
# Ctrl+B, D to detach
```

### Restarting a Remote Dashboard

`scripts/restart-remote-dashboard.sh` restarts a dashboard that runs in a
tmux session on a cluster host, and waits until it answers HTTP requests. Run
it from your workstation:

```bash
# Restart only
scripts/restart-remote-dashboard.sh

# Install a git ref, then restart
git fetch origin
scripts/restart-remote-dashboard.sh --ref origin/main
```

With `--ref`, the script builds a wheel locally from that commit, copies it
to the host and installs it offline with `--no-deps`. The cluster does not
need GitHub access. The script records the deployed commit in the
virtualenv and prints the previous one, so you can roll back with
`--ref <previous commit>`.

The defaults match the `agent_3d` deployment: host `agent_3d`, virtualenv
`~/LiteReality-Agent/.venv`, tmux session `ttsd-slurm-dashboard`, port 8787.
Override them with options or `SLURMBOARD_*` environment variables; see
`--help`. The script refuses to start if the port is in use outside the tmux
session. It never submits, cancels or modifies Slurm jobs.

## Views

### Job Table (Default)

The table combines the live `squeue` queue, a selectable 1/7/30/90-day `sacct`
accounting window (7 days by default), and jobs discovered under `--log-root`.
The footer states this scope; pagination does not imply all-time cluster history.
Extend the history window to retrieve older accounting records. Discovered logs
outside the window remain visible with unknown status/timestamps rather than
invented dates. A file's modification time is never treated as a job's queued,
started, or ended time.

Slurm commands run with UTC timestamps. Pending jobs have no actual start time:
the scheduler's predicted start is not displayed as one. Active jobs have no end
time. Missing or unavailable times are shown as `—`. Each array task keeps its
own ID. Failures of Slurm commands are shown explicitly above the table, including
partial results; failed refreshes retain previous data with a stale-data warning.

The queue refreshes every 20 seconds while visible and when returning to the tab.
Refresh preserves sorting, filters, pagination, and the open file viewer without
restarting log streams. Only the displayed page is rendered; sorting/filtering
uses the full returned dataset. Narrow screens use job cards with separate sort
controls. There are no submit/cancel controls on the default page.

The log viewer uses the configured log pattern, initially tails up to 200 KB,
and retains the latest 200,000 characters in the browser. **Follow output** can be
turned off to read earlier lines. Changing file tabs or closing the viewer closes
the previous stream. Missing/empty logs and disconnects are distinct visible states.

### Advanced Tools

The remaining views below are accessed through **Advanced tools** (`/advanced`).

### Timeline View
- Gantt-style visualization of job history
- See job overlaps and gaps
- Filter by state or job name
- Zoom in/out for different time scales

### Pipeline View
- DAG visualization of job dependencies
- Track pipeline progress
- See which jobs are blocking others

### Heatmap View
- GitHub-style activity calendar
- Hourly submission patterns
- Success/failure rate visualization

### Insights View
- Efficiency score (A-F grade)
- Resource usage metrics (CPU/GPU hours)
- Memory and time limit recommendations
- Failure pattern detection
- Peak hours analysis
- Top job types

## Troubleshooting

### Reading long recent-job names

The default job table always wraps the full job name, including names without
spaces on narrow screens. In **Advanced tools**, expand a recent job by clicking
its name or chevron; collapsed rows there remain compact.

### Viewing submitted batch scripts

Use **Script** next to a job's Stdout/Stderr buttons. In **Advanced tools**, use
**script** or **View submitted script** in the job details modal.
This reads the copy submitted to Slurm, not
the current file named in the submission command. Changing or deleting that
source file does not change Slurm's retained copy. The viewer does not submit,
cancel, or modify jobs.

The dashboard first sends `write batch_script JOB_ID -` to the `scontrol`
interactive command reader. Slurm documents this command for interactive use;
the dashboard pipes it to `scontrol --quiet` and reads the controller's retained
copy from stdout without writing a file. It then tries
`sacct --batch-script -j JOB_ID --noheader` as a fallback.
Historical retrieval requires a supported Slurm version and
`AccountingStoreFlags=job_script` to have been enabled when the job ran; the
dashboard does not enable it. Access uses the dashboard process's Slurm identity;
the controller permits only the job owner or a privileged user. Slurm permission
denials, unavailable scripts, unsupported clients, and retrieval errors are
shown explicitly. There is no fallback to a possibly changed source file.

The preview is limited to 200 lines or 1 MiB and labels truncation. It includes
the submitted script (including generated `sbatch --wrap` scripts), but not
command-line resource overrides, the submission environment, or the contents of
files referenced by the script. Jobs submitted without batch scripts have no
script to display. Numeric job IDs, individual array task IDs, and heterogeneous
component IDs are accepted; grouped array ranges are not.
Keep the dashboard behind a trusted SSH tunnel: scripts can
contain sensitive values.

### "No recent logs found"
- Check that `--log-root` points to the correct directory
- Verify `--log-pattern` matches your log file naming
- Ensure log files exist and are readable

### "sacct: command not found"
- The dashboard requires Slurm commands to be in your PATH
- Make sure you're running on a node with Slurm client tools installed

### Jobs not showing up
- Check that `--user` matches the username used when submitting jobs
- Verify jobs are in sacct history: `sacct -u $USER --starttime=now-7days`

### Log streaming not working
- Ensure the log files are accessible from the node running the dashboard
- Check file permissions on the log directory

## Development

```bash
# Install in development mode
pip install -e ".[dev]"

# Run with debug mode
FLASK_DEBUG=1 slurm-dashboard

# Run tests
pytest

# Run frontend regression tests (requires Node.js 18+; no npm dependencies)
node --test tests/dashboard_*.test.cjs
```

## License

MIT
