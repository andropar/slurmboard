# slurmboard

A lightweight web dashboard for monitoring Slurm jobs.

## Features

- Real-time job monitoring with live log streaming
- Multiple view modes: Table, Timeline, Pipeline DAG, Heatmap
- Job submission and resubmission with templates
- Advanced filtering and saved searches
- Batch operations (cancel, resubmit, export)
- Smart log analysis with error detection
- Resource usage tracking and efficiency insights
- Cost/allocation tracking
- Customizable dashboard layouts

## Installation

```bash
pip install -e .
```

## Usage

```bash
slurm-dashboard --log-root /path/to/logs --user $USER
```

## License

MIT
