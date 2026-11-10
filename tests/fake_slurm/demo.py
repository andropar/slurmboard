"""Create a demo cluster state (jobs + log files) for the fake Slurm CLI."""

from __future__ import annotations

import getpass
import json
import random
import sys
import time
from pathlib import Path

SCRIPT = "#!/bin/bash\n#SBATCH --job-name={name}\n#SBATCH --time=04:00:00\n\npython train.py --config {name}.yaml\n"


def build(root: Path, me: str = "", seed: int = 0, history_jobs: int = 40) -> Path:
    """Write state.json, scripts and logs under `root`; return the state path.

    `history_jobs` finished jobs are spread four hours apart, so 40 cover about a week.
    """
    me = me or getpass.getuser()
    rng = random.Random(seed)
    now = time.time()
    logs = root / "logs"
    work = root / "project"
    work.mkdir(parents=True, exist_ok=True)
    jobs = []

    def add(job_id, name, state, user=me, age_h=0.0, run_h=1.0, **extra):
        log_dir = logs / name
        log_dir.mkdir(parents=True, exist_ok=True)
        script = work / f"{name}.sh"
        script.write_text(SCRIPT.format(name=name))
        job = {
            "id": job_id,
            "name": name,
            "user": user,
            "state": state,
            "partition": "gpu",
            "cpus": 8,
            "nodes": 1,
            "req_mem": "32G",
            "timelimit_min": 240,
            "tres": "billing=8,cpu=8,gres/gpu=1,mem=32G,node=1",
            "gres": "gres/gpu:1",
            "work_dir": str(work),
            "submit_line": f"sbatch {script.name}",
            "command": str(script),
            "script": script.read_text(),
            "submit": now - (age_h + run_h) * 3600 - 300,
            "stdout": str(logs / "%x" / "job.out.%j"),
            "stderr": str(logs / "%x" / "job.err.%j"),
            "stdout_path": str(log_dir / f"job.out.{job_id}"),
            "stderr_path": str(log_dir / f"job.err.{job_id}"),
        }
        if state != "PENDING":
            job["start"] = now - (age_h + run_h) * 3600
            job["total_cpu_s"] = int(run_h * 3600 * 8 * rng.uniform(0.3, 0.95))
            job["max_rss_kb"] = int(32 * 1024 * 1024 * rng.uniform(0.1, 0.9))
        if state not in ("RUNNING", "PENDING"):
            job["end"] = now - age_h * 3600
        job.update(extra)
        jobs.append(job)
        if state != "PENDING" and user == me:
            out = [
                f"[{i:04d}] epoch {i // 10} step {i} loss={2.0 / (i + 1):.4f}" for i in range(120)
            ]
            err = []
            if extra.get("warn"):
                out.insert(
                    5, "/site-packages/torch/nn/modules.py:12: UserWarning: synthetic warning"
                )
            if state == "FAILED":
                err = [
                    "Traceback (most recent call last):",
                    '  File "train.py", line 42, in <module>',
                    "    main()",
                    "RuntimeError: CUDA error: device-side assert triggered",
                ]
            if state == "OUT_OF_MEMORY":
                err = [
                    "slurmstepd: error: Detected 1 oom-kill event(s). Some of your processes may have been killed."
                ]
            if state == "TIMEOUT":
                err = [f"slurmstepd: error: *** JOB {job_id} CANCELLED DUE TO TIME LIMIT ***"]
            (log_dir / f"job.out.{job_id}").write_text("\n".join(out) + "\n")
            (log_dir / f"job.err.{job_id}").write_text("\n".join(err) + ("\n" if err else ""))
        return job

    add("5001", "train_resnet", "RUNNING", run_h=2.5, warn=True)
    add("5002", "train_vit", "RUNNING", run_h=0.4)
    add(
        "5003",
        "eval_resnet",
        "PENDING",
        dependency="afterok:5001(unfulfilled)",
        reason="Dependency",
    )
    add("5004", "report", "PENDING", dependency="afterok:5003(unfulfilled)", reason="Dependency")
    add("5005", "sweep_lr", "PENDING", reason="Priority", expected_start=now + 1800)
    add("5010_1", "sweep_seed", "RUNNING", run_h=0.2, raw_id="5011")
    add("5010_2", "sweep_seed", "RUNNING", run_h=0.2, raw_id="5012")
    # Tasks that have not started are one condensed queue entry, as squeue prints them.
    add("5010_[3-8%2]", "sweep_seed", "PENDING", reason="JobArrayTaskLimit", raw_id="5010")
    add("6001", "simulate", "RUNNING", user="alice", run_h=5)
    add("6002", "align_reads", "PENDING", user="bob", reason="Resources")
    states = ["COMPLETED"] * 6 + ["FAILED", "TIMEOUT", "OUT_OF_MEMORY", "CANCELLED"]
    for idx in range(history_jobs):
        state = rng.choice(states)
        codes = {"FAILED": "1:0", "OUT_OF_MEMORY": "0:125", "TIMEOUT": "0:0", "CANCELLED": "0:15"}
        add(
            str(4990 - idx),
            rng.choice(["train_resnet", "train_vit", "preprocess", "eval_resnet"]),
            state,
            age_h=idx * 4 + rng.uniform(0.2, 3),
            run_h=4.0 if state == "TIMEOUT" else rng.uniform(0.1, 3),
            exit_code=codes.get(state, "0:0"),
            partition=rng.choice(["gpu", "gpu", "cpu"]),
        )
    for idx in range(5):
        add(str(3000 + idx), "simulate", "COMPLETED", user="alice", age_h=idx * 7 + 1, run_h=2)

    # A job with logs on disk that accounting no longer knows.
    old = logs / "legacy_run"
    old.mkdir(parents=True, exist_ok=True)
    (old / "job.out.1234").write_text("old output\n")

    state_path = root / "state.json"
    state_path.write_text(
        json.dumps(
            {"me": me, "next_id": 9000, "partitions": ["gpu*", "cpu"], "jobs": jobs}, indent=1
        )
    )
    return state_path


if __name__ == "__main__":
    target = Path(sys.argv[1] if len(sys.argv) > 1 else "demo-cluster").resolve()
    print(build(target))
