"""A small stand-in for the Slurm CLI, driven by a JSON state file.

Used by the test suite and for running the dashboard without a cluster:

    python tests/fake_slurm/demo.py /tmp/demo     # writes state + log files
    PATH=tests/fake_slurm:$PATH FAKE_SLURM_STATE=/tmp/demo/state.json \
        slurm-dashboard --log-root /tmp/demo/logs

Only the options the dashboard uses are implemented.
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime

ACTIVE = {"RUNNING", "PENDING"}
SACCT_FIELDS = [
    "JobID",
    "JobIDRaw",
    "JobName",
    "User",
    "State",
    "ExitCode",
    "Submit",
    "Start",
    "End",
    "ElapsedRaw",
    "TimelimitRaw",
    "Partition",
    "AllocCPUS",
    "NNodes",
    "ReqMem",
    "MaxRSS",
    "TotalCPU",
    "NodeList",
    "WorkDir",
    "AllocTRES",
    "SubmitLine",
    "StdOut",
    "StdErr",
]


def load():
    with open(os.environ["FAKE_SLURM_STATE"]) as handle:
        return json.load(handle)


def save(state):
    path = os.environ["FAKE_SLURM_STATE"]
    with open(path + ".tmp", "w") as handle:
        json.dump(state, handle, indent=1)
    os.replace(path + ".tmp", path)


def stamp(epoch, missing="Unknown"):
    return datetime.fromtimestamp(epoch).strftime("%Y-%m-%dT%H:%M:%S") if epoch else missing


def clock(seconds):
    seconds = int(seconds)
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    mins, secs = divmod(rem, 60)
    if days:
        return f"{days}-{hours:02d}:{mins:02d}:{secs:02d}"
    return f"{hours}:{mins:02d}:{secs:02d}" if hours else f"{mins}:{secs:02d}"


def elapsed(job):
    if not job.get("start"):
        return 0
    return int((job.get("end") or time.time()) - job["start"])


def option(args, *names, default=None):
    """Value of --name=value, --name value or -n value."""
    for idx, arg in enumerate(args):
        for name in names:
            if arg == name and idx + 1 < len(args):
                return args[idx + 1]
            if name.startswith("--") and arg.startswith(name + "="):
                return arg.split("=", 1)[1]
    return default


def fail_if_requested(state, tool):
    if tool in state.get("fail", []):
        sys.stderr.write(
            f"{tool}: error: slurm_load_jobs error: Unable to contact slurm controller\n"
        )
        sys.exit(1)
    delay = state.get("delay", {}).get(tool)
    if delay:
        time.sleep(delay)


def squeue(args, state):
    fmt = option(args, "-o", "--format", default="%i %j")
    user = option(args, "-u", "--user")
    for job in state["jobs"]:
        if job["state"] not in ACTIVE or (user and job["user"] != user):
            continue
        pending = job["state"] == "PENDING"
        values = {
            "i": job["id"],
            "j": job["name"],
            "u": job["user"],
            "T": job["state"],
            "M": clock(elapsed(job)),
            "l": clock(job.get("timelimit_min", 60) * 60),
            "D": str(job.get("nodes", 1)),
            "r": job.get("reason", "None"),
            "N": "" if pending else job.get("nodelist", "node01"),
            "P": job["partition"],
            "S": stamp(job.get("expected_start") if pending else job.get("start"), "N/A"),
            "V": stamp(job.get("submit"), "N/A"),
            "E": job.get("dependency") or "(null)",
            "b": job.get("gres", "N/A"),
            "C": str(job.get("cpus", 1)),
            "F": job["id"].split("_")[0],
            "K": job["id"].split("_")[1] if "_" in job["id"] else "N/A",
            "Q": str(job.get("priority", 1000)),
        }
        line = fmt
        for key, value in values.items():
            line = line.replace("%" + key, value)
        print(line)


def sacct(args, state):
    if "--helpformat" in args:
        print(" ".join(SACCT_FIELDS))
        return
    job_id = option(args, "-j", "--jobs")
    if "--batch-script" in args or "-B" in args:
        job = next(
            (j for j in state["jobs"] if j.get("raw_id", j["id"]) == job_id or j["id"] == job_id),
            None,
        )
        if job is None or not job.get("script"):
            sys.stderr.write("sacct: error: No job script available\n")
            sys.exit(1)
        print(f"Batch Script for {job_id}")
        print("-" * 80)
        sys.stdout.write(job["script"])
        return
    fields = option(args, "--format", default="JobID").split(",")
    sep = option(args, "--delimiter", default="|")
    user = option(args, "-u", "--user")
    start = option(args, "--starttime", "-S")
    cutoff = 0
    if start and start.startswith("now-") and start.endswith("days"):
        cutoff = time.time() - int(start[4:-4]) * 86400
    for job in state["jobs"]:
        if job_id and job["id"] != job_id:
            continue
        if not job_id and user and job["user"] != user:
            continue
        if not job_id and job.get("end") and job["end"] < cutoff:
            continue
        seconds = elapsed(job)
        cancelled = job["state"] == "CANCELLED"
        row = {
            "JobID": job["id"],
            "JobIDRaw": job.get("raw_id", job["id"]),
            "JobName": job["name"],
            "User": job["user"],
            "State": "CANCELLED by 1000" if cancelled else job["state"],
            "ExitCode": job.get("exit_code", "0:0"),
            "Submit": stamp(job.get("submit")),
            "Start": stamp(job.get("start"), "None"),
            "End": stamp(job.get("end")),
            "ElapsedRaw": str(seconds),
            "TimelimitRaw": str(job.get("timelimit_min", 60)),
            "Partition": job["partition"],
            "AllocCPUS": str(job.get("cpus", 1)),
            "NNodes": str(job.get("nodes", 1)),
            "ReqMem": job.get("req_mem", "4G"),
            "MaxRSS": "",
            "TotalCPU": clock(job.get("total_cpu_s", 0)) + ".000",
            "NodeList": job.get("nodelist", "node01"),
            "WorkDir": job.get("work_dir", ""),
            "AllocTRES": job.get("tres", ""),
            "SubmitLine": job.get("submit_line", ""),
            "StdOut": job.get("stdout", ""),
            "StdErr": job.get("stderr", ""),
        }
        print(sep.join(row.get(f, "") for f in fields))
        if job.get("start"):
            step = dict.fromkeys(fields, "")
            step.update(
                {
                    "JobID": job["id"] + ".batch",
                    "JobIDRaw": row["JobIDRaw"] + ".batch",
                    "JobName": "batch",
                    "State": row["State"].split()[0],
                    "MaxRSS": f"{job.get('max_rss_kb', 0)}K",
                    "ElapsedRaw": str(seconds),
                }
            )
            print(sep.join(step.get(f, "") for f in fields))


def scontrol(args, state):
    if args[:2] == ["show", "job"]:
        job = next((j for j in state["jobs"] if j["id"] == args[2] and j["state"] in ACTIVE), None)
        if job is None:
            sys.stderr.write("slurm_load_jobs error: Invalid job id specified\n")
            sys.exit(1)
        print(f"JobId={job['id']} JobName={job['name']}")
        print(f"   UserId={job['user']}(1000) GroupId=users(100)")
        print(f"   JobState={job['state']} Reason={job.get('reason', 'None')} Dependency=(null)")
        print(
            f"   RunTime={clock(elapsed(job))} TimeLimit={clock(job.get('timelimit_min', 60) * 60)}"
        )
        print(f"   Partition={job['partition']} NumCPUs={job.get('cpus', 1)}")
        print(f"   Command={job.get('command', '(null)')}")
        print(f"   WorkDir={job.get('work_dir', '')}")
        print(f"   StdErr={job.get('stderr_path', '')}")
        print(f"   StdOut={job.get('stdout_path', '')}")
    elif args[:2] == ["write", "batch_script"]:
        job = next((j for j in state["jobs"] if j["id"] == args[2] and j["state"] in ACTIVE), None)
        if job is None or not job.get("script"):
            sys.stderr.write("job script retrieval failed: Invalid job id specified\n")
            sys.exit(1)
        sys.stdout.write(job["script"])


def scancel(args, state):
    job = next((j for j in state["jobs"] if j["id"] == args[0] and j["state"] in ACTIVE), None)
    if job is None:
        sys.stderr.write(
            f"scancel: error: Kill job error on job id {args[0]}: Invalid job id specified\n"
        )
        sys.exit(1)
    job.update(state="CANCELLED", end=time.time())
    save(state)


def sbatch(args, state):
    script_path = next((a for a in args if not a.startswith("-")), None)
    script = open(script_path).read() if script_path else sys.stdin.read()
    if not script.startswith("#!"):
        sys.stderr.write("sbatch: error: This does not look like a batch script.\n")
        sys.exit(1)
    new_id = str(state.get("next_id", 9000))
    state["next_id"] = int(new_id) + 1
    work_dir = option(args, "--chdir", default=os.getcwd())
    state["jobs"].append(
        {
            "id": new_id,
            "name": option(args, "--job-name", default=os.path.basename(script_path or "sbatch")),
            "user": state.get("me", "tester"),
            "state": "PENDING",
            "reason": "Priority",
            "partition": option(args, "--partition", default="cpu"),
            "submit": time.time(),
            "cpus": int(option(args, "--cpus-per-task", default="1")),
            "req_mem": option(args, "--mem", default="4G"),
            "work_dir": work_dir,
            "script": script,
            "command": script_path or "(null)",
            "dependency": option(args, "--dependency"),
            "submit_line": "sbatch " + " ".join(args),
        }
    )
    save(state)
    print(new_id if "--parsable" in args else f"Submitted batch job {new_id}")


def sinfo(args, state):
    for name in state.get("partitions", ["cpu*", "gpu"]):
        print(name)


def sstat(args, state):
    job_id = option(args, "-j")
    job = next((j for j in state["jobs"] if j["id"] == job_id and j["state"] == "RUNNING"), None)
    if job is None:
        sys.stderr.write("sstat: error: no steps running for job\n")
        sys.exit(1)
    print(
        f"{job_id}.batch|00:10:00|{job.get('max_rss_kb', 0) // 2}K|{job.get('max_rss_kb', 0)}K|900000K|1"
    )


TOOLS = {
    "squeue": squeue,
    "sacct": sacct,
    "scontrol": scontrol,
    "scancel": scancel,
    "sbatch": sbatch,
    "sinfo": sinfo,
    "sstat": sstat,
}

if __name__ == "__main__":
    tool, arguments = sys.argv[1], sys.argv[2:]
    current = load()
    fail_if_requested(current, tool)
    TOOLS[tool](arguments, current)
