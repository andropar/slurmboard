from slurm_dashboard.config import LogPattern
from slurm_dashboard.services import parsing
from slurm_dashboard.services.logs import expand_filename_pattern
from slurm_dashboard.services.slurm import parse_memory_kb, valid_job_id


def test_time_formats_including_short_total_cpu():
    assert parsing.parse_time_to_seconds("02:03.500") == 123.5  # sacct TotalCPU under an hour
    assert parsing.parse_time_to_seconds("1-02:03:04") == 93784
    assert parsing.parse_time_to_seconds("UNLIMITED") == 0
    assert parsing.parse_time_to_seconds("garbage") == 0


def test_requested_memory_resolves_per_cpu_and_per_node():
    gib = 1024**3
    assert parsing.requested_memory_bytes("4Gc", cpus=8, nodes=1) == 32 * gib
    assert parsing.requested_memory_bytes("4Gn", cpus=8, nodes=2) == 8 * gib
    assert parsing.requested_memory_bytes("4G", cpus=8, nodes=2) == 4 * gib
    assert parse_memory_kb("2048") == 2048 * 1024
    assert parse_memory_kb("2048K") == 2048 * 1024


def test_gpu_count_does_not_double_count_typed_tres():
    assert parsing.parse_gpu_count("cpu=4,gres/gpu=2,gres/gpu:a100=2") == 2


def test_states():
    assert parsing.normalize_state("CANCELLED by 1000") == "CANCELLED"
    assert parsing.state_category("OUT_OF_MEMORY") == "failed"
    assert parsing.state_category("TIMEOUT") == "timeout"
    assert parsing.state_category("COMPLETING") == "running"
    assert parsing.is_active_state("PENDING") and not parsing.is_active_state("FAILED")


def test_job_id_handling():
    assert parsing.split_job_id("123_4.batch") == ("123_4", "batch")
    assert parsing.split_job_id("123_[1-5%2]") == ("123_[1-5%2]", None)
    assert valid_job_id("123") and valid_job_id("123_4") and valid_job_id("123_[1-5%2]")
    assert not valid_job_id("123; rm -rf ~") and not valid_job_id("../etc") and not valid_job_id("")


def test_dependencies():
    assert parsing.parse_dependencies("afterok:123:456(unfulfilled),afterany:789") == [
        ("123", "afterok"),
        ("456", "afterok"),
        ("789", "afterany"),
    ]
    assert parsing.parse_dependencies("(null)") == []


def test_scontrol_values_with_spaces():
    info = parsing.parse_scontrol_job(
        "JobId=7 JobName=my job\n"
        "   Command=/home/u/run me.sh --flag=1\n"
        "   WorkDir=/home/u/dir with space\n"
    )
    assert info["JobName"] == "my job"
    assert info["Command"] == "/home/u/run me.sh --flag=1"
    assert info["WorkDir"] == "/home/u/dir with space"


def test_timestamp_gets_utc_offset():
    stamp = parsing.parse_timestamp("2024-01-15T10:30:00")
    assert stamp.startswith("2024-01-15T10:30:00") and len(stamp) > 19
    assert parsing.parse_timestamp("Unknown") is None


def test_filename_pattern_expansion():
    job = {"id": "100_7", "raw_id": "107", "name": "train", "user": "me", "nodelist": "gpu[03-04]"}
    assert expand_filename_pattern("%x-%A_%a-%j.out", job) == "train-100_7-107.out"
    assert expand_filename_pattern("%u/%N/%5j.log", job) == "me/gpu03/00107.log"
    assert expand_filename_pattern("slurm-%A.out", {"id": "55"}) == "slurm-55.out"


def test_log_pattern_matches_array_task_ids(tmp_path):
    pattern = LogPattern("{name}-{id}.{stream}")
    info = pattern.extract_job_info(tmp_path, tmp_path / "my-job-12_3.err")
    assert info == {"name": "my-job", "id": "12_3", "stream": "err"}
