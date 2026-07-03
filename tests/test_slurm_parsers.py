from slurm_dashboard.services.slurm import parse_gpu_count, parse_memory, parse_time_to_seconds


def test_parse_gpu_count_handles_gres_and_tres_formats() -> None:
    assert parse_gpu_count("gpu:2") == 2
    assert parse_gpu_count("gpu:a100:4") == 4
    assert parse_gpu_count("gres/gpu=2") == 2
    assert parse_gpu_count("gres/gpu:a100=4") == 4
    assert parse_gpu_count("billing=10,cpu=4,gres/gpu=2,mem=16G") == 2
    assert parse_gpu_count("") == 0


def test_parse_time_to_seconds_handles_slurm_time_formats() -> None:
    assert parse_time_to_seconds("02:03:04") == 7384
    assert parse_time_to_seconds("1-02:03:04") == 93784
    assert parse_time_to_seconds("00:00:00") == 0
    assert parse_time_to_seconds("") == 0


def test_parse_memory_handles_slurm_units() -> None:
    assert parse_memory("4G") == 4 * 1024**3
    assert parse_memory("1024M") == 1024 * 1024**2
    assert parse_memory("4Gn") == 4 * 1024**3
    assert parse_memory("") == 0
