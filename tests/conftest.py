from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def ffmpeg_available() -> str:
    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg:
        pytest.fail("ffmpeg is required for video ingestion tests")
    return ffmpeg


@pytest.fixture(scope="session")
def ffprobe_available() -> str:
    ffprobe = shutil.which("ffprobe")
    if not ffprobe:
        pytest.fail("ffprobe is required for video ingestion tests")
    return ffprobe


def make_sample_video(
    path: Path,
    *,
    ffmpeg: str,
    duration_seconds: float = 3,
    rate: int = 10,
    size: str = "160x90",
) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    command = [
        ffmpeg,
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        "-f",
        "lavfi",
        "-i",
        f"testsrc=size={size}:rate={rate}",
        "-t",
        str(duration_seconds),
        "-pix_fmt",
        "yuv420p",
        str(path),
    ]
    subprocess.run(command, check=True)
    return path

