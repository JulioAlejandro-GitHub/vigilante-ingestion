from app.capture.frame_sampler import FrameSampler


def test_frame_sampler_generates_deterministic_timestamps() -> None:
    sampler = FrameSampler(capture_fps=2, source_fps=10)

    samples = list(sampler.iter_samples(duration_seconds=2, max_frames=None))

    assert [sample.timestamp_seconds for sample in samples] == [0, 0.5, 1.0, 1.5]
    assert [sample.source_frame_index for sample in samples] == [0, 5, 10, 15]


def test_frame_sampler_honors_max_frames() -> None:
    sampler = FrameSampler(capture_fps=5, source_fps=10)

    samples = list(sampler.iter_samples(duration_seconds=10, max_frames=3))

    assert len(samples) == 3
    assert samples[-1].timestamp_seconds == 0.4

