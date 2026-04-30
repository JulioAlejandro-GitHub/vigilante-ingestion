from __future__ import annotations

from dataclasses import dataclass

from app.services.active_camera_loader import ActiveCamera


@dataclass(frozen=True)
class ActiveCameraDiff:
    start: tuple[ActiveCamera, ...]
    stop: tuple[ActiveCamera, ...]
    restart: tuple[ActiveCamera, ...]
    unchanged: tuple[ActiveCamera, ...]

    @property
    def change_count(self) -> int:
        return len(self.start) + len(self.stop) + len(self.restart)


def diff_active_cameras(
    current: dict[str, ActiveCamera],
    desired: dict[str, ActiveCamera],
) -> ActiveCameraDiff:
    start: list[ActiveCamera] = []
    stop: list[ActiveCamera] = []
    restart: list[ActiveCamera] = []
    unchanged: list[ActiveCamera] = []

    for camera_id, camera in desired.items():
        existing = current.get(camera_id)
        if existing is None:
            start.append(camera)
        elif existing.config_version_hash != camera.config_version_hash:
            restart.append(camera)
        else:
            unchanged.append(camera)

    for camera_id, camera in current.items():
        if camera_id not in desired:
            stop.append(camera)

    return ActiveCameraDiff(
        start=tuple(start),
        stop=tuple(stop),
        restart=tuple(restart),
        unchanged=tuple(unchanged),
    )
