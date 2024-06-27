from __future__ import annotations

import dataclasses
from dataclasses import dataclass


@dataclass(frozen=True)
class ProjectParams:
    project_dir: str | None = None
    profiles_dir: str | None = None
    profile: str | None = None
    target: str | None = None

    def to_args(self) -> list[str]:
        args = []
        if self.project_dir:
            args += ["--project-dir", self.project_dir]
        if self.profiles_dir:
            args += ["--profiles-dir", self.profiles_dir]
        if self.profile:
            args += ["--profile", self.profile]
        if self.target:
            args += ["--target", self.target]

        return args

    def with_project_dir(self, project_dir: str) -> ProjectParams:
        return dataclasses.replace(self, project_dir=project_dir)


@dataclass(frozen=True)
class ResourceParams:
    select: list[str] | None = None
    exclude: list[str] | None = None

    def to_args(self):
        args = []
        for select in self.select or []:
            args += ["--select", select]
        for exclude in self.exclude or []:
            args += ["--exclude", exclude]

        return args
