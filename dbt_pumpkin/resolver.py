from __future__ import annotations

from pathlib import Path

from dbt_pumpkin.exception import NotRootRelativePathError


class PathResolver:
    def resolve(self, path_template: str, resource_name: str, resource_path: Path | None = None) -> Path:
        """
        Resolves path template to root-relative path.

        Supports evaluations of {name} and {parent} keys.
        """
        require_root_relative = resource_path is None
        is_root_relative = path_template.startswith("/")

        if require_root_relative and not is_root_relative:
            raise NotRootRelativePathError(resource_name, path_template)

        params = {"name": resource_name}
        if resource_path:
            params["parent"] = resource_path.parent.name

        resolved = path_template
        for name, value in params.items():
            resolved = resolved.replace("{" + name + "}", value)

        if is_root_relative:
            return Path(resolved[1:])

        return resource_path.parent / resolved
