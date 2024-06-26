from pathlib import Path


class PumpkinError(Exception):
    pass


class NotRootRelativePathError(PumpkinError):
    def __init__(self, resource_name: str, path_template: str):
        msg = f"Project root-relative path required for resource {resource_name}, got {path_template}"
        super().__init__(msg)


class ResourceNotFoundError(PumpkinError):
    def __init__(self, name: str, path: Path):
        msg = f"Resource {name} not found at {path}"
        super().__init__(msg)


class PropertyRequiredError(PumpkinError):
    def __init__(self, property_name, unique_id):
        msg = f"Property {property_name} is required for resource {unique_id}"
        super().__init__(msg)


class PropertyNotAllowedError(PumpkinError):
    def __init__(self, property_name, unique_id):
        msg = f"Property  {property_name} is not allowed for resource {unique_id}"
        super().__init__(msg)
