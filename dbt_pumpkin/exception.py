class PumpkinError(Exception):
    pass

class NotRootRelativePathError(PumpkinError):
    def __init__(self, resource_name:str, path_template:str):
        msg = f"Project root-relative path required {resource_name}, got {path_template}"
        super().__init__(msg)
