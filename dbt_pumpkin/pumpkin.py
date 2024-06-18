from dbt.cli.main import dbtRunner, dbtRunnerResult
from dbt.contracts.graph.manifest import Manifest

def parse_manifest(project_dir:str = None, profiles_dir:str = None) -> Manifest:
    # initialize
    dbt = dbtRunner()

    # create CLI args as a list of strings
    args = ["parse"]
    if project_dir:
        args += ["--project-dir", project_dir]
    if profiles_dir:
        args += ["--profiles-dir", profiles_dir]

    # run the command
    res: dbtRunnerResult = dbt.invoke(args)

    # inspect the results
    if not res.success:
        raise res.exception
    
    return res.result
