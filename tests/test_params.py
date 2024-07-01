from dbt_pumpkin.params import ProjectParams, ResourceParams


def test_project_params_to_args():
    assert [] == ProjectParams().to_args()
    assert ["--project-dir", "test_project"] == ProjectParams(project_dir="test_project").to_args()
    assert ["--profiles-dir", "test_profiles"] == ProjectParams(profiles_dir="test_profiles").to_args()
    assert ["--project-dir", "test_project", "--profiles-dir", "test_profiles"] == (
        ProjectParams(project_dir="test_project", profiles_dir="test_profiles").to_args()
    )
    assert [
        "--project-dir",
        "test_project",
        "--profiles-dir",
        "test_profiles",
        "--profile",
        "prof",
        "--target",
        "CI",
    ] == (
        ProjectParams(project_dir="test_project", profiles_dir="test_profiles", profile="prof", target="CI").to_args()
    )


def test_project_params_with_project_dir():
    assert ProjectParams(project_dir="other_project") == ProjectParams().with_project_dir("other_project")
    assert ProjectParams(project_dir="other_project") == (
        ProjectParams(project_dir="test_project").with_project_dir("other_project")
    )
    assert ProjectParams(project_dir="other_project", profiles_dir="test_profiles") == (
        ProjectParams(profiles_dir="test_profiles").with_project_dir("other_project")
    )
    assert ProjectParams(project_dir="other_project", profiles_dir="test_profiles") == (
        ProjectParams(project_dir="test_project", profiles_dir="test_profiles").with_project_dir("other_project")
    )


def test_resource_params_to_args():
    assert [] == ResourceParams().to_args()

    assert ["--select", "abc"] == ResourceParams(select=["abc"]).to_args()
    assert ["--select", "abc", "--select", "def"] == ResourceParams(select=["abc", "def"]).to_args()

    assert ["--exclude", "abc"] == ResourceParams(exclude=["abc"]).to_args()
    assert ["--exclude", "abc", "--exclude", "def"] == ResourceParams(exclude=["abc", "def"]).to_args()

    assert ["--select", "abc", "--exclude", "def"] == ResourceParams(select=["abc"], exclude=["def"]).to_args()
