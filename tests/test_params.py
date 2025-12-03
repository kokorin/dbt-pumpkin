from dbt_pumpkin.params import ProjectParams, ResourceParams


def test_project_params_to_args():
    # fmt: off
    assert ProjectParams().to_args() == []
    assert ProjectParams(project_dir="test_project").to_args() == ["--project-dir", "test_project"]
    assert ProjectParams(profiles_dir="test_profiles").to_args() == ["--profiles-dir", "test_profiles"]
    assert (
        ProjectParams(project_dir="test_project", profiles_dir="test_profiles").to_args()
    ) == ["--project-dir", "test_project", "--profiles-dir", "test_profiles"]
    assert (
        ProjectParams(project_dir="test_project", profiles_dir="test_profiles", profile="prof", target="CI").to_args()
    ) == [
        "--project-dir", "test_project",
        "--profiles-dir", "test_profiles",
        "--profile", "prof",
        "--target", "CI",
    ]
    # fmt: on


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
    assert ResourceParams().to_args() == []

    assert ResourceParams(select=["abc"]).to_args() == ["--select", "abc"]
    assert ResourceParams(select=["abc", "def"]).to_args() == ["--select", "abc", "--select", "def"]

    assert ResourceParams(exclude=["abc"]).to_args() == ["--exclude", "abc"]
    assert ResourceParams(exclude=["abc", "def"]).to_args() == ["--exclude", "abc", "--exclude", "def"]

    assert ResourceParams(select=["abc"], exclude=["def"]).to_args() == ["--select", "abc", "--exclude", "def"]
