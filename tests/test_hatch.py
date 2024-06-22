import os
import sys

import dbt
import dbt.version


def test_expected_python_version():
    sys_version = str(sys.version_info.major) + "." + str(sys.version_info.minor)
    expected_version = os.environ.get("EXPECTED_PYTHON_VERSION")
    assert sys_version == expected_version


def test_expected_dbt_version():
    sys_version = dbt.version.get_installed_version().major + "." + dbt.version.get_installed_version().minor
    expected_version = os.environ.get("EXPECTED_DBT_VERSION")
    assert sys_version == expected_version
