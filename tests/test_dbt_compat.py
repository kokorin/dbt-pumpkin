import logging

from dbt_pumpkin.dbt_compat import dbtRunner, dbtRunnerResult, hijack_dbt_logs


def test_hijack_dbt_logs(caplog):
    hijack_dbt_logs()
    args = ["debug", "--project-dir", "tests/my_pumpkin", "--profiles-dir", "tests/my_pumpkin"]
    with caplog.at_level(logging.INFO):
        res: dbtRunnerResult = dbtRunner().invoke(args)

        assert res.success
        assert not res.exception
        assert res.result is True
        assert "All checks passed!" in caplog.text
