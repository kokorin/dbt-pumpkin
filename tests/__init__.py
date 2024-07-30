from dbt.adapters.duckdb.__version__ import version as duckdb_version

if duckdb_version.startswith("1.5."):
    from dbt.adapters.duckdb.connections import (
        Connection,
        ConnectionState,
        DuckDBConnectionManager,
        dbt,
        environments,
        logger,
    )

    """
    Unfortunately dbt-duckdb 1.5.* contains a bug: DuckDB environment is not re-created if credentials have changed.
    See https://github.com/duckdb/dbt-duckdb/issues/203 and https://github.com/duckdb/dbt-duckdb/pull/204
    During dbt-pumpkin testing we create a lot of temporary projects in different temp folders.
    So we have to monkey patch dbt-duckdb in order to test reliably on DBT 1.5.
    Should be removed after decommissioning DBT 1.5 support.
    """

    def open_backported_from_1_6(cls, connection: Connection) -> Connection:
        if connection.state == ConnectionState.OPEN:
            logger.debug("Connection is already open, skipping open.")
            return connection

        credentials = cls.get_credentials(connection.credentials)
        with cls._LOCK:
            try:
                if not cls._ENV or cls._ENV.creds != credentials:
                    cls._ENV = environments.create(credentials)
                connection.handle = cls._ENV.handle()
                connection.state = ConnectionState.OPEN

            except RuntimeError as e:
                logger.debug(f"Got an error when attempting to connect to DuckDB: '{e}'")
                connection.handle = None
                connection.state = ConnectionState.FAIL
                raise dbt.exceptions.FailedToConnectError(str(e))  # noqa:B904

            return connection

    DuckDBConnectionManager.open = classmethod(open_backported_from_1_6)
