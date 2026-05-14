from __future__ import annotations

from typing import Any, Sequence

import clickhouse_connect
from harlequin import HarlequinConnection, HarlequinCursor
from harlequin.exception import HarlequinConnectionError, HarlequinQueryError
from textual_fastdatatable.backend import AutoBackendType

from harlequin_clickhouse.adapter import ClickHouseCatalogMixin


class HarlequinClickHouseHttpCursor(HarlequinCursor):
    def __init__(self, query_result: Any) -> None:
        self._result = query_result
        self._limit: int | None = None

    def columns(self) -> list[tuple[str, str]]:
        return [(name, col_type.name) for name, col_type in zip(self._result.column_names, self._result.column_types)]

    def set_limit(self, limit: int) -> HarlequinClickHouseHttpCursor:
        self._limit = limit
        return self

    def fetchall(self) -> AutoBackendType:
        try:
            rows = self._result.result_rows
            if self._limit is not None:
                return rows[: self._limit]
            return rows
        except Exception as e:
            raise HarlequinQueryError(
                msg=str(e),
                title="Harlequin encountered an error while fetching query results.",
            ) from e


class HarlequinClickHouseHttpConnection(ClickHouseCatalogMixin, HarlequinConnection):
    def __init__(
        self,
        conn_str: Sequence[str],
        *args: Any,
        init_message: str = "Welcome to ClickHouse (HTTP) with harlequin",
        options: dict[str, Any],
    ) -> None:
        self.init_message = init_message
        self.conn_str = conn_str
        try:
            client_kwargs = self._build_client_kwargs(conn_str, options)
            self.client = clickhouse_connect.get_client(**client_kwargs)
            self.client.ping()
        except Exception as e:
            raise HarlequinConnectionError(
                msg=str(e),
                title="Harlequin could not connect to ClickHouse via HTTP (clickhouse-connect).",
            ) from e

    @staticmethod
    def _build_client_kwargs(conn_str: Sequence[str], options: dict[str, Any]) -> dict[str, Any]:
        if len(conn_str) == 1:
            return {"dsn": conn_str[0]}

        kwargs: dict[str, Any] = {}
        mapping = {
            "host": "host",
            "database": "database",
            "password": "password",
            "connect_timeout": "connect_timeout",
            "send_receive_timeout": "send_receive_timeout",
        }
        for src, dst in mapping.items():
            val = options.get(src)
            if val is not None:
                kwargs[dst] = val

        if options.get("user") is not None:
            kwargs["username"] = options["user"]

        if options.get("port") is not None:
            kwargs["port"] = int(options["port"])

        if options.get("secure") is not None:
            kwargs["secure"] = options["secure"].lower() == "true"

        if options.get("verify") is not None:
            kwargs["verify"] = options["verify"].lower() == "true"

        # Disable compression by default — proxies often strip Content-Encoding
        # headers, causing the lz4 decompressor to fail on uncompressed data.
        kwargs.setdefault("compress", False)

        return kwargs

    def execute(self, query: str) -> HarlequinCursor | None:
        try:
            result = self.client.query(query.rstrip("; \t\n"))
        except Exception as e:
            raise HarlequinQueryError(
                msg=str(e),
                title="Harlequin encountered an error while executing your query.",
            ) from e
        if result.column_names:
            return HarlequinClickHouseHttpCursor(result)
        return None

    def _execute_catalog_query(self, sql: str) -> list[tuple]:
        result = self.client.query(sql)
        return [tuple(row) for row in result.result_rows]
