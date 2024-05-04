import sys

import pytest
from harlequin.adapter import HarlequinAdapter, HarlequinConnection, HarlequinCursor
from harlequin.catalog import Catalog, CatalogItem
from harlequin.exception import HarlequinConnectionError, HarlequinQueryError
from harlequin_clickhouse.adapter import (
    HarlequinClickHouseAdapter,
    HarlequinClickHouseConnection,
    HarlequinClickHouseCursor,
)
from textual_fastdatatable.backend import create_backend
from testcontainers.clickhouse import ClickHouseContainer

if sys.version_info < (3, 10):
    from importlib_metadata import entry_points
else:
    from importlib.metadata import entry_points


def test_plugin_discovery() -> None:
    PLUGIN_NAME = "clickhouse"
    eps = entry_points(group="harlequin.adapter")
    assert eps[PLUGIN_NAME]
    adapter_cls = eps[PLUGIN_NAME].load()
    assert issubclass(adapter_cls, HarlequinAdapter)
    assert adapter_cls == HarlequinClickHouseAdapter


def test_connect() -> None:
    clickhouse = ClickHouseContainer("clickhouse/clickhouse-server:23.4")
    clickhouse.start()
    conn_str = clickhouse.get_connection_url()
    conn = HarlequinClickHouseAdapter(conn_str=(conn_str,)).connect()
    assert isinstance(conn, HarlequinConnection)
    clickhouse.stop()


def test_init_extra_kwargs() -> None:
    clickhouse = ClickHouseContainer("clickhouse/clickhouse-server:23.4")
    clickhouse.start()
    conn_str = clickhouse.get_connection_url()
    conn = HarlequinClickHouseAdapter(conn_str=(conn_str,), foo=1, bar="baz").connect()
    assert isinstance(conn, HarlequinConnection)
    clickhouse.stop()


def test_connect_raises_connection_error() -> None:
    with pytest.raises(HarlequinConnectionError):
        _ = HarlequinClickHouseAdapter(conn_str=("foo",)).connect()


@pytest.fixture(scope="module")
def connection_setup(request):
    clickhouse = ClickHouseContainer("clickhouse/clickhouse-server:23.4")
    clickhouse.start()

    def remove_container():
        clickhouse.stop()

    request.addfinalizer(remove_container)
    conn_str = clickhouse.get_connection_url()
    return HarlequinClickHouseAdapter(conn_str=(conn_str,)).connect()


def test_get_catalog(connection_setup: HarlequinClickHouseConnection) -> None:
    catalog = connection_setup.get_catalog()
    assert isinstance(catalog, Catalog)
    assert catalog.items
    assert isinstance(catalog.items[0], CatalogItem)


def test_execute_ddl(connection_setup: HarlequinClickHouseConnection) -> None:
    cur = connection_setup.execute("CREATE TABLE foo (a Int16) ENGINE = Memory")
    assert cur is None
    connection_setup.execute("DROP TABLE foo")  # some cleanup after test on teardown


def test_execute_select(connection_setup: HarlequinClickHouseConnection) -> None:
    cur = connection_setup.execute("select 1 as a")
    assert isinstance(cur, HarlequinCursor)
    # assert cur.columns() == [("a", "##")]
    assert cur.columns() == [("a", "UInt8")]
    data = cur.fetchall()
    backend = create_backend(data)
    assert backend.column_count == 1
    assert backend.row_count == 1


@pytest.mark.skip(
    reason="ClickHouse does not support duplicate column names in a select statement."
    "DB::Exception: Different expressions with the same alias a"
)
def test_execute_select_dupe_cols(
    connection_setup: HarlequinClickHouseConnection,
) -> None:
    cur = connection_setup.execute("select 1 as a, 2 as a, 3 as a")
    assert isinstance(cur, HarlequinCursor)
    assert len(cur.columns()) == 3
    data = cur.fetchall()
    backend = create_backend(data)
    assert backend.column_count == 3
    assert backend.row_count == 1


def test_set_limit(connection_setup: HarlequinClickHouseConnection) -> None:
    cur = connection_setup.execute(
        "select 1 as a union all select 2 union all select 3"
    )
    assert isinstance(cur, HarlequinCursor)
    cur = cur.set_limit(2)
    assert isinstance(cur, HarlequinCursor)
    data = cur.fetchall()
    backend = create_backend(data)
    assert backend.column_count == 1
    assert backend.row_count == 2


def test_execute_raises_query_error(
    connection_setup: HarlequinClickHouseConnection,
) -> None:
    with pytest.raises(HarlequinQueryError):
        _ = connection_setup.execute("selec;")
