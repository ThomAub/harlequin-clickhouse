from __future__ import annotations

from harlequin.options import (
    FlagOption,  # noqa
    ListOption,  # noqa
    PathOption,  # noqa
    SelectOption,  # noqa
    TextOption,
)

# All configuration options for the ClickHouse adapter
# are from [clickhouse_driver clickhouse_driver.connection.Connection](https://clickhouse-driver.readthedocs.io/en/latest/api.html#connection)
host = TextOption(
    name="host",
    description=(
        "Specifies the host name of the machine on which the server is running. "
        "If the value begins with a slash, it is used as the directory for the "
        "Unix-domain socket."
    ),
    short_decls=["-h"],
    default="localhost",
)


port = TextOption(
    name="port",
    description=(
        "Port number to connect to at the server host, or socket file name extension for Unix-domain connections."
    ),
    short_decls=["-p"],
    default="9000",
)


database = TextOption(
    name="database",
    description=("The database name to use when connecting with the ClickHouse server."),
    short_decls=["-d"],
    default="default",
)


user = TextOption(
    name="user",
    description=("ClickHouse user name to connect as."),
    short_decls=["-u", "--username", "-U"],
)


password = TextOption(
    name="password",
    description=("Password to be used if the server demands password authentication."),
)


def _int_validator(s: str | None) -> tuple[bool, str]:
    if s is None:
        return True, ""
    try:
        _ = int(s)
    except ValueError:
        return False, f"Cannot convert {s} to an int!"
    else:
        return True, ""


def _bool_validator(s: str | None) -> tuple[bool, str]:
    if s is None:
        return True, ""
    if s.lower() not in ["true", "false"]:
        return False, f"{s} is not a valid boolean value!"
    return True, ""


connect_timeout = TextOption(
    name="connect_timeout",
    description=("Maximum time to wait while connecting. Defaults 10 seconds."),
    validator=_int_validator,
    default="10",
)

send_receive_timeout = TextOption(
    name="send_receive_timeout",
    description=("Timeout for sending and receiving data. Defaults to 300 seconds."),
    validator=_int_validator,
    default="300",
)

secure = TextOption(
    name="secure",
    description=("Establish a secure connection. Defaults to False."),
    short_decls=["-s"],
    validator=_bool_validator,
    default="False",
)

verify_ssl = TextOption(
    name="verify",
    description=(
        "Specifies whether a certificate is required and whether it will be "
        "validated after connection. Defaults to True."
    ),
    validator=_bool_validator,
    default="True",
)

CLICKHOUSE_OPTIONS = [
    host,
    port,
    database,
    user,
    password,
    connect_timeout,
    send_receive_timeout,
    secure,
    verify_ssl,
]
