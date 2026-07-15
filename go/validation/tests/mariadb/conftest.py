# Copyright (c) 2025 ADBC Drivers Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os

import pytest
from tests import mariadb


def pytest_generate_tests(metafunc) -> None:
    quirks = mariadb.get_quirks(metafunc.config.getoption("vendor_version"))
    if quirks.name != "mariadb":
        pytest.skip()
        return

    metafunc.parametrize(
        "driver",
        [f"{quirks.name}:{quirks.short_version}"],
        scope="module",
        indirect=["driver"],
    )


@pytest.fixture(scope="session")
def mariadb_host() -> str:
    """MariaDB host. Example: MARIADB_HOST=localhost"""
    return os.environ.get("MARIADB_HOST", "localhost")


@pytest.fixture(scope="session")
def mariadb_port() -> str:
    """MariaDB port. Example: MARIADB_PORT=3307"""
    return os.environ.get("MARIADB_PORT", "3306")


@pytest.fixture(scope="session")
def mariadb_database() -> str:
    """MariaDB database name. Example: MARIADB_DATABASE=db"""
    return os.environ.get("MARIADB_DATABASE", "db")


@pytest.fixture(scope="session")
def creds() -> tuple[str, str]:
    """MariaDB credentials. Example: MARIADB_USERNAME=my MARIADB_PASSWORD=password"""
    username = os.environ.get("MARIADB_USERNAME", "my")
    password = os.environ.get("MARIADB_PASSWORD", "password")
    return username, password


@pytest.fixture(scope="session")
def uri(mariadb_host: str, mariadb_port: str, mariadb_database: str) -> str:
    """
    Constructs a clean MariaDB URI without credentials.
    Example: mariadb://localhost:3306/db
    """
    return f"mariadb://{mariadb_host}:{mariadb_port}/{mariadb_database}"


@pytest.fixture(scope="session")
def dsn(
    creds: tuple[str, str], mariadb_host: str, mariadb_port: str, mariadb_database: str
) -> str:
    """
    Constructs a MariaDB DSN in Go MariaDB Driver's native format.
    Example: my:password@tcp(localhost:3306)/db
    """
    username, password = creds
    return f"{username}:{password}@tcp({mariadb_host}:{mariadb_port})/{mariadb_database}"


@pytest.fixture(scope="session")
def mariadb_socket_path() -> str:
    """
    Returns the path to MariaDB Unix socket file.
    Requires a local MariaDB server running with Unix socket enabled.
    Example: MARIADB_SOCKET_PATH=/tmp/mariadb.sock
    """
    path = os.environ.get("MARIADB_SOCKET_PATH")
    if not path:
        pytest.skip("Must set MARIADB_SOCKET_PATH (e.g., /tmp/mariadb.sock)")
    return path
