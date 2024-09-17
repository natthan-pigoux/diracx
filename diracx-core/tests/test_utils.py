from __future__ import annotations

import fcntl
from multiprocessing import Process
from pathlib import Path
from tempfile import NamedTemporaryFile

import pytest

from diracx.core.models import TokenResponse
from diracx.core.utils import dotenv_files_from_environment, write_credentials


def test_dotenv_files_from_environment(monkeypatch):
    monkeypatch.setattr("os.environ", {})
    assert dotenv_files_from_environment("TEST_PREFIX") == []

    monkeypatch.setattr("os.environ", {"TEST_PREFIX": "/a"})
    assert dotenv_files_from_environment("TEST_PREFIX") == ["/a"]

    monkeypatch.setattr("os.environ", {"TEST_PREFIX": "/a", "TEST_PREFIX_1": "/b"})
    assert dotenv_files_from_environment("TEST_PREFIX") == ["/a", "/b"]

    monkeypatch.setattr(
        "os.environ",
        {"TEST_PREFIX_2": "/c", "TEST_PREFIX": "/a", "TEST_PREFIX_1": "/b"},
    )
    assert dotenv_files_from_environment("TEST_PREFIX") == ["/a", "/b", "/c"]

    monkeypatch.setattr(
        "os.environ",
        {"TEST_PREFIX_2a": "/c", "TEST_PREFIX": "/a", "TEST_PREFIX_1": "/b"},
    )
    assert dotenv_files_from_environment("TEST_PREFIX") == ["/a", "/b"]


def read_lock_file(file_path):
    with open(file_path, "r") as f:
        print("Trying to read the file")
        fcntl.flock(f, fcntl.LOCK_NB)
        f.read()


def write_lock_file(file_path):
    with open(file_path, "a") as f:
        print("Trying to write the file")
        fcntl.flock(f, fcntl.LOCK_NB)
        f.write("Hello")


def test_write_credentials_is_locking_file():
    """Test that the refresh token cannot be opened while some process write it."""
    token_location = Path(NamedTemporaryFile().name)
    token_response = {
        "access_token": "test",
        "expires_in": 10,
        "token_type": "Bearer",
        "refresh_token": "test",
    }
    proc_write = Process(
        target=write_credentials(
            token_response=TokenResponse(**token_response), location=token_location
        )
    )
    with pytest.raises(OSError) as exec_info:
        proc_read = Process(target=read_lock_file(token_location))

        proc_write.start()
        proc_read.start()

        proc_write.join()
        proc_read.join()

    assert isinstance(exec_info.value, OSError) is False


def test_read_creadentials_is_locking_file():
    token_location = Path(NamedTemporaryFile().name)
    token_response = {
        "access_token": "test",
        "expires_in": 10,
        "token_type": "Bearer",
        "refresh_token": "test",
    }
    proc_read = Process(
        target=read_credentials(
            token_response=TokenResponse(**token_response), location=token_location
        )
    )
    with pytest.raises(OSError) as exec_info:
        proc_write = Process(target=write_lock_file(token_location))

        proc_write.start()
        proc_read.start()

        proc_write.join()
        proc_read.join()

    assert isinstance(exec_info.value, OSError) is False
