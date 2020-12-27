import shlex
import os
import subprocess
import time

import psycopg2
import pytest

NAME = "test-streamz-postgres"


def stop(fail=False):
    cmd = shlex.split(f"docker stop {NAME}")
    try:
        subprocess.check_call(cmd)
    except subprocess.SubprocessError:
        if fail:
            raise


@pytest.fixture(scope="session")
def pg():
    cwd = os.path.dirname(os.path.abspath(__file__))
    cmd = shlex.split(
        f"docker run -d --rm --name {NAME} -p 5432:5432 "
        f"-v {cwd}/postgresql/postgresql.conf:/etc/postgresql/postgresql.conf "
        "-e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=test postgres "
        "-c 'config_file=/etc/postgresql/postgresql.conf'"
    )
    subprocess.check_call(cmd, cwd=cwd)
    params = dict(host="localhost", dbname="postgres", user="postgres", password="test")
    for _ in range(30):
        try:
            psycopg2.connect(**params)
            break
        except psycopg2.OperationalError:
            time.sleep(1)
    yield params
    stop()
