import shlex
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
    cmd = shlex.split(
        f"docker run -d --rm --name {NAME} -p 5432:5432 "
        "-e POSTGRES_USER=test -e POSTGRES_PASSWORD=test postgres"
    )
    subprocess.check_call(cmd)
    params = dict(host="localhost", dbname="test", user="test", password="test")
    for _ in range(30):
        try:
            psycopg2.connect(**params)
            break
        except psycopg2.OperationalError:
            time.sleep(1)
    yield params
    stop()
