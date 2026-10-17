import os
import asyncpg
import pytest
import pytest_asyncio

from foundation.supplier import SmartConnection
from foundation.pg_task_store import PostgresTaskStore, PostgresStoredSettings


def _load_env_test():
    config = {}
    env_path = os.path.join(os.path.dirname(__file__), 'environment')
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, _, value = line.partition('=')
                config[key.strip()] = value.strip()
    return config


@pytest.fixture(scope="session")
def db_config():
    cfg = _load_env_test()
    return {
        'host':     cfg.get('DB_HOST', 'localhost'),
        'port':     int(cfg.get('DB_PORT', 5432)),
        'database': cfg['DB_NAME'],
        'user':     cfg['DB_USER'],
        'password': cfg.get('DB_PASSWORD', ''),
    }


@pytest_asyncio.fixture(scope="session")
async def pool(db_config):
    p = await asyncpg.create_pool(**db_config)

    schema_dir = os.path.join(os.path.dirname(__file__), '..', 'foundation', 'database')
    schema_files = sorted(f for f in os.listdir(schema_dir) if f.endswith('.sql'))

    async with p.acquire() as conn:
        for filename in schema_files:
            with open(os.path.join(schema_dir, filename)) as fd:
                await conn.execute(fd.read())

    yield p

    async with p.acquire() as conn:
        await conn.execute('DROP SCHEMA IF EXISTS pavao CASCADE')

    await p.close()


@pytest_asyncio.fixture
async def task_store(pool):
    return PostgresTaskStore(pool)


@pytest_asyncio.fixture
async def stored_settings(pool):
    return PostgresStoredSettings(pool)


@pytest_asyncio.fixture
async def connection(pool):
    raw = await pool.acquire()
    yield SmartConnection(raw)
    await pool.release(raw)
