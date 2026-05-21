import os

import pytest
from sqlalchemy import text

from threephi_framework.db.db import get_engine, new_session


@pytest.fixture(scope="session")
def engine():
    required = ("DB_USER", "DB_PASSWORD", "DB_HOST", "DB_PORT", "DB_NAME")
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        pytest.skip(f"DB env vars not set: {missing}")
    return get_engine()


@pytest.fixture
def db_session(engine):
    session = new_session()
    try:
        yield session
    finally:
        session.rollback()
        session.close()


@pytest.fixture
def clean_run_result(db_session):
    """Truncate run_result before each test that uses it. Caller must commit."""
    meta_schema = os.getenv("META_SCHEMA", "meta")
    db_session.execute(text(f'TRUNCATE TABLE "{meta_schema}".run_result CASCADE'))
    db_session.commit()
    yield db_session
