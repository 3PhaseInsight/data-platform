from collections.abc import Iterator

from sqlalchemy.orm import Session

from threephi_framework.db.db import new_session


def get_session() -> Iterator[Session]:
    session = new_session()
    try:
        yield session
    finally:
        session.close()
