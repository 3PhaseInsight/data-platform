from collections.abc import Iterator

from sqlalchemy.orm import Session


def get_session() -> Iterator[Session]:
    from threephi_framework.db.db import new_session  # lazy: avoids heavy framework init at import time

    session = new_session()
    try:
        yield session
    finally:
        session.close()
