import os
import logging
import sqlalchemy as sa
from config.settings import settings
from db.models import metadata

logger = logging.getLogger(__name__)

engine: sa.engine.Engine | None = None


def get_engine() -> sa.engine.Engine:
    global engine
    if engine is None:
        db_path = settings.database_path
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        engine = sa.create_engine(
            f"sqlite:///{db_path}",
            echo=False,
            connect_args={"check_same_thread": False},
        )
    return engine


def init_db():
    """Create all tables if they don't exist."""
    eng = get_engine()
    metadata.create_all(eng)
    logger.info("Database initialized at %s", settings.database_path)


def get_connection():
    """Get a new connection from the engine."""
    return get_engine().connect()


def execute(stmt):
    """Execute a statement and return the result, auto-committing."""
    with get_engine().begin() as conn:
        return conn.execute(stmt)


def fetch_all(stmt) -> list:
    """Execute a SELECT and return all rows as list of Row objects."""
    with get_engine().connect() as conn:
        result = conn.execute(stmt)
        return result.fetchall()


def fetch_one(stmt):
    """Execute a SELECT and return a single row."""
    with get_engine().connect() as conn:
        result = conn.execute(stmt)
        return result.fetchone()


# --- Helper: DB-stored settings ---

def get_setting(key: str, default: str = "") -> str:
    from db.models import app_settings
    row = fetch_one(sa.select(app_settings.c.value).where(app_settings.c.key == key))
    return row[0] if row else default


def set_setting(key: str, value: str):
    from db.models import app_settings
    with get_engine().begin() as conn:
        existing = conn.execute(
            sa.select(app_settings.c.key).where(app_settings.c.key == key)
        ).fetchone()
        if existing:
            conn.execute(
                app_settings.update()
                .where(app_settings.c.key == key)
                .values(value=value, updated_at=sa.func.current_timestamp())
            )
        else:
            conn.execute(app_settings.insert().values(key=key, value=value))
