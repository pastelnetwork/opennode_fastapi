from pathlib import Path
from typing import Callable, Optional
import asyncio
import time
import logging
import sqlalchemy as sa
import sqlalchemy.orm as orm
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine, AsyncSession
from sqlalchemy.orm import Session
from sqlalchemy import text
from data.modelbase import SqlAlchemyBase

log = logging.getLogger("PastelOpenNodeFastAPI")

# Exponential backoff parameters
initial_delay = 0.1  # Initial delay in seconds
max_delay = 60  # Maximum delay in seconds
backoff_factor = 2  # Factor to multiply the delay each time a write fails

__factory: Optional[Callable[[], Session]] = None
__async_engine: Optional[AsyncEngine] = None

# Global write queue
db_write_queue = asyncio.Queue()

# Global last write time
last_write_time = time.time()

def global_init(db_file: str):
    global __factory, __async_engine
    if __factory:
        return
    if not db_file or not db_file.strip():
        raise Exception("You must specify a db file.")
    folder = Path(db_file).parent
    folder.mkdir(parents=True, exist_ok=True)
    conn_str = 'sqlite+pysqlite:///' + db_file.strip() + '?timeout=15'
    async_conn_str = 'sqlite+aiosqlite:///' + db_file.strip() + '?timeout=15'
    engine = sa.create_engine(conn_str, echo=False, connect_args={"check_same_thread": False})
    __async_engine = create_async_engine(async_conn_str, echo=False, connect_args={"check_same_thread": False})
    with engine.begin() as connection:
        connection.execute(text("PRAGMA journal_mode=WAL;"))
        connection.execute(text("PRAGMA synchronous=NORMAL;"))
        connection.execute(text("PRAGMA cache_size=-524288;"))
        connection.execute(text("PRAGMA busy_timeout=5000;"))  # 5 second timeout. Adjust as needed
        connection.execute(text("PRAGMA journal_size_limit=5242880;"))  # Set a 5 MB limit. Adjust as needed
    __factory = orm.sessionmaker(bind=engine)
    import data.__all_models  # noqa: F401
    SqlAlchemyBase.metadata.create_all(engine)

def create_session() -> Session:
    if not __factory:
        raise Exception("You must call global_init() before using this method.")
    session: Session = __factory()
    session.expire_on_commit = False
    return session

def create_async_session() -> AsyncSession:
    global __async_engine
    if not __async_engine:
        raise Exception("You must call global_init() before using this method.")
    session: AsyncSession = AsyncSession(__async_engine)
    session.sync_session.expire_on_commit = False
    return session

async def wal_checkpoint_func():
    global last_write_time
    retry_count = 0
    while True:
        await asyncio.sleep(1)
        if time.time() - last_write_time > 1:  # Check if there's been a pause in write activity for at least 1 second
            try:
                async with __async_engine.begin() as connection:
                    result = await connection.execute('PRAGMA wal_checkpoint;')
                    log.info(f"WAL checkpoint result: {result.fetchone()}")
                    retry_count = 0  # Reset the retry count if the operation was successful
            except Exception as e:
                retry_count += 1
                backoff_time = min(max_delay, initial_delay * (backoff_factor ** retry_count))
                log.error(f"Error occurred during WAL checkpoint: {e}, retrying in {backoff_time} seconds...")
                await asyncio.sleep(backoff_time)
                
async def write_records_to_db_func():
    global last_write_time
    while True:
        record, retry_count = await db_write_queue.get()
        async with create_async_session() as session:
            try:
                async with session.begin():
                    session.add(record)
                await session.commit()
                last_write_time = time.time()
            except (orm.exc.FlushError, sa.exc.IntegrityError) as e:
                await session.rollback()
                log.error(f"Error during DB write: {e}. Not retrying.")
            except Exception as e:
                await session.rollback()
                log.error(f"Error occurred during DB write: {e}, retrying...")
                await asyncio.sleep(min(max_delay, initial_delay * (backoff_factor ** retry_count)))
                await db_write_queue.put((record, retry_count + 1))
            else:
                db_write_queue.task_done()

async def add_record_to_write_queue(record):
    if not isinstance(record, SqlAlchemyBase):
        log.error(f"Invalid record type: {type(record)}. Expected an instance of a SQLAlchemy mapped class.")
        return
    await db_write_queue.put((record, 0))
