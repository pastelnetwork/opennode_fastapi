from pathlib import Path

import fastapi
import fastapi_chameleon
import uvicorn
import random

from starlette.staticfiles import StaticFiles

from data import db_session
from data.db_session import db_write_queue
from views import opennode_fastapi
from fastapi.middleware.cors import CORSMiddleware
from services import opennode_fastapi_service
from multiprocessing import Process, freeze_support
from pathos.pools import ProcessPool
from pydantic import BaseModel
from fastapi import BackgroundTasks
import nest_asyncio
nest_asyncio.apply()
import asyncio
import time


description_string = """
🎢 Pastel's OpenNode FastAPI provides various informational API endpoints to retrieve information about the Pastel Blockchain. 💸
"""

app = fastapi.FastAPI(title="OpenNode FastAPI", description=description_string, docs_url="/", redoc_url="/redoc")
background_tasks = BackgroundTasks()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True,
)


async def run_task_periodically(background_tasks: BackgroundTasks):
    while True:
        return_message = await opennode_fastapi_service.run_populate_database_with_all_dd_service_data_func(background_tasks)
        # return_message2 = await opennode_fastapi_service.run_scan_new_blocks_func(background_tasks)
        await asyncio.sleep(30)


async def process_db_queue(background_tasks: BackgroundTasks):
    while True:
        await db_write_queue.join()
        await asyncio.sleep(1)  # sleep for a while before checking the queue again
    
    
def start_background_tasks(app, background_tasks: BackgroundTasks):
    asyncio.create_task(opennode_fastapi_service.startup_cascade_file_download_lock_cleanup_func(background_tasks))
    asyncio.create_task(process_db_queue(background_tasks))
    asyncio.create_task(run_task_periodically(background_tasks))
    

class LogConfig(BaseModel):
    """Logging configuration to be set for the server"""

    LOGGER_NAME: str = "opennode_fastapi"
    LOG_FORMAT: str = "%(levelprefix)s | %(asctime)s | %(message)s"
    LOG_LEVEL: str = "DEBUG"

    # Logging config
    version = 1
    disable_existing_loggers = False
    formatters = {
        "default": {
            "()": "uvicorn.logging.DefaultFormatter",
            "fmt": LOG_FORMAT,
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
    }
    handlers = {
        "default": {
            "formatter": "default",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stderr",
        },
    }
    loggers = {
        "opennode_fastapi": {"handlers": ["default"], "level": LOG_LEVEL},
    }


def main():
    configure(dev_mode=True)
    # noinspection PyTypeChecker
    #uvicorn.run(app, host='127.0.0.1', port=8002, debug=True)
    log_config["formatters"]["access"]["fmt"] = "%(asctime)s - %(levelname)s - %(message)s"
    log_config["formatters"]["default"]["fmt"] = "%(asctime)s - %(levelname)s - %(message)s"


def configure(dev_mode: bool):
    configure_routes()
    configure_db(dev_mode)


def configure_db(dev_mode: bool):
    file = (Path(__file__).parent / 'db' / 'opennode_fastapi.sqlite').absolute()
    db_session.global_init(file.as_posix())


def configure_routes():
    app.mount('/static', StaticFiles(directory='static'), name='static')
    app.include_router(opennode_fastapi.router)

if __name__ == '__main__':
    freeze_support()    
    main()
else:
    start_background_tasks(app, background_tasks)
    configure(dev_mode=False)
