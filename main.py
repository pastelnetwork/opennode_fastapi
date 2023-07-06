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
from pydantic import BaseModel
from fastapi import BackgroundTasks
import nest_asyncio
nest_asyncio.apply()
import asyncio
import time

from fastapi import Request
import logging
from typing import Callable



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


@app.middleware("http")
async def suppress_logging_middleware(request: Request, call_next: Callable):
    if request.method == "GET":
        # Get the current log level
        current_log_level = logging.getLogger('uvicorn.access').getEffectiveLevel()
        # Set the log level to WARNING, so it will only log WARNING, ERROR and CRITICAL
        logging.getLogger('uvicorn.access').setLevel(logging.WARNING)
        response = await call_next(request)
        # Reset the log level to its original value
        logging.getLogger('uvicorn.access').setLevel(current_log_level)
    else:
        response = await call_next(request)
    return response


async def run_task_periodically():
    while True:
        return_message = await opennode_fastapi_service.run_populate_database_with_all_dd_service_data_func()
        # return_message2 = await opennode_fastapi_service.run_scan_new_blocks_func()
        await asyncio.sleep(30)


async def process_db_queue():
    while True:
        await db_write_queue.join()
        await asyncio.sleep(1)  # sleep for a while before checking the queue again
    
    
@app.on_event("startup")
async def start_background_tasks():
    asyncio.create_task(opennode_fastapi_service.startup_cascade_file_download_lock_cleanup_func())
    asyncio.create_task(process_db_queue())
    asyncio.create_task(run_task_periodically())
    

def main():
    configure(dev_mode=True)
    #uvicorn.run(app, host='127.0.0.1', port=8002, debug=True)


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
    configure(dev_mode=False)
