from pathlib import Path

import fastapi
import fastapi_chameleon
import uvicorn

from starlette.staticfiles import StaticFiles

from data import db_session
from views import opennode_fastapi
from fastapi.middleware.cors import CORSMiddleware
from services import opennode_fastapi_service
from multiprocessing import Process, freeze_support
from pathos.pools import ProcessPool
from pydantic import BaseModel

description_string = """
🎢 Pastel's OpenNode FastAPI provides various informational API endpoints to retrieve information about the Pastel Blockchain. 💸
"""

app = fastapi.FastAPI(title="OpenNode FastAPI", description=description_string, docs_url="/", redoc_url="/redoc")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True,
)

test_code_payload = 0

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
    if test_code_payload:
        print('Test code:')
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
    configure(dev_mode=False)
