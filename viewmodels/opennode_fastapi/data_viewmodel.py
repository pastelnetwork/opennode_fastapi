import datetime
from typing import List, Optional
from starlette.requests import Request
from data.opennode_fastapi import OpenNodeFastAPIRequests
from services import opennode_fastapi_service
from viewmodels.shared.viewmodel import ViewModelBase


class DataViewModel(ViewModelBase):
    def __init__(self, request: Request):
        super().__init__(request)
        self.opennode_fastapi_requests: Optional[List[OpenNodeFastAPIRequests]] = None

    async def load(self):
        self.opennode_fastapi_requests: List[OpenNodeFastAPIRequests] = await opennode_fastapi_service.get_opennode_requests()

