import datetime
from typing import List

import sqlalchemy as sa
import sqlalchemy.orm as orm
from data.modelbase import SqlAlchemyBase


class OpenNodeFastAPIRequests(SqlAlchemyBase):
    __tablename__ = 'opennode_fastapi_requests'
    id = sa.Column(sa.Integer, primary_key=True)
    datetime_request_received: datetime.datetime = sa.Column(sa.DateTime, default=datetime.datetime.now, index=True)
    datetime_request_fulfilled: datetime.datetime = sa.Column(sa.DateTime, index=True)
    user_specified_pastel_address_to_send_to: str = sa.Column(sa.String, nullable=False, index=True)
    requesting_ip_address: str = sa.Column(sa.String, nullable=True, index=True)
    requested_amount_in_lsp: float = sa.Column(sa.Float, nullable=True)
    request_local_opid: str = sa.Column(sa.String, nullable=True)
    request_status: str = sa.Column(sa.String, nullable=True)
    completed_request_txid: str = sa.Column(sa.String, nullable=True)
    valid_request: bool = sa.Column(sa.Boolean, default=True)
    operation_status_json: str = sa.Column(sa.String, nullable=True)

    # User relationship
    user_id: int = sa.Column(sa.Integer, sa.ForeignKey("users.id"))
    user = orm.relationship('User')

    @property
    def __repr__(self):
        return '<OpenNodeFastAPIRequests {}>'.format(self.id)


class ValidationError(Exception):
    def __init__(self, error_msg: str, status_code: int):
        super().__init__(error_msg)

        self.status_code = status_code
        self.error_msg = error_msg