# /data/opennode_fastapi.py

import sqlalchemy as sa
from data.modelbase import SqlAlchemyBase
from sqlalchemy import Column, String, DateTime
from typing import List, Optional, Dict, Any
from datetime import datetime
from pydantic import BaseModel, ConfigDict

class OpenNodeFastAPIRequests(SqlAlchemyBase):
    __tablename__ = 'opennode_fastapi_requests'
    id = sa.Column(sa.Integer, primary_key=True)
    datetime_request_received: datetime = sa.Column(sa.DateTime, default=datetime.now, index=True)
    datetime_request_fulfilled: datetime = sa.Column(sa.DateTime, index=True)
    user_specified_pastel_address_to_send_to: str = sa.Column(sa.String, nullable=False, index=True)
    requesting_ip_address: str = sa.Column(sa.String, nullable=True, index=True)
    requested_amount_in_lsp: float = sa.Column(sa.Float, nullable=True)
    request_local_opid: str = sa.Column(sa.String, nullable=True)
    request_status: str = sa.Column(sa.String, nullable=True)
    completed_request_txid: str = sa.Column(sa.String, nullable=True)
    valid_request: bool = sa.Column(sa.Boolean, default=True)
    operation_status_json: str = sa.Column(sa.String, nullable=True)

    @property
    def __repr__(self):
        return '<OpenNodeFastAPIRequests {}>'.format(self.id)


class ValidationError(Exception):
    def __init__(self, error_msg: str, status_code: int):
        super().__init__(error_msg)

        self.status_code = status_code
        self.error_msg = error_msg
        
class ParsedDDServiceData(SqlAlchemyBase):
    __tablename__ = 'parsed_dd_service_data'
    ticket_type: str = sa.Column(sa.String, nullable=False, index=True)
    registration_ticket_txid: str = sa.Column(sa.String, nullable=False, index=True, primary_key=True)
    hash_of_candidate_image_file: str = sa.Column(sa.String, nullable=False, index=True)
    pastel_id_of_submitter: str = sa.Column(sa.String, nullable=False, index=True)
    pastel_block_hash_when_request_submitted: str = sa.Column(sa.String, nullable=False)
    pastel_block_height_when_request_submitted: str = sa.Column(sa.String, nullable=False)
    dupe_detection_system_version: str = sa.Column(sa.String, nullable=False)
    candidate_image_thumbnail_webp_as_base64_string: str = sa.Column(sa.String, nullable=False)
    collection_name_string: str = sa.Column(sa.String, nullable=False)
    open_api_group_id_string: str = sa.Column(sa.String, nullable=False)
    does_not_impact_the_following_collection_strings: str = sa.Column(sa.String, nullable=False)
    overall_rareness_score: float = sa.Column(sa.Float, nullable=False)
    group_rareness_score: float = sa.Column(sa.Float, nullable=False)
    open_nsfw_score: float = sa.Column(sa.Float, nullable=False)
    alternative_nsfw_scores: str = sa.Column(sa.String, nullable=False)
    utc_timestamp_when_request_submitted: str = sa.Column(sa.String, nullable=False)
    is_likely_dupe: str = sa.Column(sa.String, nullable=False)
    is_rare_on_internet: str = sa.Column(sa.String, nullable=False)
    is_pastel_openapi_request: str = sa.Column(sa.String, nullable=False)
    is_invalid_sense_request: str = sa.Column(sa.String, nullable=False)
    invalid_sense_request_reason: str = sa.Column(sa.String, nullable=False)
    similarity_score_to_first_entry_in_collection: float = sa.Column(sa.Float, nullable=False)
    cp_probability: float = sa.Column(sa.Float, nullable=False)
    child_probability: float = sa.Column(sa.Float, nullable=False)
    image_file_path: str = sa.Column(sa.String, nullable=False)
    image_fingerprint_of_candidate_image_file: str = sa.Column(sa.String, nullable=False)
    pct_of_top_10_most_similar_with_dupe_prob_above_25pct: float = sa.Column(sa.Float, nullable=False)
    pct_of_top_10_most_similar_with_dupe_prob_above_33pct: float = sa.Column(sa.Float, nullable=False)
    pct_of_top_10_most_similar_with_dupe_prob_above_50pct: float = sa.Column(sa.Float, nullable=False)
    internet_rareness__min_number_of_exact_matches_in_page: str = sa.Column(sa.String, nullable=False)
    internet_rareness__earliest_available_date_of_internet_results: str = sa.Column(sa.String, nullable=False)
    internet_rareness__b64_image_strings_of_in_page_matches: str = sa.Column(sa.String, nullable=False)
    internet_rareness__original_urls_of_in_page_matches: str = sa.Column(sa.String, nullable=False)
    internet_rareness__result_titles_of_in_page_matches: str = sa.Column(sa.String, nullable=False)
    internet_rareness__date_strings_of_in_page_matches: str = sa.Column(sa.String, nullable=False)
    internet_rareness__misc_related_images_as_b64_strings: str = sa.Column(sa.String, nullable=False)
    internet_rareness__misc_related_images_url: str = sa.Column(sa.String, nullable=False)
    alternative_rare_on_internet__number_of_similar_results: str = sa.Column(sa.String, nullable=False)
    alternative_rare_on_internet__b64_image_strings: str = sa.Column(sa.String, nullable=False)
    alternative_rare_on_internet__original_urls: str = sa.Column(sa.String, nullable=False)
    alternative_rare_on_internet__google_cache_urls: str = sa.Column(sa.String, nullable=False)
    alternative_rare_on_internet__alt_strings: str = sa.Column(sa.String, nullable=False)
    corresponding_pastel_blockchain_ticket_data: str = sa.Column(sa.String, nullable=False)

class RawDDServiceData(SqlAlchemyBase):
    __tablename__ = 'raw_dd_service_data'
    ticket_type: str = sa.Column(sa.String, nullable=False, index=True)
    registration_ticket_txid: str = sa.Column(sa.String, nullable=False, index=True, primary_key=True)
    hash_of_candidate_image_file: str = sa.Column(sa.String, nullable=False, index=True)
    pastel_id_of_submitter: str = sa.Column(sa.String, nullable=False, index=True)
    pastel_block_hash_when_request_submitted: str = sa.Column(sa.String, nullable=False)
    pastel_block_height_when_request_submitted: str = sa.Column(sa.String, nullable=False)
    raw_dd_service_data_json: str = sa.Column(sa.String, nullable=False)
    corresponding_pastel_blockchain_ticket_data: str = sa.Column(sa.String, nullable=False)

class CascadeCacheFileLocks(SqlAlchemyBase):
    __tablename__ = 'cascade_cache_file_locks'
    txid = Column(String(64), primary_key=True)
    lock_created_at = Column(DateTime, default=datetime.utcnow)  # lock creation timestamp
    
class DdServiceLocks(SqlAlchemyBase):
    __tablename__ = 'dd_service_locks'
    txid = Column(String(64), primary_key=True)
    lock_created_at = Column(DateTime, default=datetime.utcnow)  # lock creation timestamp
    
class BadTXID(SqlAlchemyBase):
    __tablename__ = 'bad_txids'
    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    txid = sa.Column(sa.String, nullable=False, index=True)
    ticket_type = sa.Column(sa.String, nullable=False, index=True)
    reason_txid_is_bad = sa.Column(sa.String, nullable=True)
    datetime_txid_marked_as_bad = sa.Column(sa.DateTime, default=datetime.now, index=True)
    failed_attempts = sa.Column(sa.Integer, default=0)
    next_attempt_time = sa.Column(sa.DateTime, default=datetime.now)

class ShowLogsIncrementalModel(BaseModel):
    logs: str
    last_position: int
    
class LogLines(BaseModel):
    last_lines: str

class AddressMempool(BaseModel):
    addresses: List[str]

class BlockDeltasResponse(BaseModel):
    data: dict

class AddressTxIdsParams(BaseModel):
    addresses: List[str]
    start: Optional[int] = None
    end: Optional[int] = None

class AddressBalanceParams(BaseModel):
    addresses: List[str]

class AddressDeltasParams(BaseModel):
    addresses: List[str]
    start: Optional[int] = None
    end: Optional[int] = None
    chainInfo: Optional[bool] = False

class AddressUtxosParams(BaseModel):
    addresses: List[str]
    chainInfo: Optional[bool] = False

class SpentInfoParams(BaseModel):
    txid: str
    index: int

class BlockHashesOptions(BaseModel):
    noOrphans: Optional[bool] = None
    logicalTimes: Optional[bool] = None
    
class TransactionStatusResponse(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    
    txid: str
    status: str  # "confirmed", "pending", or "not_found"
    confirmations: Optional[int] = None
    in_mempool: bool
    block_hash: Optional[str] = None
    block_height: Optional[int] = None 
    block_time: Optional[datetime] = None
    mempool_time: Optional[datetime] = None
    check_time: datetime

class MempoolStatus(BaseModel):
    in_mempool: bool
    mempool_time: Optional[str] = None  # Store as ISO format string
    mempool_size: Optional[int] = None
    mempool_bytes: Optional[int] = None

class TransactionResponse(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    
    txid: str
    decoded_tx: Dict[str, Any]
    mempool_status: MempoolStatus
    message: str
    status: str
    timestamp: str  # Store as ISO format string