from typing import List
import sqlalchemy as sa
import sqlalchemy.orm as orm
from data.modelbase import SqlAlchemyBase
from sqlalchemy import Column, String, BigInteger, ForeignKey, DECIMAL, DateTime
from sqlalchemy.orm import relationship
import datetime

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

class PastelBlockData(SqlAlchemyBase):
    __tablename__ = 'pastel_block_data'
    block_hash = Column(String(64), nullable=False, index=True, primary_key=True)
    block_height = Column(BigInteger, nullable=False, index=True)
    previous_block_hash = Column(String(64), nullable=True, index=True)
    transactions = relationship("PastelTransactionData", back_populates="block", cascade="all, delete-orphan")
    timestamp = Column(DateTime, nullable=True)
    
class PastelAddressData(SqlAlchemyBase):
    __tablename__ = 'pastel_address_data'
    pastel_address = Column(String(34), nullable=False, index=True, primary_key=True)
    outputs = relationship("PastelTransactionOutputData", back_populates="address")
    balance = Column(DECIMAL(precision=16, scale=8), nullable=False, default=0.0)

class PastelTransactionData(SqlAlchemyBase):
    __tablename__ = 'pastel_transaction_data'
    transaction_id = Column(String(64), nullable=False, index=True, primary_key=True)
    inputs = relationship("PastelTransactionInputData", back_populates="transaction", cascade="all, delete-orphan")
    outputs = relationship("PastelTransactionOutputData", back_populates="transaction", cascade="all, delete-orphan")
    block_hash = Column(String(64), ForeignKey('pastel_block_data.block_hash'), nullable=True, index=True)
    block = relationship("PastelBlockData", back_populates="transactions")
    total_value = Column(DECIMAL(precision=16, scale=8), nullable=False, default=0.0)
    confirmations = Column(BigInteger, nullable=False, default=0)

class PastelTransactionInputData(SqlAlchemyBase):
    __tablename__ = 'pastel_transaction_input_data'
    input_id = Column(BigInteger, nullable=False, primary_key=True)
    transaction_id = Column(String(64), ForeignKey('pastel_transaction_data.transaction_id'), nullable=False, index=True)
    transaction = relationship("PastelTransactionData", back_populates="inputs")
    previous_output_id = Column(BigInteger, ForeignKey('pastel_transaction_output_data.output_id'), nullable=False, index=True)
    previous_output = relationship("PastelTransactionOutputData", back_populates="inputs")

class PastelTransactionOutputData(SqlAlchemyBase):
    __tablename__ = 'pastel_transaction_output_data'
    output_id = Column(BigInteger, nullable=False, primary_key=True)
    transaction_id = Column(String(64), ForeignKey('pastel_transaction_data.transaction_id'), nullable=False, index=True)
    transaction = relationship("PastelTransactionData", back_populates="outputs")
    amount = Column(DECIMAL(precision=16, scale=8), nullable=False)
    pastel_address = Column(String(34), ForeignKey('pastel_address_data.pastel_address'), nullable=False, index=True)
    address = relationship("PastelAddressData", back_populates="outputs")
    inputs = relationship("PastelTransactionInputData", back_populates="previous_output")
    
class CascadeCacheFileLocks(SqlAlchemyBase):
    __tablename__ = 'cascade_cache_file_locks'
    txid = Column(String(64), primary_key=True)    
