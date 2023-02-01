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

    @property
    def __repr__(self):
        return '<OpenNodeFastAPIRequests {}>'.format(self.id)


class ValidationError(Exception):
    def __init__(self, error_msg: str, status_code: int):
        super().__init__(error_msg)

        self.status_code = status_code
        self.error_msg = error_msg
        
class OpenAPISenseData(SqlAlchemyBase):
    __tablename__ = 'openapi_sense_data'
    sense_registration_ticket_txid: str = sa.Column(sa.String, nullable=False, index=True, primary_key=True)
    hash_of_candidate_image_file: str = sa.Column(sa.String, nullable=False, index=True)
    pastel_id_of_submitter: str = sa.Column(sa.String, nullable=False, index=True)
    pastel_block_hash_when_request_submitted: str = sa.Column(sa.String, nullable=False)
    pastel_block_height_when_request_submitted: str = sa.Column(sa.String, nullable=False)
    dupe_detection_system_version: str = sa.Column(sa.String, nullable=False)
    overall_rareness_score: float = sa.Column(sa.Float, nullable=False)
    open_nsfw_score: float = sa.Column(sa.Float, nullable=False)
    alternative_nsfw_scores: str = sa.Column(sa.String, nullable=False)
    utc_timestamp_when_request_submitted: str = sa.Column(sa.String, nullable=False)
    is_likely_dupe: str = sa.Column(sa.String, nullable=False)
    is_rare_on_internet: str = sa.Column(sa.String, nullable=False)
    is_pastel_openapi_request: str = sa.Column(sa.String, nullable=False)
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
    alternative_rare_on_internet__number_of_similar_results: str = sa.Column(sa.String, nullable=False)
    alternative_rare_on_internet__b64_image_strings: str = sa.Column(sa.String, nullable=False)
    alternative_rare_on_internet__original_urls: str = sa.Column(sa.String, nullable=False)
    alternative_rare_on_internet__result_titles: str = sa.Column(sa.String, nullable=False)
    corresponding_pastel_blockchain_ticket_data: str = sa.Column(sa.String, nullable=False)

      
class OpenAPIRawSenseData(SqlAlchemyBase):
    __tablename__ = 'openapi_raw_sense_data'
    sense_registration_ticket_txid: str = sa.Column(sa.String, nullable=False, index=True, primary_key=True)
    hash_of_candidate_image_file: str = sa.Column(sa.String, nullable=False, index=True)
    pastel_id_of_submitter: str = sa.Column(sa.String, nullable=False, index=True)
    pastel_block_hash_when_request_submitted: str = sa.Column(sa.String, nullable=False)
    pastel_block_height_when_request_submitted: str = sa.Column(sa.String, nullable=False)
    raw_sense_data_json: str = sa.Column(sa.String, nullable=False)
    corresponding_pastel_blockchain_ticket_data: str = sa.Column(sa.String, nullable=False)
