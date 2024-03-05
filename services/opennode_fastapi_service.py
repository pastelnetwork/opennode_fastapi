import asyncio
import base64
import decimal
import hashlib
import html
import ipaddress
import json
import os
import platform
import random
import re
import statistics
import subprocess
import time
import warnings
import traceback
from collections import Counter
from datetime import datetime, timedelta
from typing import List, Optional, Tuple
from pathlib import Path
import aiofiles
import dirtyjson
import magic
import numpy as np
import pandas as pd
import psutil
import zstandard as zstd
from cachetools import LRUCache
from httpx import AsyncClient, Limits, Timeout
from sqlalchemy.future import select
from sqlalchemy.exc import OperationalError, IntegrityError, DBAPIError

from data import db_session
from data.db_session import add_record_to_write_queue
from data.opennode_fastapi import (
    BadTXID,
    CascadeCacheFileLocks,
    DdServiceLocks,
    ParsedDDServiceData,
    RawDDServiceData,
)
try:
    import urllib.parse as urlparse
except ImportError:
    import urlparse
import logging
from logging.handlers import RotatingFileHandler

from decouple import Config as DecoupleConfig, RepositoryEnv

config = DecoupleConfig(RepositoryEnv('.env'))
REQUESTER_PASTELID = config.get("PASTELID")
REQUESTER_PASTELID_PASSPHRASE = config.get("PASTELID_PASSPHRASE")

#You must install libmagic with: sudo apt-get install libmagic-dev
number_of_cpus = os.cpu_count()
my_os = platform.system()
loop = asyncio.get_event_loop()
warnings.filterwarnings('ignore')
parent = psutil.Process()

if 'Windows' in my_os:
    parent.nice(psutil.BELOW_NORMAL_PRIORITY_CLASS)
else:
    parent.nice(19)

USER_AGENT = "AuthServiceProxy/0.1"
HTTP_TIMEOUT = 180

# Setup logging:
log = logging.getLogger("PastelOpenNodeFastAPI")
log.propagate = False  # Stop propagating to parent loggers
for handler in log.handlers[:]:
    log.removeHandler(handler)
log.setLevel(logging.INFO)
log_folder = Path('old_application_log_files')
log_folder.mkdir(parents=True, exist_ok=True)
log_file_full_path = log_folder / 'opennode_fastapi_log.txt'
rotating_handler = RotatingFileHandler(log_file_full_path, maxBytes=5*1024*1024, backupCount=10)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
rotating_handler.setFormatter(formatter)
log.addHandler(rotating_handler)
log.addHandler(ch)

# Initialize the LRU cache and cache directory
cache_dir = "/home/ubuntu/cascade_opennode_fastapi_cache"
#create cache directory if it doesn't exist
os.makedirs(cache_dir, exist_ok=True)

class SizeLimitedLRUCache(LRUCache):
    def __init__(self, maxsize, *args, **kwargs):
        super().__init__(maxsize, *args, **kwargs)
        self.getsize = os.path.getsize
        self.current_size = 0

    def __setitem__(self, key, value):
        old_value = self.get(key)
        if old_value is not None:
            self.current_size -= self.getsize(old_value)
        self.current_size += self.getsize(value)
        while self.current_size > self.maxsize:
            removed_key, removed_value = self.popitem(last=False)
            self.current_size -= self.getsize(removed_value)
            os.remove(os.path.join(cache_dir, removed_key))  # delete the file associated with the key
        super().__setitem__(key, value)
        
# Create a cache with a maximum size
cache = SizeLimitedLRUCache(maxsize=10 * 1024 * 1024 * 1024)  # 10 GB
active_sessions = {}  # Dictionary to hold active sessions and their last used times
df_lock = asyncio.Lock()

async def cleanup_idle_sessions():
    max_idle_time_in_seconds = 1000
    while True:
        current_time = asyncio.get_event_loop().time()
        idle_sessions = [s for s, last_used in active_sessions.items() if current_time - last_used > max_idle_time_in_seconds]
        for session in idle_sessions:
            await session.close()
            del active_sessions[session]
        await asyncio.sleep(60)
        
# cleanup_task = asyncio.create_task(cleanup_idle_sessions())

class ClientSessionManager:
    def __init__(self, max_connections: int = 100, max_keepalive_connections: int = 10):
        self.client_session: Optional[AsyncClient] = None
        self.max_connections = max_connections
        self.max_keepalive_connections = max_keepalive_connections

    async def get_or_create_session(self) -> AsyncClient:
        if self.client_session is None:
            self.client_session = AsyncClient(
                timeout=Timeout(600),
                limits=Limits(max_connections=self.max_connections, max_keepalive_connections=self.max_keepalive_connections)
            )
        return self.client_session

    async def close_session(self):
        if self.client_session:
            await self.client_session.aclose()
            self.client_session = None

session_manager = ClientSessionManager() # Initialize global session manager
        
def parse_mime_type(mime_type):
    return tuple(mime_type.split(';')[0].split('/'))
        
async def load_cache():  # Populate cache from the existing files in the cache directory
    for filename in os.listdir(cache_dir):
        file_path = os.path.join(cache_dir, filename)
        if os.path.isfile(file_path):
            cache[filename] = file_path

def get_local_rpc_settings_func(directory_with_pastel_conf=os.path.expanduser("~/.pastel/")):
    with open(os.path.join(directory_with_pastel_conf, "pastel.conf"), 'r') as f:
        lines = f.readlines()
    other_flags = {}
    rpchost = '127.0.0.1'
    rpcport = '19932'
    for line in lines:
        if line.startswith('rpcport'):
            value = line.split('=')[1]
            rpcport = value.strip()
        elif line.startswith('rpcuser'):
            value = line.split('=')[1]
            rpcuser = value.strip()
        elif line.startswith('rpcpassword'):
            value = line.split('=')[1]
            rpcpassword = value.strip()
        elif line.startswith('rpchost'):
            pass
        elif line == '\n':
            pass
        else:
            current_flag = line.strip().split('=')[0].strip()
            current_value = line.strip().split('=')[1].strip()
            other_flags[current_flag] = current_value
    return rpchost, rpcport, rpcuser, rpcpassword, other_flags
    
def get_remote_rpc_settings_func():
    rpchost = '45.67.221.205'
    #rpchost = '209.145.54.164'
    rpcuser = 'IzfUzMZI'
    rpcpassword = 'ku5YhVtKSNWMIYp'
    rpcport = '9932'
    return rpchost, rpcport, rpcuser, rpcpassword    

class JSONRPCException(Exception):
    def __init__(self, rpc_error):
        parent_args = []
        try:
            parent_args.append(rpc_error['message'])
        except Exception as e:
            log.error(f"Error occurred in JSONRPCException: {e}")
            pass
        Exception.__init__(self, *parent_args)
        self.error = rpc_error
        self.code = rpc_error['code'] if 'code' in rpc_error else None
        self.message = rpc_error['message'] if 'message' in rpc_error else None

    def __str__(self):
        return '%d: %s' % (self.code, self.message)

    def __repr__(self):
        return '<%s \'%s\'>' % (self.__class__.__name__, self)

def EncodeDecimal(o):
    if isinstance(o, decimal.Decimal):
        return float(round(o, 8))
    raise TypeError(repr(o) + " is not JSON serializable")
    
class AsyncAuthServiceProxy:
    max_concurrent_requests = 5000
    _semaphore = asyncio.BoundedSemaphore(max_concurrent_requests)
    def __init__(self, service_url, service_name=None, reconnect_timeout=15, reconnect_amount=2, request_timeout=20):
        self.service_url = service_url
        self.service_name = service_name
        self.url = urlparse.urlparse(service_url)        
        self.client = AsyncClient(timeout=Timeout(request_timeout), limits=Limits(max_connections=200, max_keepalive_connections=10))
        self.id_count = 0
        user = self.url.username
        password = self.url.password
        authpair = f"{user}:{password}".encode('utf-8')
        self.auth_header = b'Basic ' + base64.b64encode(authpair)
        self.reconnect_timeout = reconnect_timeout
        self.reconnect_amount = reconnect_amount
        self.request_timeout = request_timeout

    def __getattr__(self, name):
        if name.startswith('__') and name.endswith('__'):
            raise AttributeError
        if self.service_name is not None:
            name = f"{self.service_name}.{name}"
        return AsyncAuthServiceProxy(self.service_url, name)

    async def __call__(self, *args):
        async with self._semaphore: # Acquire a semaphore
            self.id_count += 1
            postdata = json.dumps({
                'version': '1.1',
                'method': self.service_name,
                'params': args,
                'id': self.id_count
            }, default=EncodeDecimal)
            headers = {
                'Host': self.url.hostname,
                'User-Agent': "AuthServiceProxy/0.1",
                'Authorization': self.auth_header,
                'Content-type': 'application/json'
            }
            for i in range(self.reconnect_amount):
                try:
                    if i > 0:
                        log.warning(f"Reconnect try #{i+1}")
                        sleep_time = self.reconnect_timeout * (2 ** i)
                        log.info(f"Waiting for {sleep_time} seconds before retrying.")
                        await asyncio.sleep(sleep_time)
                    response = await self.client.post(
                        self.service_url, headers=headers, data=postdata)
                    break
                except Exception as e:
                    log.error(f"Error occurred in __call__: {e}")
                    err_msg = f"Failed to connect to {self.url.hostname}:{self.url.port}"
                    rtm = self.reconnect_timeout
                    if rtm:
                        err_msg += f". Waiting {rtm} seconds."
                    log.exception(err_msg)
            else:
                log.error("Reconnect tries exceeded.")
                return
            response_json = response.json()
            if response_json['error'] is not None:
                raise JSONRPCException(response_json['error'])
            elif 'result' not in response_json:
                raise JSONRPCException({
                    'code': -343, 'message': 'missing JSON-RPC result'})
            else:
                return response_json['result']
        
async def get_current_pastel_block_height_func():
    global rpc_connection
    best_block_hash = await rpc_connection.getbestblockhash()
    best_block_details = await rpc_connection.getblock(best_block_hash)
    curent_block_height = best_block_details['height']
    return curent_block_height

async def get_previous_block_hash_and_merkle_root_func():
    global rpc_connection
    previous_block_height = await get_current_pastel_block_height_func()
    previous_block_hash = await rpc_connection.getblockhash(previous_block_height)
    previous_block_details = await rpc_connection.getblock(previous_block_hash)
    previous_block_merkle_root = previous_block_details['merkleroot']
    return previous_block_hash, previous_block_merkle_root, previous_block_height

async def get_last_block_data_func():
    global rpc_connection
    current_block_height = await get_current_pastel_block_height_func()
    block_data = await rpc_connection.getblock(str(current_block_height))
    return block_data

async def check_psl_address_balance_func(address_to_check):
    global rpc_connection
    balance_at_address = await rpc_connection.z_getbalance(address_to_check) 
    return balance_at_address

async def get_raw_transaction_func(txid):
    global rpc_connection
    raw_transaction_data = await rpc_connection.getrawtransaction(txid, 1) 
    return raw_transaction_data

async def verify_message_with_pastelid_func(pastelid, message_to_verify, pastelid_signature_on_message) -> str:
    global rpc_connection
    verification_result = await rpc_connection.pastelid('verify', message_to_verify, pastelid_signature_on_message, pastelid, 'ed448')
    return verification_result['verification']

async def check_masternode_top_func():
    global rpc_connection
    masternode_top_command_output = await rpc_connection.masternode('top')
    return masternode_top_command_output

async def check_supernode_list_func():
    global rpc_connection
    masternode_list_full_command_output = await rpc_connection.masternodelist('full')
    masternode_list_rank_command_output = await rpc_connection.masternodelist('rank')
    masternode_list_pubkey_command_output = await rpc_connection.masternodelist('pubkey')
    masternode_list_extra_command_output = await rpc_connection.masternodelist('extra')
    masternode_list_full_df = pd.DataFrame([masternode_list_full_command_output[x].split() for x in masternode_list_full_command_output])
    masternode_list_full_df['txid_vout'] = [x for x in masternode_list_full_command_output]
    masternode_list_full_df.columns = ['supernode_status', 'protocol_version', 'supernode_psl_address', 'lastseentime', 'activeseconds', 'lastpaidtime', 'lastpaidblock', 'ipaddress:port', 'txid_vout']
    masternode_list_full_df.index = masternode_list_full_df['txid_vout']
    masternode_list_full_df.drop(columns=['txid_vout'], inplace=True)
    for current_row in masternode_list_full_df.iterrows():
            current_row_df = pd.DataFrame(current_row[1]).T
            current_txid_vout = current_row_df.index[0]
            current_rank = masternode_list_rank_command_output[current_txid_vout]
            current_pubkey = masternode_list_pubkey_command_output[current_txid_vout]
            current_extra = masternode_list_extra_command_output[current_txid_vout]
            masternode_list_full_df.loc[current_row[0], 'rank'] = current_rank
            masternode_list_full_df.loc[current_row[0], 'pubkey'] = current_pubkey
            masternode_list_full_df.loc[current_row[0], 'extAddress'] = current_extra['extAddress']
            masternode_list_full_df.loc[current_row[0], 'extP2P'] = current_extra['extP2P']
            masternode_list_full_df.loc[current_row[0], 'extKey'] = current_extra['extKey']
    masternode_list_full_df['lastseentime'] = pd.to_datetime(masternode_list_full_df['lastseentime'], unit='s')
    masternode_list_full_df['lastpaidtime'] = pd.to_datetime(masternode_list_full_df['lastpaidtime'], unit='s')
    masternode_list_full_df['activeseconds'] = masternode_list_full_df['activeseconds'].astype(int)
    masternode_list_full_df['lastpaidblock'] = masternode_list_full_df['lastpaidblock'].astype(int)
    masternode_list_full_df['activedays'] = [float(x)/86400.0 for x in masternode_list_full_df['activeseconds'].values.tolist()]
    masternode_list_full_df['rank'] = masternode_list_full_df['rank'].astype(int)
    masternode_list_full_df__json = masternode_list_full_df.to_json(orient='index')
    return masternode_list_full_df__json

async def get_network_storage_fees_func():
    global rpc_connection
    network_median_storage_fee = await rpc_connection.storagefee('getnetworkfee')
    network_median_nft_ticket_fee = await rpc_connection.storagefee('getnftticketfee')
    json_results = {'network_median_storage_fee': network_median_storage_fee, 'network_median_nft_ticket_fee': network_median_nft_ticket_fee}
    return json_results
    
async def get_local_machine_supernode_data_func():
    local_machine_ip = get_external_ip_func()
    supernode_list_full_df = await check_supernode_list_func()
    proper_port_number = statistics.mode([x.split(':')[1] for x in supernode_list_full_df['ipaddress:port'].values.tolist()])
    local_machine_ip_with_proper_port = local_machine_ip + ':' + proper_port_number
    local_machine_supernode_data = supernode_list_full_df[supernode_list_full_df['ipaddress:port'] == local_machine_ip_with_proper_port]
    if len(local_machine_supernode_data) == 0:
        log.error('Local machine is not a supernode!')
        return 0, 0, 0, 0
    else:
        log.info('Local machine is a supernode!')
        local_sn_rank = local_machine_supernode_data['rank'].values[0]
        local_sn_pastelid = local_machine_supernode_data['extKey'].values[0]
    return local_machine_supernode_data, local_sn_rank, local_sn_pastelid, local_machine_ip_with_proper_port

async def get_sn_data_from_pastelid_func(specified_pastelid):
    supernode_list_full_df = await check_supernode_list_func()
    specified_machine_supernode_data = supernode_list_full_df[supernode_list_full_df['extKey'] == specified_pastelid]
    if len(specified_machine_supernode_data) == 0:
        log.error('Specified machine is not a supernode!')
        return pd.DataFrame()
    else:
        return specified_machine_supernode_data

async def get_sn_data_from_sn_pubkey_func(specified_sn_pubkey):
    supernode_list_full_df = await check_supernode_list_func()
    specified_machine_supernode_data = supernode_list_full_df[supernode_list_full_df['pubkey'] == specified_sn_pubkey]
    if len(specified_machine_supernode_data) == 0:
        log.error('Specified machine is not a supernode!')
        return pd.DataFrame()
    else:
        return specified_machine_supernode_data

def check_if_transparent_psl_address_is_valid_func(pastel_address_string):
    if len(pastel_address_string) == 35 and (pastel_address_string[0:2] == 'Pt'):
        pastel_address_is_valid = 1
    else:
        pastel_address_is_valid = 0
    return pastel_address_is_valid

def check_if_transparent_lsp_address_is_valid_func(pastel_address_string):
    if len(pastel_address_string) == 35 and (pastel_address_string[0:2] == 'tP'):
        pastel_address_is_valid = 1
    else:
        pastel_address_is_valid = 0
    return pastel_address_is_valid

async def get_df_json_from_tickets_list_rpc_response_func(rpc_response):
    tickets_df = pd.DataFrame.from_records([rpc_response[idx]['ticket'] for idx, x in enumerate(rpc_response)])
    tickets_df['txid'] = [rpc_response[idx]['txid'] for idx, x in enumerate(rpc_response)]
    tickets_df['height'] = [rpc_response[idx]['height'] for idx, x in enumerate(rpc_response)]
    tickets_df_json = tickets_df.to_json(orient='index')
    return tickets_df_json


async def get_pastel_blockchain_ticket_func(txid):
    global rpc_connection
    response_json = await rpc_connection.tickets('get', txid )
    if len(response_json) > 0:
        ticket_type_string = response_json['ticket']['type']
        corresponding_reg_ticket_block_height = response_json['height']
        latest_block_height = await get_current_pastel_block_height_func()
        if int(corresponding_reg_ticket_block_height) < 0:
            log.warning(f'The corresponding reg ticket block height of {corresponding_reg_ticket_block_height} is less than 0!')
        if int(corresponding_reg_ticket_block_height) > int(latest_block_height):
            log.info(f'The corresponding reg ticket block height of {corresponding_reg_ticket_block_height} is greater than the latest block height of {latest_block_height}!')
        corresponding_reg_ticket_block_info = await rpc_connection.getblock(str(corresponding_reg_ticket_block_height))
        corresponding_reg_ticket_block_timestamp = corresponding_reg_ticket_block_info['time']
        corresponding_reg_ticket_block_timestamp_utc_iso = datetime.utcfromtimestamp(corresponding_reg_ticket_block_timestamp).isoformat()
        response_json['reg_ticket_block_timestamp_utc_iso'] = corresponding_reg_ticket_block_timestamp_utc_iso
        if ticket_type_string == 'nft-reg':
            activation_response_json = await rpc_connection.tickets('find', 'act', txid )
        elif ticket_type_string == 'action-reg':
            activation_response_json = await rpc_connection.tickets('find', 'action-act', txid )
        elif ticket_type_string == 'collection-reg':
            activation_response_json = await rpc_connection.tickets('find', 'collection-act', txid )
        else:
            activation_response_json = f'No activation ticket needed for this ticket type ({ticket_type_string})'
        if len(activation_response_json) > 0:
            response_json['activation_ticket'] = activation_response_json
        else:
            response_json['activation_ticket'] = 'No activation ticket found for this ticket-- check again soon'
        return response_json
    else:
        response_json = 'No ticket found for this txid'
    return response_json


async def get_all_pastel_blockchain_tickets_func(verbose=0):
    if verbose:
        log.info('Now retrieving all Pastel blockchain tickets...')
    tickets_obj = {}
    list_of_ticket_types = ['id', 'nft', 'offer', 'accept', 'transfer', 'royalty', 'username', 'ethereumaddress', 'action', 'action-act'] # 'collection', 'collection-act'
    for current_ticket_type in list_of_ticket_types:
        if verbose:
            log.info('Getting ' + current_ticket_type + ' tickets...')
        response = await rpc_connection.tickets('list', current_ticket_type)
        if response is not None and len(response) > 0:
            tickets_obj[current_ticket_type] = await get_df_json_from_tickets_list_rpc_response_func(response)
    return tickets_obj

async def get_usernames_from_pastelid_func(pastelid):
    global rpc_connection
    response = await rpc_connection.tickets('list', 'username')
    list_of_returned_usernames = []
    if response is not None and len(response) > 0:
        for idx, x in enumerate(response):
            if response[idx]['ticket']['pastelID'] == pastelid:
                list_of_returned_usernames.append(response[idx]['ticket']['username'])
    if len(list_of_returned_usernames) > 0:
        if len(list_of_returned_usernames) == 1:
            return list_of_returned_usernames[0]
        else:
            return list_of_returned_usernames
    else:
        return 'Error! No username found for this pastelid'

async def get_pastelid_from_username_func(username):
    global rpc_connection
    response = await rpc_connection.tickets('list', 'username')
    if response is not None and len(response) > 0:
        for idx, x in enumerate(response):
            if response[idx]['ticket']['username'] == username:
                return response[idx]['ticket']['pastelID']
    return 'Error! No pastelid found for this username'

async def testnet_pastelid_file_dispenser_func(password, verbose=0):
    log.info('Now generating a pastelid...')
    response = await rpc_connection.pastelid('newkey', password)
    pastelid_data = ''
    pastelid_pubkey = ''
    if response is not None and len(response) > 0:
        if 'pastelid' in response:
            log.info('The pastelid is ' + response['pastelid'])    
            log.info('Now checking to see if the pastelid file exists...')
            pastelid_pubkey = response['pastelid']
            if os.path.exists('~/.pastel/testnet3/pastelkeys/' + response['pastelid']):
                log.info('The pastelid file exists!')
                with open('~/.pastel/testnet3/pastelkeys/' + response['pastelid'], 'rb') as f:
                    pastelid_data = f.read()
                    return pastelid_data                     
            else:
                log.info('The pastelid file does not exist!')
        else:
            log.error('There was an issue creating the pastelid!')
    return pastelid_pubkey, pastelid_data

async def startup_dd_service_lock_cleanup_func():
    async with db_session.create_async_session() as session:
        lock_expiration_time = timedelta(minutes=10)
        current_time = datetime.utcnow()
        stale_locks_query = select(DdServiceLocks).where(DdServiceLocks.lock_created_at < (current_time - lock_expiration_time))
        result = await session.execute(stale_locks_query)
        stale_locks = result.scalars().all()
        for lock in stale_locks:
            session.delete(lock)
        await session.commit()

async def periodic_dd_service_lock_cleanup_func():
    while True:
        await asyncio.sleep(600)  # Run every 10 minutes
        await startup_dd_service_lock_cleanup_func()
        
async def acquire_dd_service_lock(session, txid, lock_expiration_time):
    current_time = datetime.utcnow()
    lock = await session.get(DdServiceLocks, txid)
    if lock and current_time - lock.lock_created_at < lock_expiration_time:
        return False  # Lock exists and is not stale
    retry_count = 0
    max_retries = 5  # Or any other number you consider appropriate
    initial_delay = 0.1
    backoff_factor = 2
    while retry_count < max_retries:
        try:
            if lock:  # Lock is stale, update it
                lock.lock_created_at = current_time
            else:  # Lock does not exist, create it
                lock = DdServiceLocks(txid=txid, lock_created_at=current_time)
                session.add(lock)
            await session.commit()
            return True
        except Exception as e:  # noqa: F841
            # log.error(f"An exception occurred while attempting to acquire a DD service lock for txid {txid}: {e}")
            retry_count += 1
            backoff_time = min(60, initial_delay * (backoff_factor ** retry_count))  # 60 seconds max delay
            await asyncio.sleep(backoff_time)
    return False  # Return False if all retries fail

async def delete_dd_service_lock(session, txid):
    lock = await session.get(DdServiceLocks, txid)
    if lock:
        await session.delete(lock)
        await session.commit()

async def get_raw_dd_service_results_from_local_db_func(txid: str) -> Optional[RawDDServiceData]:
    try:
        async with db_session.create_async_session() as session:
            query = select(RawDDServiceData).filter(RawDDServiceData.registration_ticket_txid == txid)
            result = await session.execute(query)
            found_data = result.scalar_one_or_none()
        return found_data
    except Exception as e:
        log.error(f"An exception occurred while attempting to get raw DD service results from local DB for txid {txid}: {e}")
        raise e

async def get_storage_challenges_metrics_func(metric_type_string='summary_stats', results_count=50):
    client_session = await session_manager.get_or_create_session()
    try:
        from_datetime_string = (datetime.now() - timedelta(weeks=1)).strftime('%Y-%m-%dT%H:%M:%SZ')
        if metric_type_string == 'detailed_log':
            request_url = f'http://localhost:8080/storage_challenges/detailed_logs?pid={REQUESTER_PASTELID}&count={results_count}'
        elif metric_type_string == 'summary_stats':
            request_url = f'http://localhost:8080/storage_challenges/summary_stats?pid={REQUESTER_PASTELID}&from={from_datetime_string}'
        else:
            raise ValueError(f'Invalid storage-challenge metrics type for {metric_type_string}! Must be "detailed_log" or "summary_stats"!')
        headers = {'Authorization': REQUESTER_PASTELID_PASSPHRASE}
        response = await client_session.get(request_url, headers=headers, timeout=250)
        if response.status_code != 200:
            raise ValueError(f"Received {response.status_code} from storage challenges metrics API.")
        body = await response.aread()
        parsed_response = json.loads(body.decode())    
        return parsed_response
    except Exception as e:
        log.error(f'Exception in get_storage_challenges_metrics_func: {e}')
        raise e
    
async def get_self_healing_metrics_func(metric_type_string='summary_stats', results_count=50):
    client_session = await session_manager.get_or_create_session()
    try:
        from_datetime_string = (datetime.now() - timedelta(weeks=4)).strftime('%Y-%m-%dT%H:%M:%SZ')
        if metric_type_string == 'detailed_log':
            request_url = f'http://localhost:8080/self_healing/detailed_logs?pid={REQUESTER_PASTELID}&count={results_count}'
        elif metric_type_string == 'summary_stats':
            request_url = f'http://localhost:8080/self_healing/summary_stats?pid={REQUESTER_PASTELID}&from={from_datetime_string}'
        else:
            raise ValueError(f'Invalid self-healing metrics type for {metric_type_string}! Must be "detailed_log" or "summary_stats"!')
        headers = {'Authorization': REQUESTER_PASTELID_PASSPHRASE}
        response = await client_session.get(request_url, headers=headers, timeout=250)
        if response.status_code != 200:
            raise ValueError(f"Received {response.status_code} from self-healing metrics API.")
        body = await response.aread()
        parsed_response = json.loads(body.decode())    
        return parsed_response
    except Exception as e:
        log.error(f'Exception in get_self_healing_metrics_func: {e}')
        raise e

async def get_raw_dd_service_results_from_sense_api_func(txid: str, ticket_type: str, corresponding_pastel_blockchain_ticket_data: dict) -> RawDDServiceData:
    lock_expiration_time = timedelta(minutes=10)
    async with db_session.create_async_session() as session:
        if not await acquire_dd_service_lock(session, txid, lock_expiration_time):
            return None
        client_session = await session_manager.get_or_create_session()
        try:
            if ticket_type == 'sense':
                request_url = f'http://localhost:8080/openapi/sense/download?pid={REQUESTER_PASTELID}&txid={txid}'
            elif ticket_type == 'nft':
                request_url = f'http://localhost:8080/nfts/download?pid={REQUESTER_PASTELID}&txid={txid}'
            else:
                raise ValueError(f'Invalid ticket type for txid {txid}! Ticket type must be "sense" or "nft"!')
            headers = {'Authorization': REQUESTER_PASTELID_PASSPHRASE}
            response = await client_session.get(request_url, headers=headers, timeout=250)
            if response.status_code != 200:
                raise ValueError(f"Received {response.status_code} from {ticket_type} API.")
            body = response.content
            parsed_response = json.loads(body.decode())
            if 'file' not in parsed_response:
                raise ValueError(f'No file was returned from the {ticket_type} API for txid {txid}!')
            if parsed_response['file'] is None:
                raise ValueError(f'No file was returned from the {ticket_type} API for txid {txid}!')
            decoded_response = base64.b64decode(parsed_response['file'])
            final_response = json.loads(decoded_response)
            final_response_df = pd.DataFrame.from_records([final_response])
            raw_dd_service_data = RawDDServiceData()
            raw_dd_service_data.ticket_type = ticket_type
            raw_dd_service_data.registration_ticket_txid = txid
            raw_dd_service_data.hash_of_candidate_image_file = final_response_df['hash_of_candidate_image_file'][0]
            raw_dd_service_data.pastel_id_of_submitter = final_response_df['pastel_id_of_submitter'][0]
            raw_dd_service_data.pastel_block_hash_when_request_submitted = final_response_df['pastel_block_hash_when_request_submitted'][0]
            raw_dd_service_data.pastel_block_height_when_request_submitted = str(final_response_df['pastel_block_height_when_request_submitted'][0])
            raw_dd_service_data.raw_dd_service_data_json = decoded_response
            raw_dd_service_data.corresponding_pastel_blockchain_ticket_data = str(corresponding_pastel_blockchain_ticket_data)
            if not all([raw_dd_service_data.ticket_type, raw_dd_service_data.registration_ticket_txid, raw_dd_service_data.hash_of_candidate_image_file]):
                raise ValueError("Validation failed: Some required fields are missing.")
            await add_record_to_write_queue(raw_dd_service_data)
            async with db_session.create_async_session() as session:
                await delete_dd_service_lock(session, txid)
            return raw_dd_service_data
        except Exception as e:
            async with db_session.create_async_session() as session:
                await delete_dd_service_lock(session, txid)
            full_stack_trace = traceback.format_exc()
            log.error(f"An exception occurred while attempting to get raw DD service results from Sense API for txid {txid}: {e}\n{full_stack_trace}")
            raise e
        
async def get_raw_dd_service_results_by_registration_ticket_txid_func(txid: str) -> RawDDServiceData:
    try:
        raw_dd_service_data = await get_raw_dd_service_results_from_local_db_func(txid)
        if raw_dd_service_data:
            log.info(f"Data found in local cache for txid {txid}.")
            return raw_dd_service_data, True
        corresponding_pastel_blockchain_ticket_data = await get_pastel_blockchain_ticket_func(txid)
        ticket = corresponding_pastel_blockchain_ticket_data.get('ticket', {})
        if 'nft_ticket' in ticket:
            ticket_type = 'nft'
        elif 'action_ticket' in ticket:
            ticket_type = 'sense'
        else:
            log.warning(f"Unknown ticket type for txid {txid}.")
            ticket_type = 'unknown'
        raw_dd_service_data = await get_raw_dd_service_results_from_sense_api_func(txid, ticket_type, corresponding_pastel_blockchain_ticket_data)
        return raw_dd_service_data, False
    except Exception as e:
        log.error(f"Failed to get raw DD service results for txid {txid} with error: {e}")
        raise e

    
def decompress_and_decode_zstd_compressed_and_base64_encoded_string_func(compressed_b64_string: str) -> str:
    try:
        decompressed_data = zstd.decompress(base64.b64decode(compressed_b64_string))
        return decompressed_data.decode()
    except Exception as e:
        log.error(f"Encountered an error while trying to decompress and decode data: {e}")
        return None

def safe_json_loads_func(json_string: str) -> dict:
    try:
        loaded_json = json.loads(json_string)
        return loaded_json
    except Exception as e:
        log.error(f"Encountered an error while trying to parse json_string: {e}")
        loaded_json = dirtyjson.loads(json_string.replace('\\"', '"').replace('/', '/').replace('\\n', ' '))
        return loaded_json

def cast_numpy_types_for_serialization(obj):
    for attr_name, attr_value in obj.__dict__.items():
        if isinstance(attr_value, np.int64):
            setattr(obj, attr_name, int(attr_value))
    return obj

async def parse_raw_dd_service_data_func(raw_dd_service_data: RawDDServiceData) -> ParsedDDServiceData:
    parsed_dd_service_data = ParsedDDServiceData()
    parsed_dd_service_data.ticket_type = raw_dd_service_data.ticket_type
    try:
        final_response = safe_json_loads_func(raw_dd_service_data.raw_dd_service_data_json)
    except Exception as e:
        log.error(f"Error loading raw_dd_service_data_json: {e}")
        return cast_numpy_types_for_serialization(parsed_dd_service_data)
    final_response_df = pd.DataFrame.from_records([final_response])
    try:
        internet_rareness_json = final_response.get('internet_rareness', {})
        internet_rareness_summary_table_json = decompress_and_decode_zstd_compressed_and_base64_encoded_string_func(
            internet_rareness_json.get('rare_on_internet_summary_table_as_json_compressed_b64', ''))
        internet_rareness_summary_table_dict = safe_json_loads_func(internet_rareness_summary_table_json)
        internet_rareness_summary_table_df = pd.DataFrame.from_records(internet_rareness_summary_table_dict)
        skip_internet_rareness = False
    except Exception as e:
        log.error(f"Error processing internet_rareness_summary_table_json: {e}")
        skip_internet_rareness = True
    try:
        alternative_rare_on_internet_dict_as_json = decompress_and_decode_zstd_compressed_and_base64_encoded_string_func(
            internet_rareness_json.get('alternative_rare_on_internet_dict_as_json_compressed_b64', ''))
        alternative_rare_on_internet_dict = safe_json_loads_func(alternative_rare_on_internet_dict_as_json)
        alternative_rare_on_internet_dict_summary_table_df = pd.DataFrame.from_records(alternative_rare_on_internet_dict)
        skip_alternative_rare_on_internet = False
    except Exception as e:
        log.error(f"Error processing alternative_rare_on_internet_dict_as_json: {e}")
        skip_alternative_rare_on_internet = True
    try:
        parsed_dd_service_data.registration_ticket_txid = raw_dd_service_data.registration_ticket_txid
        parsed_dd_service_data.hash_of_candidate_image_file = final_response_df.get('hash_of_candidate_image_file', [None])[0]
        parsed_dd_service_data.pastel_id_of_submitter = final_response_df.get('pastel_id_of_submitter', [None])[0]
        parsed_dd_service_data.pastel_block_hash_when_request_submitted = final_response_df.get('pastel_block_hash_when_request_submitted', [None])[0]
        parsed_dd_service_data.pastel_block_height_when_request_submitted = str(final_response_df.get('pastel_block_height_when_request_submitted', [None])[0])
        parsed_dd_service_data.dupe_detection_system_version = str(final_response_df.get('dupe_detection_system_version', [None])[0])
        parsed_dd_service_data.candidate_image_thumbnail_webp_as_base64_string = str(final_response_df.get('candidate_image_thumbnail_webp_as_base64_string', [None])[0])
        parsed_dd_service_data.collection_name_string = str(final_response_df.get('collection_name_string', [None])[0])
        parsed_dd_service_data.open_api_group_id_string = str(final_response_df.get('open_api_group_id_string', [None])[0])
        parsed_dd_service_data.does_not_impact_the_following_collection_strings = str(final_response_df.get('does_not_impact_the_following_collection_strings', [None])[0])
        parsed_dd_service_data.overall_rareness_score = final_response_df.get('overall_rareness_score', final_response_df.get('overall_rareness_score ', [None]))[0]
        parsed_dd_service_data.group_rareness_score = final_response_df.get('group_rareness_score', [None])[0]
        parsed_dd_service_data.open_nsfw_score = final_response_df.get('open_nsfw_score', [None])[0] 
        parsed_dd_service_data.alternative_nsfw_scores = str(final_response_df.get('alternative_nsfw_scores', [None])[0])
        parsed_dd_service_data.utc_timestamp_when_request_submitted = final_response_df.get('utc_timestamp_when_request_submitted', [None])[0]
        parsed_dd_service_data.is_likely_dupe = str(final_response_df.get('is_likely_dupe', [None])[0])
        parsed_dd_service_data.is_rare_on_internet = str(final_response_df.get('is_rare_on_internet', [None])[0])
        parsed_dd_service_data.is_pastel_openapi_request = str(final_response_df.get('is_pastel_openapi_request', [None])[0])
        parsed_dd_service_data.is_invalid_sense_request = str(final_response_df.get('is_invalid_sense_request', [None])[0])
        parsed_dd_service_data.invalid_sense_request_reason = str(final_response_df.get('invalid_sense_request_reason', [None])[0])
        parsed_dd_service_data.corresponding_pastel_blockchain_ticket_data = str(raw_dd_service_data.corresponding_pastel_blockchain_ticket_data)
    except Exception as e:
        log.error(f'Error filling in parsed_dd_service_data fields: {e}')
        return cast_numpy_types_for_serialization(parsed_dd_service_data)
    try:
        parsed_dd_service_data.similarity_score_to_first_entry_in_collection = float(final_response_df.get('similarity_score_to_first_entry_in_collection', [None])[0])
        parsed_dd_service_data.cp_probability = float(final_response_df.get('cp_probability', [None])[0])
        parsed_dd_service_data.child_probability = float(final_response_df.get('child_probability', [None])[0])
        parsed_dd_service_data.image_file_path = str(final_response_df.get('image_file_path', [None])[0])
        parsed_dd_service_data.image_fingerprint_of_candidate_image_file = str(final_response_df.get('image_fingerprint_of_candidate_image_file', [None])[0])
        parsed_dd_service_data.pct_of_top_10_most_similar_with_dupe_prob_above_25pct = float(final_response_df.get('pct_of_top_10_most_similar_with_dupe_prob_above_25pct', [None])[0])
        parsed_dd_service_data.pct_of_top_10_most_similar_with_dupe_prob_above_33pct = float(final_response_df.get('pct_of_top_10_most_similar_with_dupe_prob_above_33pct', [None])[0])
        parsed_dd_service_data.pct_of_top_10_most_similar_with_dupe_prob_above_50pct = float(final_response_df.get('pct_of_top_10_most_similar_with_dupe_prob_above_50pct', [None])[0])
        parsed_dd_service_data.internet_rareness__min_number_of_exact_matches_in_page = str(internet_rareness_json.get('min_number_of_exact_matches_in_page', None))
        parsed_dd_service_data.internet_rareness__earliest_available_date_of_internet_results = internet_rareness_json.get('earliest_available_date_of_internet_results', None)
    except Exception as e:
        log.error(f'Error filling in the next part of parsed_dd_service_data fields: {e}')
        return cast_numpy_types_for_serialization(parsed_dd_service_data)
    try:
        if not skip_internet_rareness:
            parsed_dd_service_data.internet_rareness__b64_image_strings_of_in_page_matches = str(internet_rareness_summary_table_df.get('img_src_string', []).values.tolist())
            parsed_dd_service_data.internet_rareness__original_urls_of_in_page_matches = str(internet_rareness_summary_table_df.get('original_url', []).values.tolist())
            parsed_dd_service_data.internet_rareness__result_titles_of_in_page_matches = str(internet_rareness_summary_table_df.get('title', []).values.tolist())
            parsed_dd_service_data.internet_rareness__date_strings_of_in_page_matches = str(internet_rareness_summary_table_df.get('date_string', []).values.tolist())
            parsed_dd_service_data.internet_rareness__misc_related_images_as_b64_strings =  str(internet_rareness_summary_table_df.get('misc_related_image_as_b64_string', []).values.tolist())
            parsed_dd_service_data.internet_rareness__misc_related_images_url = str(internet_rareness_summary_table_df.get('misc_related_image_url', []).values.tolist())
        else:
            parsed_dd_service_data.internet_rareness__b64_image_strings_of_in_page_matches = '[]'
            parsed_dd_service_data.internet_rareness__original_urls_of_in_page_matches = '[]'
            parsed_dd_service_data.internet_rareness__result_titles_of_in_page_matches = '[]'
            parsed_dd_service_data.internet_rareness__date_strings_of_in_page_matches = '[]'
            parsed_dd_service_data.internet_rareness__misc_related_images_as_b64_strings = '[]'
            parsed_dd_service_data.internet_rareness__misc_related_images_url = '[]'
        if not skip_alternative_rare_on_internet:            
            parsed_dd_service_data.alternative_rare_on_internet__number_of_similar_results = str(len(alternative_rare_on_internet_dict_summary_table_df))
            parsed_dd_service_data.alternative_rare_on_internet__b64_image_strings = str(alternative_rare_on_internet_dict_summary_table_df.get('list_of_images_as_base64', []).values.tolist())
            parsed_dd_service_data.alternative_rare_on_internet__original_urls = str(alternative_rare_on_internet_dict_summary_table_df.get('list_of_href_strings', []).values.tolist())
            parsed_dd_service_data.alternative_rare_on_internet__google_cache_urls = str(alternative_rare_on_internet_dict_summary_table_df.get('list_of_image_src_strings', []).values.tolist())
            parsed_dd_service_data.alternative_rare_on_internet__alt_strings = str(alternative_rare_on_internet_dict_summary_table_df.get('list_of_image_alt_strings', []).values.tolist())
        else:
            parsed_dd_service_data.alternative_rare_on_internet__number_of_similar_results = '0'
            parsed_dd_service_data.alternative_rare_on_internet__b64_image_strings = '[]'
            parsed_dd_service_data.alternative_rare_on_internet__original_urls = '[]'
            parsed_dd_service_data.alternative_rare_on_internet__google_cache_urls = '[]'
            parsed_dd_service_data.alternative_rare_on_internet__alt_strings = '[]'
    except Exception as e:
        log.error(f'Error filling in the final part of parsed_dd_service_data fields: {e}')
        return cast_numpy_types_for_serialization(parsed_dd_service_data)
    return cast_numpy_types_for_serialization(parsed_dd_service_data)

async def check_for_parsed_dd_service_result_in_db_func(txid: str) -> Tuple[Optional[ParsedDDServiceData], bool]:
    try:
        async with db_session.create_async_session() as session:
            query = select(ParsedDDServiceData).filter(ParsedDDServiceData.registration_ticket_txid == txid)
            result = await session.execute(query)
        parsed_data = result.scalar_one_or_none()
        is_cached = parsed_data is not None
        return parsed_data, is_cached
    except Exception as e:
        log.error(f'Error checking for parsed data in DB: {e}')
        return None, False

async def save_parsed_dd_service_data_to_db_func(parsed_data: ParsedDDServiceData):
    try:
        await add_record_to_write_queue(parsed_data)
    except Exception as e:
        log.error(f'Error saving parsed data to DB: {e}')

async def get_parsed_dd_service_results_by_registration_ticket_txid_func(txid: str) -> Tuple[Optional[ParsedDDServiceData], bool]:
    start_time = time.time()
    parsed_data, is_cached = await check_for_parsed_dd_service_result_in_db_func(txid)
    if is_cached:
        return parsed_data, True
    raw_data, _ = await get_raw_dd_service_results_by_registration_ticket_txid_func(txid)
    parsed_data = await parse_raw_dd_service_data_func(raw_data)
    _, is_cached = await check_for_parsed_dd_service_result_in_db_func(txid)
    if not is_cached:
        await save_parsed_dd_service_data_to_db_func(parsed_data)
        elapsed_time = round(time.time() - start_time, 2)
        log.info(f'Parsed data generated and saved for txid {txid} in {elapsed_time}s.')
    return parsed_data, False

async def add_bad_txid_to_db_func(session, txid, type, reason):
    query = select(BadTXID).where(BadTXID.txid == txid).with_for_update()
    result = await session.execute(query)
    bad_txid_exists = result.scalars().first()
    if bad_txid_exists is None:
        new_bad_txid = BadTXID(txid=txid, ticket_type=type, reason_txid_is_bad=reason, failed_attempts=1, next_attempt_time=datetime.utcnow() + timedelta(days=1))
        session.add(new_bad_txid)
    else:
        bad_txid_exists.failed_attempts += 1
        if bad_txid_exists.failed_attempts == 2:
            bad_txid_exists.next_attempt_time = datetime.utcnow() + timedelta(days=3)
        else:
            bad_txid_exists.next_attempt_time = datetime.utcnow() + timedelta(days=1)
    await session.commit()

async def get_bad_txids_from_db_func(session, type):
    query = select(BadTXID).where(BadTXID.ticket_type == type, BadTXID.failed_attempts >= 1)
    result = await session.execute(query)
    bad_txids = result.scalars().all()
    return [bad_tx.txid for bad_tx in bad_txids]

async def check_bad_txid_exists(session, txid):
    query = select(BadTXID).where(BadTXID.txid == txid).with_for_update()
    result = await session.execute(query)
    return result.scalars().first()

async def update_bad_txid(session, bad_txid):
    bad_txid.failed_attempts += 1
    if bad_txid.failed_attempts == 3:
        bad_txid.next_attempt_time = datetime.utcnow() + timedelta(days=3)
    else:
        bad_txid.next_attempt_time = datetime.utcnow() + timedelta(days=1)
    await session.commit()

async def check_and_update_cascade_ticket_bad_txid_func(session, txid):
    bad_txid_exists = await check_bad_txid_exists(session, txid)
    if bad_txid_exists:
        if datetime.utcnow() < bad_txid_exists.next_attempt_time:
            return f"Error: txid {txid} is marked as bad and has not reached its next attempt time.", ""
        await update_bad_txid(session, bad_txid_exists)
    return None

async def startup_cascade_file_download_lock_cleanup_func():
    async with db_session.create_async_session() as session:
        lock_expiration_time = timedelta(minutes=10)
        current_time = datetime.utcnow()
        stale_locks_query = select(CascadeCacheFileLocks).where(CascadeCacheFileLocks.lock_created_at < (current_time - lock_expiration_time))
        result = await session.execute(stale_locks_query)
        stale_locks = result.scalars().all()
        for lock in stale_locks:
            session.delete(lock)
        await session.commit()
            
async def periodic_cascade_file_download_lock_cleanup_func():
    while True:
        await asyncio.sleep(600)  # Run every 10 minutes
        await startup_cascade_file_download_lock_cleanup_func()

async def acquire_cascade_file_lock(session, txid, lock_expiration_time):
    current_time = datetime.utcnow()
    lock = await session.get(CascadeCacheFileLocks, txid)
    if lock and current_time - lock.lock_created_at < lock_expiration_time:
        return False  # Lock exists and is not stale
    retry_count = 0
    max_retries = 5  # Choose an appropriate maximum number of retries
    initial_delay = 0.1
    backoff_factor = 2
    while retry_count < max_retries:
        try:
            if lock:  # Lock is stale, update it
                lock.lock_created_at = current_time
            else:  # Lock does not exist, create it
                lock = CascadeCacheFileLocks(txid=txid, lock_created_at=current_time)
                session.add(lock)
            await session.commit()
            return True
        except Exception as e:  # noqa: F841
            # log.error(f"Exception while attempting to acquire lock for txid {txid}: {e}")
            retry_count += 1
            backoff_time = min(60, initial_delay * (backoff_factor ** retry_count))  # 60 seconds max delay
            await asyncio.sleep(backoff_time)
    return False  # Return False if all retries fail

async def delete_cascade_file_lock_func(session, txid: str):
    lock_exists = await session.get(CascadeCacheFileLocks, txid)
    if lock_exists:
        await session.delete(lock_exists)
        await session.commit()

async def download_and_cache_cascade_file_func(txid: str, request_url: str, headers, cache_dir: str, retry_count=0, max_retries=3):
    lock_expiration_time = timedelta(minutes=10)
    async with db_session.create_async_session() as session:
        if not await acquire_cascade_file_lock(session, txid, lock_expiration_time):
            return None, None
    client_session = await session_manager.get_or_create_session()
    try:
        cache_file_path, decoded_response = None, None
        response = await client_session.get(request_url, headers=headers, timeout=600.0)
        body = response.content
        parsed_response = json.loads(body.decode())
        if parsed_response.get('name') == 'InternalServerError':
            log.error(f"Server error during download: {parsed_response.get('message')}")
            async with db_session.create_async_session() as session:
                await delete_cascade_file_lock_func(session, txid)
            return f"Server error: {parsed_response.get('message')}", None
        if 'file_id' in parsed_response.keys():
            file_identifier = parsed_response['file_id']
            file_download_url = f"http://localhost:8080/files/{file_identifier}"
            response = await client_session.get(file_download_url, headers=headers, timeout=500.0)
            decoded_response = response.content
            cache_file_path = os.path.join(cache_dir, txid)
            async with aiofiles.open(cache_file_path, mode='wb') as f:
                await f.write(decoded_response)
        else:
            log.error("Response did not contain a file_id.")
            async with db_session.create_async_session() as session:
                await delete_cascade_file_lock_func(session, txid)
            return "No file_id in response.", None
    except Exception as e:
        log.error(f'Exception during download: {e}')
        if retry_count < max_retries:
            return await download_and_cache_cascade_file_func(txid, request_url, headers, cache_dir, retry_count=retry_count + 1)
        else:
            async with db_session.create_async_session() as session:
                await delete_cascade_file_lock_func(session, txid)
                await add_bad_txid_to_db_func(session, txid, 'cascade', str(e))
            return f"Exception occurred: {e}", None
    return cache_file_path, decoded_response

async def check_and_manage_cascade_file_cache_func(txid: str, cache_file_path: str, decoded_response):
    cache[txid] = cache_file_path  # Update LRU cache
    log.info(f'Successfully decoded response from Cascade API for txid {txid}!')
    return decoded_response


async def get_cascade_original_file_metadata_func(txid: str):
    try:
        ticket_response = await get_pastel_blockchain_ticket_func(txid)
        action_ticket = json.loads(base64.b64decode(ticket_response['ticket']['action_ticket']))
        api_ticket_str = action_ticket['api_ticket']
        correct_padding = len(api_ticket_str) % 4
        if correct_padding != 0:
            api_ticket_str += '=' * (4 - correct_padding)
        api_ticket = json.loads(base64.b64decode(api_ticket_str).decode('utf-8'))
        original_file_name_string = api_ticket['file_name']
        is_publicly_accessible = api_ticket['make_publicly_accessible']
        publicly_accessible_description_string = "publicly accessible" if is_publicly_accessible else "private"
        base64_encoded_original_file_sha3_256_hash = api_ticket['data_hash']
        original_file_sha3_256_hash = base64.b64decode(base64_encoded_original_file_sha3_256_hash).hex()
        original_file_size_in_bytes = api_ticket['original_file_size_in_bytes']
        original_file_mime_type = api_ticket['file_type']
        log.info(f'Got original file name from the Cascade blockchain ticket with registration txid {txid}; Filename: {original_file_name_string}, a {publicly_accessible_description_string} "{original_file_mime_type}" file of size {round(original_file_size_in_bytes/1024/1024, 3)}mb and SHA3-256 hash "{original_file_sha3_256_hash}"')
        return original_file_name_string, is_publicly_accessible, original_file_sha3_256_hash, original_file_size_in_bytes, original_file_mime_type
    except Exception as e:
        log.warning('Unable to get original file name from the Cascade blockchain ticket! Using txid instead as the default file name...')
        await add_bad_txid_to_db_func(txid, 'cascade', str(e))
        return str(txid), False


async def verify_downloaded_cascade_file_func(file_data, expected_hash, expected_size, expected_mime_type):
    try:
        sha3_256 = hashlib.sha3_256(file_data).hexdigest()
        if sha3_256 != expected_hash:
            error_msg = f'Hash mismatch: expected {expected_hash}, got {sha3_256}'
            log.error(error_msg)
            return error_msg
        size = len(file_data)
        if size != expected_size:
            error_msg = f'Size mismatch: expected {expected_size} bytes, got {size} bytes'
            log.error(error_msg)
            return error_msg
        mime_type = magic.from_buffer(file_data, mime=True)
        expected_main_type, expected_sub_type = parse_mime_type(expected_mime_type)
        actual_main_type, actual_sub_type = parse_mime_type(mime_type)
        if (expected_main_type, expected_sub_type) != (actual_main_type, actual_sub_type):
            error_msg = f'MIME type mismatch: expected {expected_mime_type}, got {mime_type}'
            log.error(error_msg)
            return error_msg
        log.info(f'File verification successful for expected_hash: {expected_hash}, expected_size: {expected_size} bytes, expected_mime_type: {expected_mime_type}')
        return True
    except Exception as e:
        error_msg = f'An unexpected error occurred during file verification: {e}'
        log.error(error_msg)
        return error_msg


async def download_publicly_accessible_cascade_file_by_registration_ticket_txid_func(txid: str, use_cascade_file_cache: bool = True):
    global cache
    log.info(f'Starting download process for txid {txid}.')
    try:
        await load_cache()
        start_time = time.time()
        request_url = f'http://localhost:8080/openapi/cascade/download?pid={REQUESTER_PASTELID}&txid={txid}'
        headers = {'Authorization': REQUESTER_PASTELID_PASSPHRASE}
        async with db_session.create_async_session() as session:
            check_bad_txid_result = await check_and_update_cascade_ticket_bad_txid_func(session, txid)
            if check_bad_txid_result:
                log.warning(f'TXID {txid} is marked as bad.')
                return check_bad_txid_result, ""
            try:
                original_file_name_string, is_publicly_accessible, original_file_sha3_256_hash, original_file_size_in_bytes, original_file_mime_type = await get_cascade_original_file_metadata_func(txid)
            except Exception as e:
                log.warning(f'An error occurred while fetching metadata for txid {txid}. Error: {e}')
                original_file_name_string = txid  # Use txid as filename if we can't fetch metadata
            if not is_publicly_accessible:
                log.warning(f'The file for the Cascade ticket with registration txid {txid} is not publicly accessible!')
                return f'The file for the Cascade ticket with registration txid {txid} is not publicly accessible!', ""
            if use_cascade_file_cache:
                if txid in cache and os.path.exists(cache[txid]):  
                    log.info(f"File is already cached, returning the cached file for txid {txid}...")
                    async with aiofiles.open(cache[txid], mode='rb') as f:
                        decoded_response = await f.read()
                    return decoded_response, original_file_name_string
            log.info(f'Now attempting to download the file from Cascade API for txid {txid}...')
            cache_file_path, decoded_response = await download_and_cache_cascade_file_func(txid, request_url, headers, session, cache_dir)
            if cache_file_path is None or decoded_response is None:
                log.warning(f"Skipping file download for txid {txid} as it's already in progress.")
                return "File download in progress.", ""                          
            if isinstance(decoded_response, str) and decoded_response.startswith("Error"):
                log.error(f'An error occurred while downloading the file for txid {txid}. Error: {decoded_response}')
                return decoded_response, ""               
            decoded_response = await check_and_manage_cascade_file_cache_func(txid, cache_file_path, decoded_response)
            try:
                verification_result = await verify_downloaded_cascade_file_func(decoded_response, original_file_sha3_256_hash, original_file_size_in_bytes, original_file_mime_type)
                log.info(f'Finished download process for txid {txid} in {time.time() - start_time} seconds.')
                if verification_result:
                    return decoded_response, original_file_name_string                
                else:
                    log.error(f'File verification failed for txid {txid}. Error: {verification_result}')
                    return verification_result, ""
            except Exception as e:
                log.error(f'An error occurred while verifying the file for txid {txid}. Error: {e}')
        return f"An error occurred while downloading the file for txid {txid}.", ""
    except Exception as e:
        log.error(f'An unexpected error occurred during download process for txid {txid}. Error: {e}')
        raise


async def filter_tickets_to_dataframes(tickets_obj):
    sense_ticket_dict = json.loads(tickets_obj['action'])
    sense_ticket_df = pd.DataFrame(sense_ticket_dict).T
    sense_ticket_df_filtered = sense_ticket_df[sense_ticket_df['action_type'] == 'sense'].drop_duplicates(subset=['txid'])
    nft_ticket_dict = json.loads(tickets_obj['nft'])
    nft_ticket_df = pd.DataFrame(nft_ticket_dict).T
    nft_ticket_df_filtered = nft_ticket_df.drop_duplicates(subset=['txid'])
    return sense_ticket_df_filtered, nft_ticket_df_filtered

def shuffle_and_combine_txids(sense_ticket_df, nft_ticket_df):
    sense_txids = sense_ticket_df['txid'].values.tolist()
    nft_txids = nft_ticket_df['txid'].values.tolist()
    combined_txids = sense_txids + nft_txids
    random.shuffle(combined_txids)
    return combined_txids

async def get_ticket_height(txid):
    ticket_data = await get_pastel_blockchain_ticket_func(txid)
    return ticket_data['height']

async def should_skip_txid(txid, is_nft_ticket, min_height_for_nft, min_height_for_sense):
    if is_nft_ticket and await get_ticket_height(txid) <= min_height_for_nft:
        return True
    if not is_nft_ticket and await get_ticket_height(txid) <= min_height_for_sense:
        return True
    return False

async def populate_database_with_all_dd_service_data_func():
    try:
        tickets_obj = await get_all_pastel_blockchain_tickets_func()
        sense_ticket_df, nft_ticket_df = await filter_tickets_to_dataframes(tickets_obj)
    except JSONRPCException as e:
        log.error(f"JSONRPC error: {str(e)}")
        return
    except json.JSONDecodeError as e:
        log.error(f"JSON Decode Error: {str(e)}")
        return
    except asyncio.TimeoutError:
        log.error("Timeout while fetching tickets.")
        return
    except Exception as e:
        log.error(f"Unexpected error: {str(e)}")
        return
    min_height_for_nft = 335000
    min_height_for_sense = 335000
    combined_txids = shuffle_and_combine_txids(sense_ticket_df, nft_ticket_df)
    random_delay_in_seconds = random.randint(1, 15)
    await asyncio.sleep(random_delay_in_seconds)
    async with db_session.create_async_session() as session:
        for txid in combined_txids:
            try:
                is_nft_ticket = txid in nft_ticket_df['txid'].values.tolist()
                ticket_type = 'nft' if is_nft_ticket else 'sense'
                if txid in await get_bad_txids_from_db_func(session, ticket_type):
                    continue
                if await should_skip_txid(txid, is_nft_ticket, min_height_for_nft, min_height_for_sense):
                    continue
                data, is_cached = await get_parsed_dd_service_results_by_registration_ticket_txid_func(txid)
            except JSONRPCException as e:
                await add_bad_txid_to_db_func(session, txid, ticket_type, str(e))
                log.error(f"JSONRPC error while processing txid {txid}: {str(e)}")
            except asyncio.TimeoutError:
                log.error(f"Async TimeoutError while processing txid {txid}")
            except OperationalError:
                log.error(f"Database operational error while processing txid {txid}")
            except IntegrityError:
                log.error(f"Database integrity error while processing txid {txid}")
            except DBAPIError:
                log.error(f"Database API error while processing txid {txid}")
            except Exception as e:
                log.error(f"Unexpected error while processing txid {txid}: {str(e)}")

def print_dict_structure(d, indent=0):
    for key, value in d.items():
        log.info('\t' * indent + str(key))
        if isinstance(value, dict):
            print_dict_structure(value, indent+1)
        elif isinstance(value, list):
            if value:  # check if list is not empty
                if isinstance(value[0], dict):
                    log.info('\t' * (indent+1) + str(type(value[0])))
                    print_dict_structure(value[0], indent+2)
                else:
                    log.info('\t' * (indent+1) + str(type(value[0])))
            else:
                log.info('\t' * (indent+1) + 'list is empty')
        else:
            log.info('\t' * (indent+1) + str(type(value)))


async def convert_dict_to_make_it_safe_for_json_func(combined_output_dict):
    def convert(item):
        if isinstance(item, (pd.Timestamp, pd._libs.tslibs.timestamps.Timestamp, datetime)):
            return item.isoformat()
        elif item == "NA":
            return None # Convert "NA" strings into None
        elif isinstance(item, pd._libs.tslibs.nattype.NaTType) or item is None:
            return "None" # or you can return some other value that signifies None in your use case
        elif isinstance(item, (np.int64, np.int32, np.float64, np.float32, int, float)):
            return item.item() if hasattr(item, 'item') else item
        elif isinstance(item, dict):
            return {k: convert(v) for k, v in item.items()}
        elif isinstance(item, list):
            return [convert(elem) for elem in item]
        else:
            return str(item)
    converted_dict = {k: convert(v) for k, v in combined_output_dict.items()}
    return converted_dict


async def get_random_cascade_txids_func(n: int) -> List[str]:
    min_block_height = 315000
    log.info(f'Attempting to get {n} random cascade TXIDs...')
    tickets_obj = await get_all_pastel_blockchain_tickets_func()
    cascade_ticket_dict = json.loads(tickets_obj['action'])
    cascade_ticket_dict_df = pd.DataFrame(cascade_ticket_dict).T
    cascade_ticket_dict_df_filtered = cascade_ticket_dict_df[cascade_ticket_dict_df['action_type'] == 'cascade'].drop_duplicates(subset=['txid'])
    txids = cascade_ticket_dict_df_filtered['txid'].tolist()
    if n < 10:
        txids_sample = random.sample(txids, min(20*n, len(txids))) # Sample more TXIDs than we need to increase the chance of getting enough publicly accessible ones.
    else:
        txids_sample = random.sample(txids, min(8*n, len(txids)))
    accessible_txids = []
    for txid in txids_sample:
        try:
            ticket_response = await get_pastel_blockchain_ticket_func(txid)
            action_ticket = json.loads(base64.b64decode(ticket_response['ticket']['action_ticket']))
            if action_ticket['blocknum'] < min_block_height:
                continue
            api_ticket_str = action_ticket['api_ticket']
            correct_padding = len(api_ticket_str) % 4
            if correct_padding != 0:
                api_ticket_str += '=' * (4 - correct_padding)
            api_ticket = json.loads(base64.b64decode(api_ticket_str).decode('utf-8'))
            if api_ticket['make_publicly_accessible']:
                accessible_txids.append(txid)
        except Exception as e:
            log.error(f'An error occurred while checking if txid {txid} is publicly accessible! Error message: {e}')
    accessible_txids = [x for x in accessible_txids if len(x) == 64 and x.isalnum()]
    accessible_txids = list(set(accessible_txids))  # convert list to set to remove duplicates, then convert back to list
    if len(accessible_txids) < n:
        log.warning('Not enough publicly accessible TXIDs for the requested number. Returning all that were found.')
    return random.sample(accessible_txids, min(n, len(accessible_txids)))
    
    
async def download_cascade_file_test_func(txid: str) -> dict:
    try:
        start_time = datetime.utcnow()
        use_cascade_file_cache = False
        result = await download_publicly_accessible_cascade_file_by_registration_ticket_txid_func(txid, use_cascade_file_cache)
        end_time = datetime.utcnow()
        file_data, file_name = result
        file_size_in_megabytes = len(file_data)/(1024.0**2)
        download_speed_in_mb_per_second = file_size_in_megabytes / (end_time - start_time).total_seconds()
        log.info(f'Successfully finished test download for txid {txid}.')
        return {
            'txid': txid,
            'datetime_started': start_time,
            'datetime_finished': end_time,
            'file_size_in_megabytes': round(file_size_in_megabytes, 3),
            'file_name': file_name,
            'download_time': round(float((end_time - start_time).total_seconds()),3),
            'download_speed_in_mb_per_second': round(download_speed_in_mb_per_second,3)
        }
    except Exception as e:
        log.error(f"Exception occurred in download_cascade_file_test_func for txid {txid}: {e}")
        
        
def get_walletnode_log_data_func():
    log_file_path = '/home/ubuntu/.pastel/walletnode.log'
    command = f'tail -n 300 {log_file_path}'
    try:
        process = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
        output, _ = process.communicate()
        if process.returncode != 0:
            log.error('Non-zero exit code received.')
            return None
        return output.decode('utf-8')
    except Exception as e:
        log.error(f'Error: {e}')
        return None
    
    
def parse_walletnode_log_data_func(log_data: str):
    if log_data is None:
        log.error('Log data is None.')
        return [], {}
    log_pattern = r'\[(.*?)\]\s+INFO\s+(.*?):\s+(.*?)\sserver_ip=(.*?)\s(.*?)$'
    txid_pattern = r'txid=(\w+)'
    downloaded_pattern = r'Downloaded from supernode address=(.*?)\s'
    parsed_data = []
    supernode_ips_dict = {} 
    current_index = {}
    current_year = datetime.utcnow().year
    for line in log_data.split('\n'):
        match = re.match(log_pattern, line)
        if match:
            datetime_str = match.group(1) + f" {current_year}"
            log_datetime = pd.to_datetime(datetime_str, format='%b %d %H:%M:%S.%f %Y', errors='coerce')
            additional_data = match.group(5)
            txid_match = re.search(txid_pattern, additional_data)
            if txid_match:
                txid = txid_match.group(1)
                if txid not in current_index:
                    current_index[txid] = 1
                else:
                    current_index[txid] += 1
                data = {
                    'index': current_index[txid],
                    'timestamp': log_datetime,
                    'message': match.group(3),
                    'txid': txid,
                }
                if 'Downloaded from supernode' in line:
                    supernode_match = re.search(downloaded_pattern, line)
                    if supernode_match:
                        supernode_ip = supernode_match.group(1)
                        if txid not in supernode_ips_dict:
                            supernode_ips_dict[txid] = []
                        supernode_ips_dict[txid].append(supernode_ip)
                parsed_data.append(data)
    parsed_data_df = pd.DataFrame(parsed_data)
    return parsed_data_df, supernode_ips_dict


def update_cascade_status_func(df):
    df = df.copy()
    log_data = get_walletnode_log_data_func()
    parsed_log_data, supernode_ips_dict = parse_walletnode_log_data_func(log_data)
    txid_log_dict = {}
    for idx, row in parsed_log_data.iterrows():
        txid = row['txid']
        log_line = {'index': row['index'], 'datetime': row['timestamp'], 'message': row['message']}
        if txid in txid_log_dict:
            txid_log_dict[txid]['log_lines'].append(log_line)
        else:
            txid_log_dict[txid] = {'last_log_status': row['message'], 'log_lines': [log_line]}
        if 'txid' in df.columns and txid in df['txid'].values:
            df.loc[df['txid'] == txid, 'last_log_status'] = row['message']
            df.loc[df['txid'] == txid, 'supernode_ips'] = ", ".join(supernode_ips_dict.get(txid, []))
            if row['message'] == 'Start downloading':
                df.loc[df['txid'] == txid, 'datetime_started'] = row['timestamp']
            elif row['message'] == 'Finished downloading':
                df.loc[df['txid'] == txid, 'datetime_finished'] = row['timestamp']
        else:
            new_row = {'index': row['index'], 'timestamp': row['timestamp'], 'message': row['message'], 'txid': txid, 'supernode_ips': ", ".join(supernode_ips_dict.get(txid, []))}
            if not isinstance(df, pd.DataFrame):
                log.warning(f"df is not a DataFrame at the point of append. It is: {type(df)}")
                return pd.DataFrame([new_row]), {}, {}
            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    return df, txid_log_dict, supernode_ips_dict


async def update_cascade_status_periodically_func(df_container):
    txid_log_dict = {}
    supernode_ips_dict = {}
    while True:
        if not isinstance(df_container[0], pd.DataFrame):
            log.warning(f"df_container[0] is not a DataFrame. It is: {type(df_container[0])}")
        new_df, new_txid_log_dict, new_supernode_ips_dict = update_cascade_status_func(df_container[0])
        async with df_lock:
            if new_df is not None and isinstance(new_df, pd.DataFrame):
                df_container[0] = new_df.copy()
            else:
                log.warning("new_df is not a DataFrame.")
            txid_log_dict = {**new_txid_log_dict}
            supernode_ips_dict = {**new_supernode_ips_dict}
        await asyncio.sleep(0.5)
    return df_container[0].copy(), txid_log_dict.copy(), supernode_ips_dict.copy()


def merge_and_resolve_columns(df1, df2, key_column):
    merged_df = pd.merge(df1, df2, on=key_column, how='left', suffixes=('_x', '_y'))
    for col in df1.columns:
        if col == key_column:
            continue
        if f"{col}_x" in merged_df.columns and f"{col}_y" in merged_df.columns:
            def resolve(row, col):
                x = row[col + '_x']
                y = row[col + '_y']
                if pd.isna(x) and pd.isna(y):
                    return None
                elif pd.isna(x):
                    return y
                elif pd.isna(y):
                    return x
                elif isinstance(x, str) and isinstance(y, str):
                    return x if len(x) > len(y) else y
                elif isinstance(x, pd.Timestamp) and isinstance(y, pd.Timestamp):
                    return pd.Timestamp((x.value + y.value) / 2)
                else:
                    return (x + y) / 2
            merged_df[col] = merged_df.apply(lambda row: resolve(row, col), axis=1)
            merged_df.drop([col + '_x', col + '_y'], axis=1, inplace=True)
    return merged_df


async def perform_bulk_cascade_test_download_tasks_func(txids: list, seconds_to_wait_for_all_files_to_finish_downloading: int):
    try:
        df_container = [pd.DataFrame()]
        txid_log_dict = {}
        supernode_ips_dict = {}
        update_task = asyncio.create_task(update_cascade_status_periodically_func(df_container))
        await asyncio.sleep(2)
        download_tasks = [asyncio.create_task(download_cascade_file_test_func(txid)) for txid in txids]
        log.info('Waiting for download tasks to finish...')
        finished, unfinished = await asyncio.wait(download_tasks, timeout=seconds_to_wait_for_all_files_to_finish_downloading)
        download_data = []
        for t in finished:
            try:
                result = t.result()
                if result is not None and 'txid' in result:
                    download_data.append(result)
            except Exception as e:
                log.error(f"Task failed: {e}")        
        update_task.cancel()
        try:
            await asyncio.wait([update_task], timeout=600)
            async with df_lock:
                if update_task.done():
                    new_df, new_txid_log_dict, new_supernode_ips_dict = update_task.result()
                    if not isinstance(new_df, pd.DataFrame):
                        log.warning(f"new_df is not a DataFrame. It is: {type(new_df)}")
                    df_container[0] = new_df
                    txid_log_dict = new_txid_log_dict
                    supernode_ips_dict = new_supernode_ips_dict
        except asyncio.exceptions.TimeoutError:
            log.warning('Update task did not finish within the expected time.')
        except asyncio.exceptions.CancelledError:
            async with df_lock:
                log.info('The status update task was cancelled.')
        log.info('All download tasks finished.')
        download_df = pd.DataFrame(download_data)
        download_df['log_lines'] = download_df['txid'].map(txid_log_dict)
        download_df['supernode_ips'] = download_df['txid'].apply(lambda x: ", ".join(supernode_ips_dict.get(x, [])))
        download_df['txid'] = download_df['txid'].astype(str)
        async with df_lock:
            if isinstance(df_container[0], pd.DataFrame):
                df = merge_and_resolve_columns(download_df, df_container[0], 'txid')
        if 'datetime_finished' in df.columns and 'datetime_started' in df.columns:
            df['time_elapsed'] = (df['datetime_finished'] - df['datetime_started']).dt.total_seconds()
        else:
            df['time_elapsed'] = None
        return df, txid_log_dict, supernode_ips_dict
    except Exception as e:
        log.error(f'Error occurred while performing download tasks: {e}', exc_info=True)
        return None, None, None


def create_bulk_cascade_test_summary_stats_func(df: pd.DataFrame, seconds_to_wait_for_all_files_to_finish_downloading: int, number_of_concurrent_downloads: int):
    finished_before_timeout = df[df['time_elapsed'] < seconds_to_wait_for_all_files_to_finish_downloading].shape[0]
    finished_within_one_min = df[df['time_elapsed'] < 60].shape[0]
    if 'supernode_ips' in df.columns:
        all_supernode_ips = [ip.strip() for sublist in df['supernode_ips'].str.split(',').tolist() for ip in sublist if ip != '']
    else:
        all_supernode_ips = []
    if 'file_size_in_megabytes' in df.columns:
        average_file_size_in_megabytes = df['file_size_in_megabytes'].mean().round(3)
        max_file_size_in_megabytes = df['file_size_in_megabytes'].max().round(3)
    else:
        average_file_size_in_megabytes = np.nan    
        max_file_size_in_megabytes = np.nan
    if 'download_speed_in_mb_per_second' in df.columns:
        median_download_speed_in_mb_per_second = df['download_speed_in_mb_per_second'].median().round(3)
    else:
        median_download_speed_in_mb_per_second = np.nan                
    supernode_ip_counts = dict(Counter(all_supernode_ips))
    total_counts = sum(supernode_ip_counts.values())
    supernode_ip_percentages = {ip: round((count/total_counts)*100, 2) for ip, count in supernode_ip_counts.items()}
    summary_df = pd.DataFrame({
        'total_number_of_files_tested': number_of_concurrent_downloads,
        'number_of_files_downloaded': [len(df)],
        'finished_before_timeout': [finished_before_timeout],
        'percentage_finished_before_timeout': [round(finished_before_timeout / number_of_concurrent_downloads * 100, 3)],
        'finished_within_one_min': [finished_within_one_min],
        'percentage_finished_within_one_minute': [round(finished_within_one_min / number_of_concurrent_downloads * 100, 3)],
        'average_file_size_in_megabytes': [average_file_size_in_megabytes],
        'max_file_size_in_megabytes': [max_file_size_in_megabytes],
        'median_download_speed_in_mb_per_second': [median_download_speed_in_mb_per_second],
        'supernode_ip_counts': [supernode_ip_counts],
        'supernode_ip_percentages': [supernode_ip_percentages],
    })
    return summary_df


async def bulk_test_cascade_func(n: int):
    combined_output_dict = {}
    datetime_test_started = datetime.utcnow()
    try:
        log.info('Starting bulk test of cascade...')
        txids = await get_random_cascade_txids_func(n)
        log.info(f'Got {len(txids)} txids for testing.')
        seconds_to_wait_for_all_files_to_finish_downloading = 600
        df, txid_log_dict, supernode_ips = await perform_bulk_cascade_test_download_tasks_func(txids, seconds_to_wait_for_all_files_to_finish_downloading)
    except Exception as e:
        log.error(f'Error occurred in perform_bulk_cascade_test_download_tasks_func: {e}', exc_info=True)
        df = None
    if df is not None:
        if not isinstance(df, pd.DataFrame):
            log.error(f"df is not a DataFrame. It is: {type(df)}")
        else:
            summary_df = create_bulk_cascade_test_summary_stats_func(df.copy(), seconds_to_wait_for_all_files_to_finish_downloading, n)
            log.info('Processing test results...')
            async with df_lock:
                df_dict = df.copy().replace([np.inf, -np.inf], np.nan).fillna('NA').to_dict('records')
            summary_dict = summary_df.replace([np.inf, -np.inf], np.nan).fillna('NA').to_dict('records')
            combined_output_dict = {
                'datetime_test_started': datetime_test_started.strftime('%Y-%m-%dT%H:%M:%S'),
                'duration_of_test_in_seconds': round((datetime.utcnow() - datetime_test_started).total_seconds(), 2),
                'cascade_bulk_download_test_results__data': df_dict,
                'cascade_bulk_download_test_results__summary': summary_dict
            }
    else:
        combined_output_dict = {
            'datetime_test_started': datetime_test_started.strftime('%Y-%m-%dT%H:%M:%S'),
            'duration_of_test_in_seconds': round((datetime.utcnow() - datetime_test_started).total_seconds(), 2),
            'error': 'perform_bulk_cascade_test_download_tasks_func failed'
        }
    log.info('Finished bulk test of cascade.')
    combined_output_dict = await convert_dict_to_make_it_safe_for_json_func(combined_output_dict)
    return combined_output_dict


async def run_populate_database_with_all_dd_service_data_func():
    await populate_database_with_all_dd_service_data_func()
    return {"message": 'Started background task to populate database with all sense data...'}


async def get_parsed_dd_service_results_by_image_file_hash_func(image_file_hash: str) -> Optional[List[ParsedDDServiceData]]:
    async with db_session.create_async_session() as session: #First check if we already have the results in our local sqlite database:
        query = select(ParsedDDServiceData).filter(ParsedDDServiceData.hash_of_candidate_image_file == image_file_hash)
        result = await session.execute(query)
    results_already_in_local_db = result.scalars()
    list_of_results = list({r for r in results_already_in_local_db})
    if results_already_in_local_db is not None:
        return list_of_results
    else: 
        error_string = 'Cannot find a Sense or NFT registration ticket with that image file hash-- it might still be processing or it might not exist!'
        log.error(error_string)
        return error_string


async def get_raw_dd_service_results_by_image_file_hash_func(image_file_hash: str) ->  Optional[List[RawDDServiceData]]:
    async with db_session.create_async_session() as session: #First check if we already have the results in our local sqlite database:
        query = select(RawDDServiceData).filter(RawDDServiceData.hash_of_candidate_image_file == image_file_hash)
        result = await session.execute(query)
    results_already_in_local_db = result.scalars()
    list_of_results = list({r for r in results_already_in_local_db})
    if results_already_in_local_db is not None:
        return list_of_results
    else:
        error_string = 'Cannot find a Sense or NFT registration ticket with that image file hash-- it might still be processing or it might not exist!'
        log.error(error_string)
        return error_string
    

async def get_parsed_dd_service_results_by_pastel_id_of_submitter_func(pastel_id_of_submitter: str) ->  Optional[List[ParsedDDServiceData]]:
    async with db_session.create_async_session() as session: #First check if we already have the results in our local sqlite database:
        query = select(ParsedDDServiceData).filter(ParsedDDServiceData.pastel_id_of_submitter == pastel_id_of_submitter)
        result = await session.execute(query)
    results_already_in_local_db = result.scalars()
    list_of_results = list({r for r in results_already_in_local_db})
    if results_already_in_local_db is not None:
        return list_of_results
    else: 
        error_string = 'Cannot find any Sense or NFT registration tickets for that PastelID-- they might still be processing or they might not exist!'
        log.error(error_string)
        return error_string


async def get_raw_dd_service_results_by_pastel_id_of_submitter_func(pastel_id_of_submitter: str) -> Optional[List[RawDDServiceData]]:
    async with db_session.create_async_session() as session: #First check if we already have the results in our local sqlite database:
        query = select(RawDDServiceData).filter(RawDDServiceData.pastel_id_of_submitter == pastel_id_of_submitter)
        result = await session.execute(query)
    results_already_in_local_db = result.scalars()
    list_of_results = list({r for r in results_already_in_local_db})
    if results_already_in_local_db is not None:
        return list_of_results
    else:
        error_string = 'Cannot find any Sense or NFT registration tickets for that PastelID-- they might still be processing or they might not exist!'
        log.error(error_string)
        return error_string
    

async def get_parsed_dd_service_results_by_pastel_block_hash_when_request_submitted_func(pastel_block_hash_when_request_submitted: str) ->  Optional[List[ParsedDDServiceData]]:
    async with db_session.create_async_session() as session: #First check if we already have the results in our local sqlite database:
        query = select(ParsedDDServiceData).filter(ParsedDDServiceData.pastel_block_hash_when_request_submitted == pastel_block_hash_when_request_submitted)
        result = await session.execute(query)
    results_already_in_local_db = result.scalars()
    list_of_results = list({r for r in results_already_in_local_db})
    if results_already_in_local_db is not None:
        return list_of_results
    else: 
        error_string = 'Cannot find any Sense or NFT registration tickets for that block hash when submitted-- they might still be processing or they might not exist!'
        log.error(error_string)
        return error_string


async def get_raw_dd_service_results_by_pastel_block_hash_when_request_submitted_func(pastel_block_hash_when_request_submitted: str) -> Optional[List[RawDDServiceData]]:
    async with db_session.create_async_session() as session: #First check if we already have the results in our local sqlite database:
        query = select(RawDDServiceData).filter(RawDDServiceData.pastel_block_hash_when_request_submitted == pastel_block_hash_when_request_submitted)
        result = await session.execute(query)
    results_already_in_local_db = result.scalars()
    list_of_results = list({r for r in results_already_in_local_db})
    if results_already_in_local_db is not None:
        return list_of_results
    else:
        error_string = 'Cannot find any Sense or NFT registration tickets for that block hash when submitted-- they might still be processing or they might not exist!'
        log.error(error_string)
        return error_string


async def get_parsed_dd_service_results_by_pastel_block_height_when_request_submitted_func(pastel_block_height_when_request_submitted: str) ->  Optional[List[ParsedDDServiceData]]:
    async with db_session.create_async_session() as session: #First check if we already have the results in our local sqlite database:
        query = select(ParsedDDServiceData).filter(ParsedDDServiceData.pastel_block_height_when_request_submitted == pastel_block_height_when_request_submitted)
        result = await session.execute(query)
    results_already_in_local_db = result.scalars()
    list_of_results = list({r for r in results_already_in_local_db})
    if results_already_in_local_db is not None:
        return list_of_results
    else:
        error_string = 'Cannot find any Sense or NFT registration tickets for that block height when submitted -- they might still be processing or they might not exist!'
        log.error(error_string)
        return error_string


async def get_raw_dd_service_results_by_pastel_block_height_when_request_submitted_func(pastel_block_height_when_request_submitted: str) -> Optional[List[RawDDServiceData]]:
    async with db_session.create_async_session() as session: #First check if we already have the results in our local sqlite database:
        query = select(RawDDServiceData).filter(RawDDServiceData.pastel_block_height_when_request_submitted == pastel_block_height_when_request_submitted)
        result = await session.execute(query)
    results_already_in_local_db = result.scalars()
    list_of_results = list({r for r in results_already_in_local_db})
    if results_already_in_local_db is not None:
        return list_of_results
    else: 
        error_string = 'Cannot find any Sense or NFT registration tickets for that block height when submitted -- they might still be processing or they might not exist!'
        log.error(error_string)
        return error_string
        

async def get_current_total_number_of_registered_sense_fingerprints_func():
    tickets_obj = await get_all_pastel_blockchain_tickets_func()
    sense_ticket_dict = json.loads(tickets_obj['action'])
    sense_ticket_df = pd.DataFrame(sense_ticket_dict).T
    sense_ticket_df_filtered = sense_ticket_df[sense_ticket_df['action_type'] == 'sense'].drop_duplicates(subset=['txid'])
    list_of_sense_action_tickets = sense_ticket_df_filtered['action_ticket'].values.tolist()
    boolean_filter = [len(x) > 2000 for x in list_of_sense_action_tickets]
    sense_ticket_df_filtered = sense_ticket_df_filtered[boolean_filter]
    list_of_sense_registration_ticket_txids = sense_ticket_df_filtered['txid'].values.tolist()
    fingerprint_counter = len(list_of_sense_registration_ticket_txids)
    current_datetime_utc = datetime.utcnow()
    current_datetime_utc_string = current_datetime_utc.strftime("%Y-%m-%d %H:%M:%S")
    timestamp = int(datetime.timestamp(current_datetime_utc))
    response = {'total_number_of_registered_sense_fingerprints': fingerprint_counter, 'as_of_datetime_utc_string': current_datetime_utc_string, 'as_of_timestamp': timestamp}
    return response


async def get_all_registration_ticket_txids_corresponding_to_a_collection_ticket_txid_func(collection_ticket_txid: str):
    global rpc_connection
    ticket_dict = await get_pastel_blockchain_ticket_func(collection_ticket_txid)
    ticket_type_string = ticket_dict['ticket']['type']
    if ticket_type_string == 'collection-reg':
        activation_ticket_data = ticket_dict['activation_ticket'] 
        item_type = ticket_dict['ticket']['collection_ticket']['item_type']
    elif ticket_type_string == 'collection-act':
        activation_ticket_data = ticket_dict
        item_type = ''
    else:
        error_string = 'The ticket type is neither collection-reg nor collection-act'
        log.error(error_string)
        return error_string
    activation_ticket_txid = activation_ticket_data['txid']
    if item_type == 'sense':
        response_json = await rpc_connection.tickets('find', 'action', activation_ticket_txid )
    elif item_type == 'nft':
        response_json = await rpc_connection.tickets('find', 'nft', activation_ticket_txid )
    elif item_type == '':
        try:
            response_json = await rpc_connection.tickets('find', 'action', activation_ticket_txid )
        except Exception as e:
            log.error(f'Exception occurred while trying to find the activation ticket in the blockchain: {e}')
            try:
                response_json = await rpc_connection.tickets('find', 'nft', activation_ticket_txid )
            except Exception as e:
                response_json = 'Unable to find the activation ticket in the blockchain'
    else:
        response_json = f'The txid given ({collection_ticket_txid}) is not a valid activation ticket txid for a collection ticket'
    return response_json
        

async def get_current_total_number_and_size_and_average_size_of_registered_cascade_files_func():
    tickets_obj = await get_all_pastel_blockchain_tickets_func()
    cascade_ticket_dict = json.loads(tickets_obj['action'])
    cascade_ticket_df = pd.DataFrame(cascade_ticket_dict).T
    cascade_ticket_df_filtered = cascade_ticket_df[cascade_ticket_df['action_type'] == 'cascade'].drop_duplicates(subset=['txid'])
    list_of_cascade_registration_ticket_txids = cascade_ticket_df_filtered['txid'].values.tolist()
    list_of_cascade_action_tickets = cascade_ticket_df_filtered['action_ticket'].values.tolist()
    file_counter = 0
    data_size_bytes_counter = 0
    publicly_accessible_files = 0
    publicly_accessible_bytes = 0
    file_type_counter = {}
    for idx, current_txid in enumerate(list_of_cascade_registration_ticket_txids):
        current_api_ticket = list_of_cascade_action_tickets[idx]
        decoded_action_ticket = json.loads(base64.b64decode(current_api_ticket).decode('utf-8'))
        api_ticket = decoded_action_ticket['api_ticket']
        api_ticket += "=" * (-len(api_ticket) % 4)
        api_ticket_decoded = json.loads(base64.b64decode(api_ticket).decode('utf-8'))
        if api_ticket_decoded is not None:
            if len(api_ticket_decoded) == 9:
                file_counter += 1
                file_size = api_ticket_decoded['original_file_size_in_bytes']
                data_size_bytes_counter += file_size
                if api_ticket_decoded['make_publicly_accessible']:
                    publicly_accessible_files += 1
                    publicly_accessible_bytes += file_size
                file_type = api_ticket_decoded['file_type']
                if file_type not in file_type_counter:
                    file_type_counter[file_type] = {'count': 0, 'size': 0}
                file_type_counter[file_type]['count'] += 1
                file_type_counter[file_type]['size'] += file_size
    for key in file_type_counter:
        file_type_counter[key]['percentage_files'] = (file_type_counter[key]['count'] / file_counter) * 100
        file_type_counter[key]['percentage_size'] = (file_type_counter[key]['size'] / data_size_bytes_counter) * 100
    percentage_publicly_accessible_files = (publicly_accessible_files / file_counter) * 100
    percentage_publicly_accessible_bytes = (publicly_accessible_bytes / data_size_bytes_counter) * 100
    average_file_size_in_bytes = round(data_size_bytes_counter/file_counter,3)
    current_datetime_utc = datetime.utcnow()
    current_datetime_utc_string = current_datetime_utc.strftime("%Y-%m-%d %H:%M:%S")
    timestamp = int(datetime.timestamp(current_datetime_utc))
    response = {'total_number_of_registered_cascade_files': file_counter, 
                'data_size_bytes_counter': round(data_size_bytes_counter,3), 
                'average_file_size_in_bytes': average_file_size_in_bytes, 
                'as_of_datetime_utc_string': current_datetime_utc_string, 
                'as_of_timestamp': timestamp,
                'percentage_publicly_accessible_files': percentage_publicly_accessible_files,
                'percentage_publicly_accessible_bytes': percentage_publicly_accessible_bytes,
                'file_type_statistics': file_type_counter}
    return response


#Misc helper functions:
class MyTimer():
    def __init__(self):
        self.start = time.time()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        end = time.time()
        runtime = end - self.start
        msg = '({time} seconds to complete)'
        log.info(msg.format(time=round(runtime, 2)))


def compute_elapsed_time_in_minutes_between_two_datetimes_func(start_datetime, end_datetime):
    time_delta = (end_datetime - start_datetime)
    total_seconds_elapsed = time_delta.total_seconds()
    total_minutes_elapsed = total_seconds_elapsed / 60
    return total_minutes_elapsed


def compute_elapsed_time_in_minutes_since_start_datetime_func(start_datetime):
    end_datetime = datetime.utcnow()
    total_minutes_elapsed = compute_elapsed_time_in_minutes_between_two_datetimes_func(start_datetime, end_datetime)
    return total_minutes_elapsed


def get_sha256_hash_of_input_data_func(input_data_or_string):
    if isinstance(input_data_or_string, str):
        input_data_or_string = input_data_or_string.encode('utf-8')
    sha256_hash_of_input_data = hashlib.sha3_256(input_data_or_string).hexdigest()
    return sha256_hash_of_input_data


def check_if_ip_address_is_valid_func(ip_address_string):
    try:
        _ = ipaddress.ip_address(ip_address_string)
        ip_address_is_valid = 1
    except Exception as e:
        log.error('Validation Error: ' + str(e))
        ip_address_is_valid = 0
    return ip_address_is_valid


def get_external_ip_func():
    output = os.popen('curl ifconfig.me')
    ip_address = output.read()
    return ip_address


async def check_if_pasteld_is_running_correctly_and_relaunch_if_required_func():
    pasteld_running_correctly = 0
    try:
        current_pastel_block_number = await get_current_pastel_block_height_func()
    except Exception as e:
        log.error(f"Problem running pastel-cli command: {e}")
        current_pastel_block_number = ''
    if isinstance(current_pastel_block_number, int):
        if current_pastel_block_number > 100000:
            pasteld_running_correctly = 1
            log.info('Pasteld is running correctly!')
    if pasteld_running_correctly == 0:
        log.info('Pasteld was not running correctly, trying to restart it...')
        process_output = os.system("cd /home/pastelup/ && tmux new -d ./pastelup start walletnode --development-mode")
        log.info('Pasteld restart command output: ' + str(process_output))
    return pasteld_running_correctly


def install_pasteld_func(network_name='testnet'):
    install_pastelup_script_command_string = "mkdir ~/pastelup && cd ~/pastelup && wget https://github.com/pastelnetwork/pastelup/releases/download/v1.1.3/pastelup-linux-amd64 && mv pastelup-linux-amd64 pastelup && chmod 755 pastelup"
    command_string = f"cd ~/pastelup && ./pastelup install walletnode -n={network_name} --force -r=latest -p=18.118.218.206,18.116.26.219 && \
                        sed -i -e '/hostname/s/localhost/0.0.0.0/' ~/.pastel/walletnode.yml && \
                        sed -i -e '$arpcbind=0.0.0.0' ~/.pastel/pastel.conf && \
                        sed -i -e '$arpcallowip=172.0.0.0/8' ~/.pastel/pastel.conf && \
                        sed -i -e 's/rpcuser=.*/rpcuser=rpc_user/' ~/.pastel/pastel.conf && \
                        sed -i -e 's/rpcpassword=.*/rpcpassword=rpc_pwd/' ~/.pastel/pastel.conf"
    if os.path.exists('~/pastelup/pastelup'):
        log.info('Pastelup is already installed!')
        log.info('Running pastelup install command...')
        try:
            command_result = os.system(command_string)
            if not command_result:
                log.info('Pastelup install command appears to have run successfully!')
        except Exception as e:
            log.error(f"Error running pastelup install command! Message: {e}; Command result: {command_result}")
    else:
        log.info('Pastelup is not installed, trying to install it...')
        try:
            install_result = os.system(install_pastelup_script_command_string)
            if not install_result:
                log.info('Pastelup installed successfully!')
                log.info('Running pastelup install command...')
                command_result = os.system(command_string)
            else:
                log.info(f"Pastelup installation failed! Message: {install_result}") 
        except Exception as e:
            log.error(f"Error running pastelup install command! Message: {e}; Command result: {install_result}")
    return
            

def safe_highlight_func(text, pattern, replacement):
    try:
        return re.sub(pattern, replacement, text)
    except Exception as e:
        logging.warning(f"Failed to apply highlight rule: {e}")
        return text


def highlight_rules_func(text):
    rules = [
        (re.compile(r"\b(success\w*)\b", re.IGNORECASE), '#COLOR1_OPEN#', '#COLOR1_CLOSE#'),
        (re.compile(r"\b(error|fail\w*)\b", re.IGNORECASE), '#COLOR2_OPEN#', '#COLOR2_CLOSE#'),
        (re.compile(r"\b(pending)\b", re.IGNORECASE), '#COLOR3_OPEN#', '#COLOR3_CLOSE#'),
        (re.compile(r"\b(response)\b", re.IGNORECASE), '#COLOR4_OPEN#', '#COLOR4_CLOSE#'),
        (re.compile(r'\"(.*?)\"', re.IGNORECASE), '#COLOR5_OPEN#', '#COLOR5_CLOSE#'),
        (re.compile(r"\'(.*?)\'", re.IGNORECASE), "#COLOR6_OPEN#", '#COLOR6_CLOSE#'),
        (re.compile(r"\`(.*?)\`", re.IGNORECASE), '#COLOR7_OPEN#', '#COLOR7_CLOSE#'),
        (re.compile(r"\b(https?://\S+)\b", re.IGNORECASE), '#COLOR8_OPEN#', '#COLOR8_CLOSE#'),
        (re.compile(r"\b(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2},\d{3})\b", re.IGNORECASE), '#COLOR9_OPEN#', '#COLOR9_CLOSE#'),
        (re.compile(r"\b(_{100,})\b", re.IGNORECASE), '#COLOR10_OPEN#', '#COLOR10_CLOSE#'),
        (re.compile(r"\b(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+)\b", re.IGNORECASE), '#COLOR11_OPEN#', '#COLOR11_CLOSE#'),
        (re.compile(r"\b([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})\b", re.IGNORECASE), '#COLOR12_OPEN#', '#COLOR12_CLOSE#'),
        (re.compile(r"\b([a-f0-9]{64})\b", re.IGNORECASE), '#COLOR13_OPEN#', '#COLOR13_CLOSE#')                                
    ]
    for pattern, replacement_open, replacement_close in rules:
        text = pattern.sub(f"{replacement_open}\\1{replacement_close}", text)
    text = html.escape(text)
    text = text.replace('#COLOR1_OPEN#', '<span style="color: #baffc9;">').replace('#COLOR1_CLOSE#', '</span>')
    text = text.replace('#COLOR2_OPEN#', '<span style="color: #ffb3ba;">').replace('#COLOR2_CLOSE#', '</span>')
    text = text.replace('#COLOR3_OPEN#', '<span style="color: #ffdfba;">').replace('#COLOR3_CLOSE#', '</span>')
    text = text.replace('#COLOR4_OPEN#', '<span style="color: #ffffba;">').replace('#COLOR4_CLOSE#', '</span>')
    text = text.replace('#COLOR5_OPEN#', '<span style="color: #bdc7e7;">').replace('#COLOR5_CLOSE#', '</span>')
    text = text.replace('#COLOR6_OPEN#', "<span style='color: #d5db9c;'>").replace('#COLOR6_CLOSE#', '</span>')
    text = text.replace('#COLOR7_OPEN#', '<span style="color: #a8d8ea;">').replace('#COLOR7_CLOSE#', '</span>')
    text = text.replace('#COLOR8_OPEN#', '<span style="color: #e2a8a8;">').replace('#COLOR8_CLOSE#', '</span>')
    text = text.replace('#COLOR9_OPEN#', '<span style="color: #ece2d0;">').replace('#COLOR9_CLOSE#', '</span>')
    text = text.replace('#COLOR10_OPEN#', '<span style="color: #d6e0f0;">').replace('#COLOR10_CLOSE#', '</span>')
    text = text.replace('#COLOR11_OPEN#', '<span style="color: #f2d2e2;">').replace('#COLOR11_CLOSE#', '</span>')
    text = text.replace('#COLOR12_OPEN#', '<span style="color: #d5f2ea;">').replace('#COLOR12_CLOSE#', '</span>')
    text = text.replace('#COLOR13_OPEN#', '<span style="color: #f2ebd3;">').replace('#COLOR13_CLOSE#', '</span>')
    return text



#_______________________________________________________________


rpc_host, rpc_port, rpc_user, rpc_password, other_flags = get_local_rpc_settings_func()
rpc_connection = AsyncAuthServiceProxy(f"http://{rpc_user}:{rpc_password}@{rpc_host}:{rpc_port}")


