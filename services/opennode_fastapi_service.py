import base64
import sqlite3
import io
from datetime import datetime, timedelta
from typing import List, Optional, Tuple
import cachetools
from cachetools import LRUCache, cached
from cachetools.keys import hashkey
import aiofiles
from aiofiles.os import stat as aio_stat

import sqlalchemy as sa
from sqlalchemy import func, update, delete, desc
from sqlalchemy.future import select
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine, AsyncSession
from fastapi import BackgroundTasks, Depends, Body

from data import db_session
from data.db_session import add_record_to_write_queue
from data.opennode_fastapi import OpenNodeFastAPIRequests, ParsedDDServiceData, RawDDServiceData, PastelBlockData, PastelAddressData, PastelTransactionData, PastelTransactionInputData, PastelTransactionOutputData, CascadeCacheFileLocks, BadTXID
import os
import sys
import time
import json
import traceback
import warnings
import ipaddress
import re
import random
import itertools
import requests
import hashlib
import magic
import subprocess
import asyncio
import pprint
from collections import Counter
import numpy as np
import pandas as pd
from pandas._libs.tslibs.timestamps import Timestamp
import httpx
import dirtyjson
from pathlib import Path

from timebudget import timebudget
from tqdm import tqdm
import zstandard as zstd
import concurrent.futures
import platform
import psutil
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import http.client as httplib
import base64
import decimal
import urllib.parse as urlparse
import statistics
from fastapi import BackgroundTasks, Depends

try:
    import urllib.parse as urlparse
except ImportError:
    import urlparse
import aiohttp
from httpx import AsyncClient
import logging
from sqlalchemy import Index
from sqlalchemy.orm import aliased
from sqlalchemy import select
from tabulate import tabulate


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
HTTP_TIMEOUT = 90

#Setup logging:
log = logging.getLogger("PastelOpenNodeFastAPI")
log.setLevel(logging.INFO)
fh = logging.FileHandler('opennode_fastapi_log.txt')
fh.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
log.addHandler(fh)
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


async def load_cache():  # Populate cache from the existing files in the cache directory
    for filename in os.listdir(cache_dir):
        file_path = os.path.join(cache_dir, filename)
        if os.path.isfile(file_path):
            cache[filename] = file_path

    
# Set up real-time notification system
class NotificationSystem:
    @staticmethod
    def notify(event_type: str, data: dict):
        # Sample implementation for sending notifications via email
        subject = f"Notification: {event_type}"
        body = str(data)
        recipients = ["jeff@pastel.network"]  # List of subscribers
        # send_email(subject, body, recipients)  # Implement send_email function

# Additional index to improve address balance lookup
Index('idx_pastel_address_data_address', PastelAddressData.pastel_address)

# Optimized block scanning configuration
BLOCK_SCAN_BATCH_SIZE = 100

   
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
    

class JSONRPCException(Exception):
    def __init__(self, rpc_error):
        parent_args = []
        try:
            parent_args.append(rpc_error['message'])
        except:
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
    def __init__(self, service_url, service_name=None, reconnect_timeout=15, reconnect_amount=2):
        self.service_url = service_url
        self.service_name = service_name
        self.url = urlparse.urlparse(service_url)
        self.client = AsyncClient()
        self.id_count = 0
        user = self.url.username
        password = self.url.password
        authpair = f"{user}:{password}".encode('utf-8')
        self.auth_header = b'Basic ' + base64.b64encode(authpair)
        self.reconnect_timeout = reconnect_timeout
        self.reconnect_amount = reconnect_amount

    def __getattr__(self, name):
        if name.startswith('__') and name.endswith('__'):
            raise AttributeError
        if self.service_name is not None:
            name = f"{self.service_name}.{name}"
        return AsyncAuthServiceProxy(self.service_url, name)

    async def __call__(self, *args):
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
                response = await self.client.post(
                    self.service_url, headers=headers, data=postdata)
                break
            except Exception as e:
                err_msg = f"Failed to connect to {self.url.hostname}:{self.url.port}"
                rtm = self.reconnect_timeout
                if rtm:
                    err_msg += f". Waiting {rtm} seconds."
                log.exception(err_msg)
                if rtm:
                    await asyncio.sleep(rtm)
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
       
        
def get_current_pastel_block_height_func():
    global rpc_connection
    best_block_hash = rpc_connection.getbestblockhash()
    best_block_details = rpc_connection.getblock(best_block_hash)
    curent_block_height = best_block_details['height']
    return curent_block_height


def get_previous_block_hash_and_merkle_root_func():
    global rpc_connection
    previous_block_height = get_current_pastel_block_height_func()
    previous_block_hash = rpc_connection.getblockhash(previous_block_height)
    previous_block_details = rpc_connection.getblock(previous_block_hash)
    previous_block_merkle_root = previous_block_details['merkleroot']
    return previous_block_hash, previous_block_merkle_root, previous_block_height


def get_last_block_data_func():
    global rpc_connection
    current_block_height = get_current_pastel_block_height_func()
    block_data = rpc_connection.getblock(str(current_block_height))
    return block_data
    

def check_psl_address_balance_func(address_to_check):
    global rpc_connection
    balance_at_address = rpc_connection.z_getbalance(address_to_check) 
    return balance_at_address

  
def get_raw_transaction_func(txid):
    global rpc_connection
    raw_transaction_data = rpc_connection.getrawtransaction(txid, 1) 
    return raw_transaction_data


async def verify_message_with_pastelid_func(pastelid, message_to_verify, pastelid_signature_on_message) -> str:
    global rpc_connection
    verification_result = await rpc_connection.pastelid('verify', message_to_verify, pastelid_signature_on_message, pastelid, 'ed448')
    return verification_result['verification']


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
    
    
def get_local_machine_supernode_data_func():
    local_machine_ip = get_external_ip_func()
    supernode_list_full_df = check_supernode_list_func()
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


def get_sn_data_from_pastelid_func(specified_pastelid):
    supernode_list_full_df = check_supernode_list_func()
    specified_machine_supernode_data = supernode_list_full_df[supernode_list_full_df['extKey'] == specified_pastelid]
    if len(specified_machine_supernode_data) == 0:
        log.error('Specified machine is not a supernode!')
        return pd.DataFrame()
    else:
        return specified_machine_supernode_data

    
def get_sn_data_from_sn_pubkey_func(specified_sn_pubkey):
    supernode_list_full_df = check_supernode_list_func()
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


async def get_raw_dd_service_results_from_local_db_func(txid: str) -> Optional[RawDDServiceData]:
    try:
        async with db_session.create_async_session() as session: 
            query = select(RawDDServiceData).filter(RawDDServiceData.registration_ticket_txid == txid)
            result = await session.execute(query)
        return result.scalar_one_or_none()
    except Exception as e:
        log.error(f"Failed to get raw DD service results from local DB for txid {txid} with error: {e}")
        raise e


async def get_raw_dd_service_results_from_sense_api_func(txid: str, ticket_type: str, corresponding_pastel_blockchain_ticket_data: dict) -> RawDDServiceData:
    try:
        requester_pastelid = 'jXYwVLikSSJfoX7s4VpX3osfMWnBk3Eahtv5p1bYQchaMiMVzAmPU57HMA7fz59ffxjd2Y57b9f7oGqfN5bYou'
        if ticket_type == 'sense':
            request_url = f'http://localhost:8080/openapi/sense/download?pid={requester_pastelid}&txid={txid}'
        elif ticket_type == 'nft':
            request_url = f'http://localhost:8080/nfts/get_dd_result_file?pid={requester_pastelid}&txid={txid}'
        else:
            raise ValueError(f'Invalid ticket type for txid {txid}! Ticket type must be either "sense" or "nft"!')
        headers = {'Authorization': 'testpw123'}
        async with httpx.AsyncClient() as client:
            log.info(f'Now attempting to download Raw DD-Service results for ticket type {ticket_type} and txid: {txid}...') 
            response = await client.get(request_url, headers=headers, timeout=500.0)    
            log.info(f'Finished downloading Raw DD-Service results for ticket type {ticket_type} and txid: {txid}; Took {round(response.elapsed.total_seconds(),2)} seconds')
        parsed_response = response.json()
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
        try:
            await add_record_to_write_queue(raw_dd_service_data)
        except Exception as e:
            log.error(f"Failed to save raw DD service results to local DB for txid {txid} with error: {e}")
        return raw_dd_service_data
    except Exception as e:
        log.error(f"Failed to get raw DD service results from Sense API for txid {txid} with error: {e}")
        raise e


async def get_raw_dd_service_results_by_registration_ticket_txid_func(txid: str) -> RawDDServiceData:
    try:
        #To clear out the raw_dd_service_data table of any nft type tickets, run:
        # sqlite3 /home/ubuntu/opennode_fastapi/db/opennode_fastapi.sqlite "DELETE FROM raw_dd_service_data WHERE ticket_type='nft';"        
        raw_dd_service_data = await get_raw_dd_service_results_from_local_db_func(txid)
        if raw_dd_service_data is not None:  #First check if we already have the results in our local sqlite database:
            is_cached_response = True
            return raw_dd_service_data, is_cached_response #If we don't have the results in our local sqlite database, then we need to download them from the Sense A
        corresponding_pastel_blockchain_ticket_data = await get_pastel_blockchain_ticket_func(txid)
        if 'ticket' in corresponding_pastel_blockchain_ticket_data.keys():            
            if 'nft_ticket' in corresponding_pastel_blockchain_ticket_data['ticket'].keys():
                ticket_type = 'nft'
            elif 'action_ticket' in corresponding_pastel_blockchain_ticket_data['ticket'].keys():
                ticket_type = 'sense'
            else:
                ticket_type = 'unknown'
        else:
            ticket_type = 'unknown'
        raw_dd_service_data = await get_raw_dd_service_results_from_sense_api_func(txid, ticket_type, corresponding_pastel_blockchain_ticket_data)
        is_cached_response = False
        return raw_dd_service_data, is_cached_response
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
        loaded_json = dirtyjson.loads(json_string.replace('\\"', '"').replace('\/', '/').replace('\\n', ' '))
        return loaded_json


async def parse_raw_dd_service_data_func(raw_dd_service_data: RawDDServiceData) -> ParsedDDServiceData:
    parsed_dd_service_data = ParsedDDServiceData()
    try:
        final_response = safe_json_loads_func(raw_dd_service_data.raw_dd_service_data_json)
    except Exception as e:
        log.error(f'Error loading raw_dd_service_data_json: {e}')
        return parsed_dd_service_data
    final_response_df = pd.DataFrame.from_records([final_response])
    try:
        rareness_scores_table_json = decompress_and_decode_zstd_compressed_and_base64_encoded_string_func(final_response.get('rareness_scores_table_json_compressed_b64', ''))
        rareness_scores_table_dict = safe_json_loads_func(rareness_scores_table_json)
    except Exception as e:
        log.error(f'Error processing rareness_scores_table_json: {e}')
        return parsed_dd_service_data
    top_10_most_similar_registered_images_on_pastel_file_hashes = list(rareness_scores_table_dict.get('image_hash', {}).values())
    is_likely_dupe_list = list(rareness_scores_table_dict.get('is_likely_dupe', {}).values())
    detected_dupes_from_registered_images_on_pastel_file_hashes = [x for idx, x in enumerate(top_10_most_similar_registered_images_on_pastel_file_hashes) if is_likely_dupe_list[idx]]
    detected_dupes_from_registered_images_on_pastel_thumbnail_strings = [x[0][0] for idx, x in enumerate(list(rareness_scores_table_dict.get('thumbnail', {}).values())) if is_likely_dupe_list[idx]]
    try:
        internet_rareness_json = final_response.get('internet_rareness', {})
        internet_rareness_summary_table_json = decompress_and_decode_zstd_compressed_and_base64_encoded_string_func(internet_rareness_json.get('rare_on_internet_summary_table_as_json_compressed_b64', ''))
        internet_rareness_summary_table_dict = safe_json_loads_func(internet_rareness_summary_table_json)
    except Exception as e:
        log.error(f'Error processing internet_rareness_summary_table_json: {e}')
        return parsed_dd_service_data
    internet_rareness_summary_table_df = pd.DataFrame.from_records(internet_rareness_summary_table_dict)
    try:
        alternative_rare_on_internet_dict_as_json = decompress_and_decode_zstd_compressed_and_base64_encoded_string_func(internet_rareness_json.get('alternative_rare_on_internet_dict_as_json_compressed_b64', ''))
        alternative_rare_on_internet_dict = safe_json_loads_func(alternative_rare_on_internet_dict_as_json)
    except Exception as e:
        log.error(f'Error processing alternative_rare_on_internet_dict_as_json: {e}')
        return parsed_dd_service_data
    alternative_rare_on_internet_dict_summary_table_df = pd.DataFrame.from_records(alternative_rare_on_internet_dict)
    try:
        parsed_dd_service_data.ticket_type = raw_dd_service_data.ticket_type
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
        return parsed_dd_service_data
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
        return parsed_dd_service_data
    try:
        parsed_dd_service_data.internet_rareness__b64_image_strings_of_in_page_matches = str(internet_rareness_summary_table_df.get('img_src_string', []).values.tolist())
        parsed_dd_service_data.internet_rareness__original_urls_of_in_page_matches = str(internet_rareness_summary_table_df.get('original_url', []).values.tolist())
        parsed_dd_service_data.internet_rareness__result_titles_of_in_page_matches = str(internet_rareness_summary_table_df.get('title', []).values.tolist())
        parsed_dd_service_data.internet_rareness__date_strings_of_in_page_matches = str(internet_rareness_summary_table_df.get('date_string', []).values.tolist())
        parsed_dd_service_data.internet_rareness__misc_related_images_as_b64_strings =  str(internet_rareness_summary_table_df.get('misc_related_image_as_b64_string', []).values.tolist())
        parsed_dd_service_data.internet_rareness__misc_related_images_url = str(internet_rareness_summary_table_df.get('misc_related_image_url', []).values.tolist())
        parsed_dd_service_data.alternative_rare_on_internet__number_of_similar_results = str(len(alternative_rare_on_internet_dict_summary_table_df))
        parsed_dd_service_data.alternative_rare_on_internet__b64_image_strings = str(alternative_rare_on_internet_dict_summary_table_df.get('list_of_images_as_base64', []).values.tolist())
        parsed_dd_service_data.alternative_rare_on_internet__original_urls = str(alternative_rare_on_internet_dict_summary_table_df.get('list_of_href_strings', []).values.tolist())
        parsed_dd_service_data.alternative_rare_on_internet__google_cache_urls = str(alternative_rare_on_internet_dict_summary_table_df.get('list_of_image_src_strings', []).values.tolist())
        parsed_dd_service_data.alternative_rare_on_internet__alt_strings = str(alternative_rare_on_internet_dict_summary_table_df.get('list_of_image_alt_strings', []).values.tolist())
    except Exception as e:
        log.error(f'Error filling in the final part of parsed_dd_service_data fields: {e}')
        return parsed_dd_service_data
    return parsed_dd_service_data


async def check_for_parsed_dd_service_result_in_db_func(txid: str) -> Tuple[Optional[ParsedDDServiceData], bool]:
    try:
        async with db_session.create_async_session() as session:
            query = select(ParsedDDServiceData).filter(ParsedDDServiceData.registration_ticket_txid == txid)
            result = await session.execute(query)
        results_already_in_local_db = result.scalar_one_or_none()
        is_cached_response = results_already_in_local_db is not None
        return results_already_in_local_db, is_cached_response
    except Exception as e:
        log.error(f'Error checking for parsed_dd_service_result in local sqlite database: {e}')
        

async def save_parsed_dd_service_data_to_db_func(parsed_dd_service_data):
    try:
        await add_record_to_write_queue(parsed_dd_service_data)
    except Exception as e:
        log.error(f'Error saving parsed_dd_service_data to local sqlite database: {e}')


async def get_parsed_dd_service_results_by_registration_ticket_txid_func(txid: str) -> ParsedDDServiceData:
    try:    
        start_time = time.time()
        # First check if we already have the results in our local sqlite database; if so, then return them:
        parsed_dd_service_data, is_cached_response = await check_for_parsed_dd_service_result_in_db_func(txid)
        if is_cached_response:
            return parsed_dd_service_data, is_cached_response
        raw_dd_service_data, _ = await get_raw_dd_service_results_by_registration_ticket_txid_func(txid) # If we don't have the results in our local sqlite database, then we need to download the raw DD-Service data, parse it:
        parsed_dd_service_data = await parse_raw_dd_service_data_func(raw_dd_service_data)
        _, is_cached_response = await check_for_parsed_dd_service_result_in_db_func(txid) # After parsing the data, check again if it does not exist in the database before saving:
        if not is_cached_response:
            await save_parsed_dd_service_data_to_db_func(parsed_dd_service_data)
            log.info(f'Finished generating Parsed DD-Service data for ticket type {str(parsed_dd_service_data.ticket_type)} and txid {txid} and saving it to the local sqlite database! Took {round(time.time() - start_time, 2)} seconds in total.')
        return parsed_dd_service_data, False
    except Exception as e:
        log.error(f'Error while executing `get_parsed_dd_service_results_by_registration_ticket_txid_func`: {e}')
        
        
async def add_bad_txid_to_db_func(txid, type, reason):
    try:
        async with db_session.create_async_session() as session:
            query = select(BadTXID).where(BadTXID.txid == txid)
            result = await session.execute(query)
            bad_txid_exists = result.scalars().first()
            if bad_txid_exists is None:
                new_bad_txid = BadTXID(txid=txid, ticket_type=type, reason_txid_is_bad=reason, failed_attempts=1,
                                       next_attempt_time=datetime.utcnow() + timedelta(days=1))
                await add_record_to_write_queue(new_bad_txid)
            else:
                bad_txid_exists.failed_attempts += 1  # If this txid is already marked as bad, increment the attempt count and set the next attempt time
                if bad_txid_exists.failed_attempts == 2:
                    bad_txid_exists.next_attempt_time = datetime.utcnow() + timedelta(days=3)
                else:
                    bad_txid_exists.next_attempt_time = datetime.utcnow() + timedelta(days=1)
            await session.commit()
            if bad_txid_exists is None:
                log.info(f"No BadTXID object found with txid: {txid}, creating a new one...")
            else:
                log.info(f"Updated existing BadTXID object with txid: {txid}, incrementing failed attempts count.")
    except Exception as e:
        log.error(f"Error while adding bad txid to DB. Error: {e}")
        raise


async def get_bad_txids_from_db_func(type):
    try:
        async with db_session.create_async_session() as session:
            result = await session.execute(
                select(BadTXID).where(BadTXID.ticket_type == type, BadTXID.failed_attempts >= 3,
                                      BadTXID.next_attempt_time < datetime.utcnow()))
            bad_txids = result.scalars().all()
        return [bad_tx.txid for bad_tx in bad_txids]
    except Exception as e:
        log.error(f"Error while getting bad txids from DB. Error: {e}")
        raise


async def check_and_update_cascade_ticket_bad_txid_func(txid: str, session):
    try:
        query = select(BadTXID).where(BadTXID.txid == txid)
        result = await session.execute(query)
        bad_txid_exists = result.scalars().first()
        if bad_txid_exists:
            log.info("Bad TXID exists.")
            if datetime.utcnow() < bad_txid_exists.next_attempt_time:
                return f"Error: txid {txid} is marked as bad and has not reached its next attempt time.", ""
            else:
                log.info("Incrementing failed attempts.")
                bad_txid_exists.failed_attempts += 1
                if bad_txid_exists.failed_attempts == 3:
                    bad_txid_exists.next_attempt_time = datetime.utcnow() + timedelta(days=3)
                else:
                    bad_txid_exists.next_attempt_time = datetime.utcnow() + timedelta(days=1)
                await session.commit()
        return None
    except Exception as e:
        log.error(f"Error in check_and_update_cascade_ticket_bad_txid_func. Error: {e}")
        raise


async def startup_cascade_file_download_lock_cleanup_func(background_tasks: BackgroundTasks):
    async with db_session.create_async_session() as session:
        lock_expiration_time = timedelta(minutes=10)
        current_time = datetime.utcnow()
        stale_locks_query = select(CascadeCacheFileLocks).where(CascadeCacheFileLocks.lock_created_at < (current_time - lock_expiration_time))
        result = await session.execute(stale_locks_query)
        stale_locks = result.scalars().all()
        log.info(f"Found {len(stale_locks)} stale cascade file download locks!")
        for lock in stale_locks:
            session.delete(lock)
        await session.commit()
        log.info(f"Deleted {len(stale_locks)} stale cascade file download locks")
            
    
async def download_and_cache_cascade_file_func(txid: str, request_url: str, headers, session, cache_dir: str):
    lock_expiration_time = timedelta(minutes=10)  # Define cascade file download lock expiration time to be 10 minutes (this is so that we don't try to download the same file from multiple workers at the same time)
    lock_exists = await session.get(CascadeCacheFileLocks, txid)
    current_time = datetime.utcnow()
    if lock_exists:
        lock_age = current_time - lock_exists.lock_created_at
        if lock_age < lock_expiration_time:
            log.warning(f"Download of file {txid} is already in progress, skipping...")
            return None, None  # return tuple of None
        else: # Lock is considered stale, delete it
            await session.delete(lock_exists)
            await session.commit()
            log.warning(f"Stale lock for file {txid} has been deleted...")
    new_lock = CascadeCacheFileLocks(txid=txid, lock_created_at=current_time)
    await add_record_to_write_queue(new_lock)
    cache_file_path = None
    decoded_response = None
    try:
        async with httpx.AsyncClient() as client:
            async with client.stream('GET', request_url, headers=headers, timeout=500.0) as response:
                body = await response.aread()  # async read
                parsed_response = json.loads(body.decode())  # parse JSON
        file_identifier = parsed_response['file_id']
        file_download_url = f"http://localhost:8080/files/{file_identifier}"
        async with httpx.AsyncClient() as client:
            async with client.stream('GET', file_download_url, headers=headers, timeout=500.0) as response:
                decoded_response = await response.aread()  # async read
        cache_file_path = os.path.join(cache_dir, txid)
        async with aiofiles.open(cache_file_path, mode='wb') as f:
            await f.write(decoded_response)
    except Exception as e:
        log.error(f'An error occurred while downloading the file from Cascade API for txid {txid}! Error message: {e} Deleting the lock and returning...')
        lock_exists = await session.get(CascadeCacheFileLocks, txid)
        if lock_exists:
            await session.delete(lock_exists)
            await session.commit()
        await add_bad_txid_to_db_func(txid, 'cascade', str(e))
        return f"Error: An exception occurred while downloading the file. The txid {txid} has been marked as bad.", ""
    lock_exists = await session.get(CascadeCacheFileLocks, txid)
    if lock_exists:
        await session.delete(lock_exists)
        await session.commit()
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
        if mime_type != expected_mime_type:
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
        requester_pastelid = 'jXYwVLikSSJfoX7s4VpX3osfMWnBk3Eahtv5p1bYQchaMiMVzAmPU57HMA7fz59ffxjd2Y57b9f7oGqfN5bYou'
        request_url = f'http://localhost:8080/openapi/cascade/download?pid={requester_pastelid}&txid={txid}'
        headers = {'Authorization': 'testpw123'}
        async with db_session.create_async_session() as session:
            check_bad_txid_result = await check_and_update_cascade_ticket_bad_txid_func(txid, session)
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
                if txid in cache and os.path.exists(cache[txid]):  # Check if the file is already in cache and exists in the cache_dir
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
            verification_result = await verify_downloaded_cascade_file_func(decoded_response, original_file_sha3_256_hash, original_file_size_in_bytes, original_file_mime_type)
            if verification_result is not True:
                log.error(f'File verification failed for txid {txid}. Error: {verification_result}')
                return verification_result, ""
        log.info(f'Finished download process for txid {txid} in {time.time() - start_time} seconds.')
        return decoded_response, original_file_name_string
    except Exception as e:
        log.error(f'An unexpected error occurred in download_publicly_accessible_cascade_file_by_registration_ticket_txid_func for txid {txid}. Error: {e}')
        raise


async def populate_database_with_all_dd_service_data_func():
    tickets_obj = await get_all_pastel_blockchain_tickets_func()
    sense_ticket_dict = json.loads(tickets_obj['action'])
    sense_ticket_df = pd.DataFrame(sense_ticket_dict).T
    sense_ticket_df_filtered = sense_ticket_df[sense_ticket_df['action_type'] == 'sense'].drop_duplicates(subset=['txid'])
    nft_ticket_dict = json.loads(tickets_obj['nft'])
    nft_ticket_df = pd.DataFrame(nft_ticket_dict).T
    nft_ticket_df_filtered = nft_ticket_df.drop_duplicates(subset=['txid'])
    list_of_sense_registration_ticket_txids = sense_ticket_df_filtered['txid'].values.tolist()
    list_of_nft_registration_ticket_txids = nft_ticket_df_filtered['txid'].values.tolist()
    list_of_combined_registration_ticket_txids = list_of_sense_registration_ticket_txids + list_of_nft_registration_ticket_txids
    random.shuffle(list_of_combined_registration_ticket_txids)
    random_delay_in_seconds = random.randint(1, 15)
    await asyncio.sleep(random_delay_in_seconds)
    for current_txid in list_of_combined_registration_ticket_txids:
        try: # Before fetching, check if the current txid is already marked as bad
            is_nft_ticket = current_txid in list_of_nft_registration_ticket_txids
            ticket_type = 'nft' if is_nft_ticket else 'sense'
            bad_txids = await get_bad_txids_from_db_func(ticket_type)
            if current_txid in bad_txids: # If it is bad and has not reached its next attempt time, skip it
                continue
            corresponding_pastel_blockchain_ticket_data = await get_pastel_blockchain_ticket_func(current_txid)
            minimum_testnet_height_for_nft_tickets = 290000
            if is_nft_ticket and corresponding_pastel_blockchain_ticket_data['height'] <= minimum_testnet_height_for_nft_tickets:
                continue
            current_dd_service_data, is_cached_response = await get_parsed_dd_service_results_by_registration_ticket_txid_func(current_txid)
        except Exception as e:
            await add_bad_txid_to_db_func(current_txid, ticket_type, str(e))
            pass


def print_dict_structure(d, indent=0):
    for key, value in d.items():
        print('\t' * indent + str(key))
        if isinstance(value, dict):
            print_dict_structure(value, indent+1)
        elif isinstance(value, list):
            if value:  # check if list is not empty
                if isinstance(value[0], dict):
                    print('\t' * (indent+1) + str(type(value[0])))
                    print_dict_structure(value[0], indent+2)
                else:
                    print('\t' * (indent+1) + str(type(value[0])))
            else:
                print('\t' * (indent+1) + 'list is empty')
        else:
            print('\t' * (indent+1) + str(type(value)))


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
    try:
        log.info(f'Attempting to get {n} random cascade TXIDs...')
        tickets_obj = await get_all_pastel_blockchain_tickets_func()
        cascade_ticket_dict = json.loads(tickets_obj['action'])
        cascade_ticket_dict_df = pd.DataFrame(cascade_ticket_dict).T
        cascade_ticket_dict_df_filtered = cascade_ticket_dict_df[cascade_ticket_dict_df['action_type'] == 'cascade'].drop_duplicates(subset=['txid'])
        txids = cascade_ticket_dict_df_filtered['txid'].tolist()
        if n < 10:
            txids_sample = random.sample(txids, min(6*n, len(txids))) # Sample 6n TXIDs to increase the chance of getting enough public ones.
        else:
            txids_sample = random.sample(txids, min(3*n, len(txids))) # Sample 3n TXIDs to increase the chance of getting enough public ones.
        accessible_txids = []
        for txid in txids_sample:
            try:
                ticket_response = await get_pastel_blockchain_ticket_func(txid)
                action_ticket = json.loads(base64.b64decode(ticket_response['ticket']['action_ticket']))
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
    except Exception as e:
        log.error(f'An error occurred while trying to get random cascade transaction IDs! Error message: {e}')
        return []
    
    
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
        log_data = output.decode('utf-8')
        return log_data
    except Exception as e:
        log.error(f'An error occurred while trying to get data from the wallet node log file! Error message: {e}')
        return None
    
    
def parse_walletnode_log_data_func(log_data):
    if log_data is None:
        log.error('Cannot parse wallet node log data, as it is None!')
        return []
    log_pattern = r'\[(.*?)\]\s+INFO\s+(.*?):\s+(.*?)\sserver_ip=(.*?)\s(.*?)$'
    txid_pattern = r'txid=(\w+)'
    downloaded_pattern = r'Downloaded from supernode address=(.*?)\s'
    parsed_data = []
    log_indices = {}
    downloaded_parts = {}
    supernode_ips_dict = {}  # Changed from a set to a dictionary
    current_index = {}  # Stores the current index for each txid
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
                        if txid not in supernode_ips_dict:  # Add this check
                            supernode_ips_dict[txid] = []
                        supernode_ips_dict[txid].append(supernode_ip)
                parsed_data.append(data)
    parsed_data_df = pd.DataFrame(parsed_data)
    return parsed_data_df, supernode_ips_dict  # Return the dictionary instead of a list


def update_cascade_status_func(df: pd.DataFrame):
    log_data = get_walletnode_log_data_func()
    parsed_log_data, supernode_ips_dict = parse_walletnode_log_data_func(log_data)
    txid_log_dict = {}
    for idx, row in parsed_log_data.iterrows():
        txid = row['txid']
        log_line = {
            'index': row['index'], 
            'datetime': row['timestamp'], 
            'message': row['message']
        }
        if txid in txid_log_dict:
            txid_log_dict[txid]['log_lines'].append(log_line)
        else:
            txid_log_dict[txid] = {
                'last_log_status': row['message'], 
                'log_lines': [log_line]
            }
        if 'txid' in df.columns and txid in df['txid'].values:
            df.loc[df['txid'] == txid, 'last_log_status'] = row['message']
            df.loc[df['txid'] == txid, 'supernode_ips'] = ", ".join(supernode_ips_dict.get(txid, []))
            if row['message'] == 'Start downloading':
                df.loc[df['txid'] == txid, 'datetime_started'] = row['timestamp'] if pd.notnull(row['timestamp']) else 'NA'
            elif row['message'] == 'Finished downloading':
                df.loc[df['txid'] == txid, 'datetime_finished'] = row['timestamp'] if pd.notnull(row['timestamp']) else 'NA'
        else:
            new_row = {
                'index': row['index'],
                'timestamp': row['timestamp'],
                'message': row['message'],
                'txid': txid,
                'supernode_ips': ", ".join(supernode_ips_dict.get(txid, []))
            }
            df = df.append(new_row, ignore_index=True)
    return df, txid_log_dict, supernode_ips_dict


async def update_cascade_status_periodically_func(df: pd.DataFrame):
    txid_log_dict = {}
    supernode_ips_dict = {}
    try:
        while True:
            df, new_txid_log_dict, new_supernode_ips_dict = update_cascade_status_func(df)
            txid_log_dict.update(new_txid_log_dict)  
            supernode_ips_dict.update(new_supernode_ips_dict)
            await asyncio.sleep(0.5)
    except asyncio.exceptions.CancelledError:
        log.info('The status update task was cancelled.')
    return df, txid_log_dict, supernode_ips_dict



async def perform_bulk_cascade_test_download_tasks_func(txids, seconds_to_wait_for_all_files_to_finish_downloading):
    try:
        df = pd.DataFrame()  # Initialize dataframe here
        txid_log_dict = {}
        supernode_ips_dict = {}
        try:
            update_task = asyncio.create_task(update_cascade_status_periodically_func(df))            
        except Exception as e:
            log.error(f'Exception occurred while starting update task: {e}')
        await asyncio.sleep(2)
        try:    
            download_tasks = [asyncio.create_task(download_cascade_file_test_func(txid)) for txid in txids]
        except Exception as e:
            log.error(f'Exception occurred while starting download tasks: {e}')
        log.info('Waiting for download tasks to finish...')
        try:
            finished, unfinished = await asyncio.wait(download_tasks, timeout=seconds_to_wait_for_all_files_to_finish_downloading)
            max_cancel_attempts = 3  # Set maximum cancel attempts
            cancel_timeout = 20  # Set cancel timeout
            status_df = pd.DataFrame()
            txid_log_dict = {}
            supernode_ips_dict = {}
            if update_task in finished:
                status_df, txid_log_dict, supernode_ips_dict = update_task.result()
            else:
                for attempt in range(max_cancel_attempts):
                    update_task.cancel()
                    try:
                        await asyncio.wait([update_task], timeout=cancel_timeout)
                    except asyncio.exceptions.TimeoutError:
                        log.warning(f'Update task did not finish within the expected time. Attempt {attempt + 1} of {max_cancel_attempts}.')
                        continue
                    if update_task.done():
                        try:
                            status_df, txid_log_dict, supernode_ips_dict = update_task.result()
                        except asyncio.exceptions.CancelledError:
                            log.warning('Update task was cancelled.')
                            status_df = pd.DataFrame()
                            txid_log_dict = {}
                            supernode_ips_dict = {}
                    break
                else:  # This block runs if we have gone through all the attempts without breaking
                    log.error('Update task did not finish even after multiple cancellation attempts. Aborting.')
                    return None, None, None
        except Exception as e:
            log.error(f'Exception occurred while waiting for download tasks to finish: {e}')
        log.info('All download tasks finished.')
        download_data = [t.result() for t in finished if t.result() is not None and 'txid' in t.result()]
        download_df = pd.DataFrame(download_data)
        if 'txid' in download_df.columns:
            download_df['log_lines'] = download_df['txid'].map(txid_log_dict)
            download_df['supernode_ips'] = download_df['txid'].apply(lambda x: ", ".join(supernode_ips_dict.get(x, [])))
            download_df['txid'] = download_df['txid'].astype(str)      
            if 'txid' in status_df.columns: 
                if download_df['txid'].isin(status_df['txid']).sum() != download_df.shape[0]:
                    df = pd.merge(download_df, status_df, how='left', on='txid')
                else:
                    df = download_df                
        else:
            df = download_df
        if 'datetime_finished' in df.columns and 'datetime_started' in df.columns:
            df['time_elapsed'] = (df['datetime_finished'] - df['datetime_started']).dt.total_seconds()
        else:
            df['time_elapsed'] = np.nan             
        return df, txid_log_dict, supernode_ips_dict
    except Exception as e:
        log.error(f'Error occurred while performing download tasks: {e}', exc_info=True)
        return None, None, None
    

def create_bulk_cascade_test_summary_stats_func(df, seconds_to_wait_for_all_files_to_finish_downloading, number_of_concurrent_downloads):
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
    try:
        datetime_test_started = datetime.utcnow()
        log.info('Starting bulk test of cascade...')
        status_df = pd.DataFrame()
        txids = await get_random_cascade_txids_func(n)
        log.info(f'Got {len(txids)} txids for testing.')
        seconds_to_wait_for_all_files_to_finish_downloading = 500
        df, txid_log_dict, supernode_ips = await perform_bulk_cascade_test_download_tasks_func(txids, seconds_to_wait_for_all_files_to_finish_downloading)        
        summary_df = create_bulk_cascade_test_summary_stats_func(df, seconds_to_wait_for_all_files_to_finish_downloading, n)
        log.info('Processing test results...')
        df_dict = df.replace([np.inf, -np.inf], np.nan).fillna('NA').to_dict('records')
        summary_dict = summary_df.replace([np.inf, -np.inf], np.nan).fillna('NA').to_dict('records')
        combined_output_dict = {
            'datetime_test_started': datetime_test_started.strftime('%Y-%m-%dT%H:%M:%S'),
            'duration_of_test_in_seconds': round((datetime.utcnow() - datetime_test_started).total_seconds(), 2),
            'cascade_bulk_download_test_results__data': df_dict,
            'cascade_bulk_download_test_results__summary': summary_dict
        }
        log.info('Finished bulk test of cascade.')
        combined_output_dict = await convert_dict_to_make_it_safe_for_json_func(combined_output_dict)
        return combined_output_dict
    except Exception as e:
        log.error(f'Error occurred during the bulk test of cascade: {e}', exc_info=True)


async def run_populate_database_with_all_dd_service_data_func(background_tasks: BackgroundTasks = Depends):
    background_tasks.add_task(await populate_database_with_all_dd_service_data_func())
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
        except:
            try:
                response_json = await rpc_connection.tickets('find', 'nft', activation_ticket_txid )
            except:
                response_json = 'Unable to find the activation ticket in the blockchain'
    else:
        response_json = f'The txid given ({activation_ticksense_collection_ticket_txidet_txid}) is not a valid activation ticket txid for a collection ticket'
    return response_json
        

async def get_current_total_number_and_size_and_average_size_of_registered_cascade_files_func():
    tickets_obj = await get_all_pastel_blockchain_tickets_func()
    cascade_ticket_dict = json.loads(tickets_obj['action'])
    cascade_ticket_df = pd.DataFrame(cascade_ticket_dict).T
    cascade_ticket_df_filtered = cascade_ticket_df[cascade_ticket_df['action_type'] == 'cascade'].drop_duplicates(subset=['txid'])
    list_of_cascade_registration_ticket_txids = cascade_ticket_df_filtered['txid'].values.tolist()
    list_of_cascade_action_tickets = cascade_ticket_df_filtered['action_ticket'].values.tolist()
    list_of_known_bad_cascade_txids_to_skip = []
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


async def parse_and_store_transaction(txid: str, block_height: int, session):
    try:
        # Retrieve raw transaction data
        raw_transaction_data = await rpc_connection.getrawtransaction(txid, 1)
        # Create a PastelTransactionData object
        transaction = PastelTransactionData(transaction_id=txid)
        # Parse inputs
        for vin in raw_transaction_data['vin']:
            # Create a PastelTransactionInputData object and add to transaction inputs
            input_data = PastelTransactionInputData(
                transaction_id=txid,
                previous_output_id=vin['vout']
            )
            transaction.inputs.append(input_data)
        # Parse outputs
        for vout in raw_transaction_data['vout']:
            # Create a PastelTransactionOutputData object and add to transaction outputs
            output_data = PastelTransactionOutputData(
                transaction_id=txid,
                amount=vout['value'],
                pastel_address=vout['scriptPubKey']['addresses'][0]
            )
            transaction.outputs.append(output_data)
            # Update address balance
            address_data = session.query(PastelAddressData).filter_by(pastel_address=output_data.pastel_address).one_or_none()
            if address_data:
                address_data.balance += output_data.amount
            else:
                address_data = PastelAddressData(pastel_address=output_data.pastel_address, balance=output_data.amount)
                session.add(address_data)
        # Store the transaction data in the database
        session.add(transaction)
        await session.flush()
        # Update the number of confirmations for the transaction
        current_block_height = await rpc_connection.getblockcount()
        transaction.confirmations = current_block_height - block_height + 1
    except Exception as e:
        log.error(f"Error while processing transaction {txid}: {e}")


async def parse_and_store_block(block_hash: str, session):
    try:
        # Retrieve block data
        block_data = await rpc_connection.getblock(block_hash)
        # Create a PastelBlockData object
        block = PastelBlockData(
            block_hash=block_hash,
            block_height=block_data['height'],
            previous_block_hash=block_data['previousblockhash'],
            timestamp=datetime.fromtimestamp(block_data['time'])
        )
        # Parse transactions in the block
        for txid in block_data['tx']:
            # Parse and store transaction data
            await parse_and_store_transaction(txid, block_data['height'], session)
        # Store the block data in the database
        session.add(block)
        await session.flush()
    except Exception as e:
        log.error(f"Error while processing block {block_hash}: {e}")


async def handle_block_reorganization(new_block_hash: str, session):
    # Get the latest block from the database
    last_block_query = select(PastelBlockData).order_by(PastelBlockData.block_height.desc())
    last_block_result = await session.execute(last_block_query)
    last_block = last_block_result.scalar_one_or_none()
    # Check for reorganization
    if last_block.block_hash != new_block_hash:
        # Handle reorganization: reverse changes and rescan affected blocks
        # Find common ancestor block
        new_block = await session.get(PastelBlockData, new_block_hash)
        old_block = last_block
        while old_block.block_hash != new_block.block_hash:
            if old_block is None or new_block is None:
                # Handle the case where there is no common ancestor block
                log.error("Could not find a common ancestor block during reorganization")
                return
            if old_block.block_height > new_block.block_height:
                old_block = await session.get(PastelBlockData, old_block.previous_block_hash)
            elif old_block.block_height < new_block.block_height:
                new_block = await session.get(PastelBlockData, new_block.previous_block_hash)
            else:
                old_block = await session.get(PastelBlockData, old_block.previous_block_hash)
                new_block = await session.get(PastelBlockData, new_block.previous_block_hash)
        # Delete orphaned blocks and associated transactions, inputs, and outputs
        OrphanedBlock = aliased(PastelBlockData)
        orphaned_blocks_query = select(OrphanedBlock).filter(OrphanedBlock.block_height > old_block.block_height)
        orphaned_blocks = await session.execute(orphaned_blocks_query)
        for orphaned_block in orphaned_blocks.scalars():
            session.delete(orphaned_block)
        # Rescan and add new blocks
        await parse_and_store_block(new_block_hash, session)
        # Commit changes to the database
        await session.commit()
        # Notify of blockchain reorganization
        NotificationSystem.notify('reorganization', {'new_block_hash': new_block_hash})


async def scan_new_blocks():
    global rpc_connection
    async with db_session.create_async_session() as session:
        try:
            # Get the current block height
            current_block_height = await rpc_connection.getblockcount()
            # Get the latest block height stored in the database
            last_block_query = select(PastelBlockData).order_by(PastelBlockData.block_height.desc())
            last_block_result = await session.execute(last_block_query)
            last_block = last_block_result.scalar_one_or_none()
            last_block_height = last_block.block_height if last_block else 0
            # Check for blockchain reorganization
            if last_block:
                await handle_block_reorganization(await rpc_connection.getblockhash(last_block_height), session)
            # Optimized block scanning
            while last_block_height < current_block_height:
                # Determine batch size for scanning
                scan_batch_size = min(BLOCK_SCAN_BATCH_SIZE, current_block_height - last_block_height)
                # Scan new blocks in batches
                for block_height in range(last_block_height + 1, last_block_height + scan_batch_size + 1):
                    # Get the block hash
                    block_hash = await rpc_connection.getblockhash(block_height)
                    # Parse and store block data
                    await parse_and_store_block(block_hash, session)
                # Commit changes to the database
                await session.commit()
                # Update last block height
                last_block_height += scan_batch_size
                # Notify of progress
                log.info(f"Successfully scanned and updated blocks {last_block_height - scan_batch_size + 1} to {last_block_height}")
        except Exception as e:
            log.error(f"Error while scanning new blocks: {e}")
            # Optionally, handle rollback in case of error
            await session.rollback()
            

async def run_scan_new_blocks_func(background_tasks: BackgroundTasks = Depends):
    background_tasks.add_task(await scan_new_blocks())
    return {"message": 'Started background task to scan all blocks...'}            
    
    
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


def check_if_pasteld_is_running_correctly_and_relaunch_if_required_func():
    pasteld_running_correctly = 0
    try:
        current_pastel_block_number = get_current_pastel_block_height_func()
    except:
        log.error('Problem running pastel-cli command!')
        current_pastel_block_number = ''
    if isinstance(current_pastel_block_number, int):
        if current_pastel_block_number > 100000:
            pasteld_running_correctly = 1
            log.info('Pasteld is running correctly!')
    if pasteld_running_correctly == 0:
        process_output = os.system("cd /home/pastelup/ && tmux new -d ./pastelup start walletnode --development-mode")
    return pasteld_running_correctly


def install_pasteld_func(network_name='testnet'):
    install_pastelup_script_command_string = f"mkdir ~/pastelup && cd ~/pastelup && wget https://github.com/pastelnetwork/pastelup/releases/download/v1.1.3/pastelup-linux-amd64 && mv pastelup-linux-amd64 pastelup && chmod 755 pastelup"
    command_string = f"cd ~/pastelup && ./pastelup install walletnode -n={network_name} --force -r=latest -p=18.118.218.206,18.116.26.219 && \
                        sed -i -e '/hostname/s/localhost/0.0.0.0/' ~/.pastel/walletnode.yml && \
                        sed -i -e '$arpcbind=0.0.0.0' ~/.pastel/pastel.conf && \
                        sed -i -e '$arpcallowip=172.0.0.0/8' ~/.pastel/pastel.conf && \
                        sed -i -e 's/rpcuser=.*/rpcuser=rpc_user/' ~/.pastel/pastel.conf && \
                        sed -i -e 's/rpcpassword=.*/rpcpassword=rpc_pwd/' ~/.pastel/pastel.conf"
    #check if pastelup is already installed:
    if os.path.exists('~/pastelup/pastelup'):
        log.info('Pastelup is already installed!')
        log.info('Running pastelup install command...')
        try:
            command_result = os.system(command_string)
            if not command_result:
                log.info('Pastelup install command appears to have run successfully!')
        except:
            log.error('Error running pastelup install command! Message: ' + str(command_result))
    else:
        log.info('Pastelup is not installed, trying to install it...')
        try:
            install_result = os.system(install_pastelup_script_command_string)
            if not install_result:
                log.info('Pastelup installed successfully!')
                log.info('Running pastelup install command...')
                command_result = os.system(command_string)
            else:
                log.info('Pastelup installation failed! Message: ' + str(install_result))
        except:
            log.error('Error running pastelup install command! Message: ' + str(install_result))
    return
            
#_______________________________________________________________________________________________________________________________

rpc_host, rpc_port, rpc_user, rpc_password, other_flags = get_local_rpc_settings_func()
rpc_connection = AsyncAuthServiceProxy("http://%s:%s@%s:%s"%(rpc_user, rpc_password, rpc_host, rpc_port))

