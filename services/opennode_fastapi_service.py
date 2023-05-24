import base64
import sqlite3
import io
import datetime
from typing import List, Optional
import cachetools
import aiofiles
from aiofiles.os import stat as aio_stat

import sqlalchemy as sa
from sqlalchemy import func, update, delete, desc
from sqlalchemy.future import select
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine, AsyncSession
from fastapi import BackgroundTasks, Depends, Body

from data import db_session
from data.opennode_fastapi import OpenNodeFastAPIRequests, OpenAPISenseData, OpenAPIRawSenseData, PastelBlockData, PastelAddressData, PastelTransactionData, PastelTransactionInputData, PastelTransactionOutputData, OpenAPISenseTop10MostSimilarImagesData
import os
import sys
import time
import json
import warnings
import ipaddress
import re
import random
import itertools
import requests
import hashlib
import asyncio
import numpy as np
import pandas as pd
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
# from tornado import gen, ioloop
# from tornado.httpclient import AsyncHTTPClient, HTTPRequest, HTTPError
import aiohttp
from httpx import AsyncClient
import logging
from sqlalchemy import Index
from sqlalchemy.orm import aliased
from sqlalchemy import select
from tabulate import tabulate

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
HTTP_TIMEOUT = 30

logging.basicConfig()
log = logging.getLogger("PastelRPC")

# Initialize the LRU cache and cache directory
cache_dir = "/home/ubuntu/cascade_opennode_fastapi_cache"
#create cache directory if it doesn't exist
os.makedirs(cache_dir, exist_ok=True)
cache = cachetools.LRUCache(maxsize=5 * 1024 * 1024 * 1024)  # 5 GB

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
    def __init__(self, service_url, service_name=None, reconnect_timeout=2, reconnect_amount=2):
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
        print('Local machine is not a supernode!')
        return 0, 0, 0, 0
    else:
        print('Local machine is a supernode!')
        local_sn_rank = local_machine_supernode_data['rank'].values[0]
        local_sn_pastelid = local_machine_supernode_data['extKey'].values[0]
    return local_machine_supernode_data, local_sn_rank, local_sn_pastelid, local_machine_ip_with_proper_port


def get_sn_data_from_pastelid_func(specified_pastelid):
    supernode_list_full_df = check_supernode_list_func()
    specified_machine_supernode_data = supernode_list_full_df[supernode_list_full_df['extKey'] == specified_pastelid]
    if len(specified_machine_supernode_data) == 0:
        print('Specified machine is not a supernode!')
        return pd.DataFrame()
    else:
        return specified_machine_supernode_data

    
def get_sn_data_from_sn_pubkey_func(specified_sn_pubkey):
    supernode_list_full_df = check_supernode_list_func()
    specified_machine_supernode_data = supernode_list_full_df[supernode_list_full_df['pubkey'] == specified_sn_pubkey]
    if len(specified_machine_supernode_data) == 0:
        print('Specified machine is not a supernode!')
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
        activation_response_json = await rpc_connection.tickets('find', 'action-act', txid )
        if len(activation_response_json) > 0:
            response_json['activation_ticket'] = activation_response_json
        else:
            response_json['activation_ticket'] = 'No activation ticket found for this ticket-- check again soon'
        return response_json
    else:
        response_json = 'No ticket found for this txid'
    return response_json


async def get_all_pastel_blockchain_tickets_func(verbose=0):
    with MyTimer():
        if verbose:
            print('Now retrieving all Pastel blockchain tickets...')
        tickets_obj = {}
        list_of_ticket_types = ['id', 'nft', 'offer', 'accept', 'transfer', 'royalty', 'username', 'ethereumaddress', 'action', 'action-act'] # 'collection', 'collection-act'
        for current_ticket_type in list_of_ticket_types:
            if verbose:
                print('Getting ' + current_ticket_type + ' tickets...')
            response = await rpc_connection.tickets('list', current_ticket_type)
            if response is not None and len(response) > 0:
                tickets_obj[current_ticket_type] = await get_df_json_from_tickets_list_rpc_response_func(response)
    return tickets_obj


async def testnet_pastelid_file_dispenser_func(password, verbose=0):
    with MyTimer():
        print('Now generating a pastelid...')
        response = await rpc_connection.pastelid('newkey', password)
        pastelid_data = ''
        pastelid_pubkey = ''
        if response is not None and len(response) > 0:
            if 'pastelid' in response:
                print('The pastelid is ' + response['pastelid'])    
                print('Now checking to see if the pastelid file exists...')
                pastelid_pubkey = response['pastelid']
                if os.path.exists('~/.pastel/testnet3/pastelkeys/' + response['pastelid']):
                    print('The pastelid file exists!')
                    with open('~/.pastel/testnet3/pastelkeys/' + response['pastelid'], 'rb') as f:
                        pastelid_data = f.read()
                        return pastelid_data                     
                else:
                    print('The pastelid file does not exist!')
            else:
                print('There was an issue creating the pastelid!')
    return pastelid_pubkey, pastelid_data


async def get_parsed_sense_results_by_registration_ticket_txid_func(txid: str) -> OpenAPISenseData:
    async with db_session.create_async_session() as session: #First check if we already have the results in our local sqlite database:
        query = select(OpenAPISenseData).filter(OpenAPISenseData.sense_registration_ticket_txid == txid)
        result = await session.execute(query)
    results_already_in_local_db = result.scalar_one_or_none()
    if results_already_in_local_db is not None:
        is_cached_response = True
        return results_already_in_local_db, is_cached_response
    else: #If we don't have the results in our local sqlite database, then we need to download them from the Sense API:
        is_cached_response = False
        requester_pastelid = 'jXYwVLikSSJfoX7s4VpX3osfMWnBk3Eahtv5p1bYQchaMiMVzAmPU57HMA7fz59ffxjd2Y57b9f7oGqfN5bYou'
        request_url = f'http://localhost:8080/openapi/sense/download?pid={requester_pastelid}&txid={txid}'
        headers = {'Authorization': 'testpw123'}
        with MyTimer():
            async with httpx.AsyncClient() as client:
                response = await client.get(request_url, headers=headers, timeout=60.0)    
            parsed_response = response.json()
            if parsed_response['file'] is None:
                error_string = 'No file was returned from the Sense API!'
                print(error_string)
                return error_string
            decoded_response = base64.b64decode(parsed_response['file'])
            final_response = json.loads(decoded_response)
            final_response_df = pd.DataFrame.from_records([final_response])
        with MyTimer(): #Now we have the raw results from the Sense walletnode API, but we want to parse out the various fields and generate valid results that turn into valid json, and also save the results to our local sqlite database:
            rareness_scores_table_json_compressed_b64 = final_response['rareness_scores_table_json_compressed_b64']
            rareness_scores_table_json = str(zstd.decompress(base64.b64decode(rareness_scores_table_json_compressed_b64)))[2:-1]
            rareness_scores_table_dict = json.loads(rareness_scores_table_json)
            top_10_most_similar_registered_images_on_pastel_file_hashes = list(rareness_scores_table_dict['image_hash'].values())
            is_likely_dupe_list = list(rareness_scores_table_dict['is_likely_dupe'].values())
            detected_dupes_from_registered_images_on_pastel_file_hashes = [x for idx, x in enumerate(top_10_most_similar_registered_images_on_pastel_file_hashes) if is_likely_dupe_list[idx]]
            detected_dupes_from_registered_images_on_pastel_thumbnail_strings = [x[0][0] for idx, x in enumerate(list(rareness_scores_table_dict['thumbnail'].values())) if is_likely_dupe_list[idx]]
            
            internet_rareness_json = final_response['internet_rareness']
            if internet_rareness_json['min_number_of_exact_matches_in_page'] == 0:
                internet_rareness__b64_image_strings_of_in_page_matches = ''
                internet_rareness__original_urls_of_in_page_matches = ''
                internet_rareness__result_titles_of_in_page_matches = ''
                internet_rareness__date_strings_of_in_page_matches = ''
            else:
                internet_rareness_summary_table_json = str(zstd.decompress(base64.b64decode(internet_rareness_json['rare_on_internet_summary_table_as_json_compressed_b64'])))[2:-1]
                try:
                    internet_rareness_summary_table_dict = json.loads(internet_rareness_summary_table_json.encode('utf-8').decode('unicode_escape'))
                except Exception as e:
                    print(f"Encountered an error while trying to parse internet_rareness_summary_table_json: {e}")
                    internet_rareness_summary_table_dict = dirtyjson.loads(internet_rareness_summary_table_json.replace('\\"', '"').replace('\/', '/'))
                internet_rareness_summary_table_df = pd.DataFrame.from_records(internet_rareness_summary_table_dict)
                if 'img_src_string' in internet_rareness_summary_table_df.columns:
                    internet_rareness__b64_image_strings_of_in_page_matches = str(internet_rareness_summary_table_df['img_src_string'].values.tolist())
                else:
                    internet_rareness__b64_image_strings_of_in_page_matches = ''
                if 'original_url' in internet_rareness_summary_table_df.columns:
                    internet_rareness__original_urls_of_in_page_matches = str(internet_rareness_summary_table_df['original_url'].values.tolist())
                else:
                    internet_rareness__original_urls_of_in_page_matches = ''
                if 'title' in internet_rareness_summary_table_df.columns:
                    internet_rareness__result_titles_of_in_page_matches = str(internet_rareness_summary_table_df['title'].values.tolist())
                else:
                    internet_rareness__result_titles_of_in_page_matches = ''
                if 'date_string' in internet_rareness_summary_table_df.columns:
                    internet_rareness__date_strings_of_in_page_matches = str(internet_rareness_summary_table_df['date_string'].values.tolist())
                else:
                    internet_rareness__date_strings_of_in_page_matches = ''
            alternative_rare_on_internet_dict_as_json = str(zstd.decompress(base64.b64decode(internet_rareness_json['alternative_rare_on_internet_dict_as_json_compressed_b64'])))[2:-1]
            if alternative_rare_on_internet_dict_as_json == '':
                alternative_rare_on_internet__b64_image_strings = ''
                alternative_rare_on_internet__original_urls = ''
                alternative_rare_on_internet__result_titles = ''
                alternative_rare_on_internet__number_of_similar_results = ''
            else:
                try:
                    alternative_rare_on_internet_dict = json.loads(alternative_rare_on_internet_dict_as_json.encode('utf-8').decode('unicode_escape'))
                except Exception as e:
                    print(f"Encountered an error while trying to parse alternative_rare_on_internet_dict_as_json: {e}")
                    alternative_rare_on_internet_dict = dirtyjson.loads(alternative_rare_on_internet_dict_as_json.replace('\\"', '"').replace('\/', '/').replace('\\n', ' '))
                if 'list_of_image_src_strings' in alternative_rare_on_internet_dict and len(alternative_rare_on_internet_dict['list_of_image_src_strings']) == 0:
                    alternative_rare_on_internet__b64_image_strings = ''
                    alternative_rare_on_internet__original_urls = ''
                    alternative_rare_on_internet__result_titles = ''
                    alternative_rare_on_internet__number_of_similar_results = ''
                else:
                    if 'list_of_image_src_strings' in alternative_rare_on_internet_dict:
                        alternative_rare_on_internet_df = pd.DataFrame.from_records(alternative_rare_on_internet_dict)
                        alternative_rare_on_internet__b64_image_strings = str(alternative_rare_on_internet_df['list_of_image_src_strings'].values.tolist())
                        alternative_rare_on_internet__original_urls = str(alternative_rare_on_internet_df['list_of_href_strings'].values.tolist())
                        alternative_rare_on_internet__number_of_similar_results = len(alternative_rare_on_internet_df)
                    else:
                        alternative_rare_on_internet__b64_image_strings = ''
                        alternative_rare_on_internet__original_urls = ''
                        alternative_rare_on_internet__result_titles = ''
                        alternative_rare_on_internet__number_of_similar_results = ''
            sense_data = OpenAPISenseData()
            sense_data.sense_registration_ticket_txid = txid
            sense_data.hash_of_candidate_image_file = final_response_df['hash_of_candidate_image_file'][0]
            sense_data.pastel_id_of_submitter = final_response_df['pastel_id_of_submitter'][0]
            sense_data.pastel_block_hash_when_request_submitted = final_response_df['pastel_block_hash_when_request_submitted'][0]
            sense_data.pastel_block_height_when_request_submitted = str(final_response_df['pastel_block_height_when_request_submitted'][0])
            sense_data.dupe_detection_system_version = str(final_response_df['dupe_detection_system_version'][0])
            sense_data.overall_rareness_score = final_response_df['overall_rareness_score '][0]
            sense_data.open_nsfw_score = final_response_df['open_nsfw_score'][0] 
            sense_data.alternative_nsfw_scores = str(final_response_df['alternative_nsfw_scores'][0])
            sense_data.utc_timestamp_when_request_submitted = final_response_df['utc_timestamp_when_request_submitted'][0]
            sense_data.is_likely_dupe = str(final_response_df['is_likely_dupe'][0])
            sense_data.is_rare_on_internet = str(final_response_df['is_rare_on_internet'][0])
            sense_data.is_pastel_openapi_request = str(final_response_df['is_pastel_openapi_request'][0])
            sense_data.image_fingerprint_of_candidate_image_file = str(final_response_df['image_fingerprint_of_candidate_image_file'][0])
            sense_data.pct_of_top_10_most_similar_with_dupe_prob_above_25pct = float(final_response_df['pct_of_top_10_most_similar_with_dupe_prob_above_25pct'][0])
            sense_data.pct_of_top_10_most_similar_with_dupe_prob_above_33pct = float(final_response_df['pct_of_top_10_most_similar_with_dupe_prob_above_33pct'][0])
            sense_data.pct_of_top_10_most_similar_with_dupe_prob_above_50pct = float(final_response_df['pct_of_top_10_most_similar_with_dupe_prob_above_50pct'][0])
            sense_data.internet_rareness__min_number_of_exact_matches_in_page = str(internet_rareness_json['min_number_of_exact_matches_in_page'])
            sense_data.internet_rareness__earliest_available_date_of_internet_results = internet_rareness_json['earliest_available_date_of_internet_results']
            sense_data.internet_rareness__b64_image_strings_of_in_page_matches = internet_rareness__b64_image_strings_of_in_page_matches
            sense_data.internet_rareness__original_urls_of_in_page_matches = internet_rareness__original_urls_of_in_page_matches
            sense_data.internet_rareness__result_titles_of_in_page_matches = internet_rareness__result_titles_of_in_page_matches
            sense_data.internet_rareness__date_strings_of_in_page_matches = internet_rareness__date_strings_of_in_page_matches
            sense_data.alternative_rare_on_internet__number_of_similar_results = str(alternative_rare_on_internet__number_of_similar_results)
            sense_data.alternative_rare_on_internet__b64_image_strings = alternative_rare_on_internet__b64_image_strings
            sense_data.alternative_rare_on_internet__original_urls = alternative_rare_on_internet__original_urls
            corresponding_pastel_blockchain_ticket_data = await get_pastel_blockchain_ticket_func(txid)            
            sense_data.corresponding_pastel_blockchain_ticket_data = str(corresponding_pastel_blockchain_ticket_data)
            async with db_session.create_async_session() as session:
                session.add(sense_data)
                await session.commit()
            return sense_data, is_cached_response


async def get_sense_results_top_10_most_similar_images_by_registration_ticket_txid_func(txid: str) -> OpenAPISenseData:
    async with db_session.create_async_session() as session: #First check if we already have the results in our local sqlite database:
        query = select(OpenAPISenseTop10MostSimilarImagesData).filter(OpenAPISenseTop10MostSimilarImagesData.sense_registration_ticket_txid == txid)
        result = await session.execute(query)
    results_already_in_local_db = result.scalar_one_or_none()
    if results_already_in_local_db is not None:
        is_cached_response = True
        return results_already_in_local_db, is_cached_response
    else: #If we don't have the results in our local sqlite database, then we need to download them from the Sense API:
        is_cached_response = False
        requester_pastelid = 'jXYwVLikSSJfoX7s4VpX3osfMWnBk3Eahtv5p1bYQchaMiMVzAmPU57HMA7fz59ffxjd2Y57b9f7oGqfN5bYou'
        request_url = f'http://localhost:8080/openapi/sense/download?pid={requester_pastelid}&txid={txid}'
        headers = {'Authorization': 'testpw123'}
        with MyTimer():
            async with httpx.AsyncClient() as client:
                response = await client.get(request_url, headers=headers, timeout=60.0)    
            parsed_response = response.json()
            if parsed_response['file'] is None:
                error_string = 'No file was returned from the Sense API!'
                print(error_string)
                return error_string
            decoded_response = base64.b64decode(parsed_response['file'])
            final_response = json.loads(decoded_response)
            final_response_df = pd.DataFrame.from_records([final_response])
        with MyTimer(): #Now we have the raw results from the Sense walletnode API, but we want to parse out the various fields and generate valid results that turn into valid json, and also save the results to our local sqlite database:
            rareness_scores_table_json_compressed_b64 = final_response['rareness_scores_table_json_compressed_b64']
            rareness_scores_table_json = str(zstd.decompress(base64.b64decode(rareness_scores_table_json_compressed_b64)))[2:-1]
            most_similar_images_data = OpenAPISenseTop10MostSimilarImagesData()
            most_similar_images_data.sense_registration_ticket_txid = txid
            most_similar_images_data.hash_of_candidate_image_file = final_response_df['hash_of_candidate_image_file'][0]
            most_similar_images_data.rareness_scores_table_json = rareness_scores_table_json
            async with db_session.create_async_session() as session:
                session.add(most_similar_images_data)
                await session.commit()
            return most_similar_images_data, is_cached_response


async def get_raw_sense_results_by_registration_ticket_txid_func(txid: str) -> OpenAPIRawSenseData:
    async with db_session.create_async_session() as session: #First check if we already have the results in our local sqlite database:
        query = select(OpenAPIRawSenseData).filter(OpenAPIRawSenseData.sense_registration_ticket_txid == txid)
        result = await session.execute(query)
    results_already_in_local_db = result.scalar_one_or_none()
    if results_already_in_local_db is not None:
        is_cached_response = True
        return results_already_in_local_db, is_cached_response
    else: #If we don't have the results in our local sqlite database, then we need to download them from the Sense API:
        is_cached_response = False
        requester_pastelid = 'jXYwVLikSSJfoX7s4VpX3osfMWnBk3Eahtv5p1bYQchaMiMVzAmPU57HMA7fz59ffxjd2Y57b9f7oGqfN5bYou'
        request_url = f'http://localhost:8080/openapi/sense/download?pid={requester_pastelid}&txid={txid}'
        headers = {'Authorization': 'testpw123'}
        with MyTimer():
            async with httpx.AsyncClient() as client:
                response = await client.get(request_url, headers=headers, timeout=60.0)    
            parsed_response = response.json()
            if parsed_response['file'] is None:
                error_string = 'No file was returned from the Sense API!'
                print(error_string)
                return error_string
            decoded_response = base64.b64decode(parsed_response['file'])
            final_response = json.loads(decoded_response)
            final_response_df = pd.DataFrame.from_records([final_response])
            raw_sense_data = OpenAPIRawSenseData()
            raw_sense_data.sense_registration_ticket_txid = txid
            raw_sense_data.hash_of_candidate_image_file = final_response_df['hash_of_candidate_image_file'][0]
            raw_sense_data.pastel_id_of_submitter = final_response_df['pastel_id_of_submitter'][0]
            raw_sense_data.pastel_block_hash_when_request_submitted = final_response_df['pastel_block_hash_when_request_submitted'][0]
            raw_sense_data.pastel_block_height_when_request_submitted = str(final_response_df['pastel_block_height_when_request_submitted'][0])
            raw_sense_data.raw_sense_data_json = decoded_response
            corresponding_pastel_blockchain_ticket_data = await get_pastel_blockchain_ticket_func(txid)            
            raw_sense_data.corresponding_pastel_blockchain_ticket_data = str(corresponding_pastel_blockchain_ticket_data)
            
            async with db_session.create_async_session() as session:
                session.add(raw_sense_data)
                await session.commit()
            return raw_sense_data, is_cached_response


async def download_publicly_accessible_cascade_file_by_registration_ticket_txid_func(txid: str):
    requester_pastelid = 'jXYwVLikSSJfoX7s4VpX3osfMWnBk3Eahtv5p1bYQchaMiMVzAmPU57HMA7fz59ffxjd2Y57b9f7oGqfN5bYou'
    request_url = f'http://localhost:8080/openapi/cascade/download?pid={requester_pastelid}&txid={txid}'
    headers = {'Authorization': 'testpw123'}
    is_publicly_accessible = True
    try:
        print(f'Attempting to get original file name from the Cascade blockchain ticket for registration txid {txid}...')
        ticket_response = await get_pastel_blockchain_ticket_func(txid)
        action_ticket = json.loads(base64.b64decode(ticket_response['ticket']['action_ticket']))
        api_ticket_str = action_ticket['api_ticket']
        correct_padding = len(api_ticket_str) % 4
        if correct_padding != 0:
            api_ticket_str += '='* (4 - correct_padding)
        api_ticket =  json.loads(base64.b64decode(api_ticket_str).decode('utf-8'))
        original_file_name_string = api_ticket['file_name']
        is_publicly_accessible = api_ticket['make_publicly_accessible']
        print(f'Got original file name from the Cascade blockchain ticket: {original_file_name_string}')
    except:
        print('Unable to get original file name from the Cascade blockchain ticket! Using txid instead as the default file name...')
        original_file_name_string = str(txid)
    if is_publicly_accessible == True:
        with MyTimer():
            if txid in cache: # Check if the file is already in cache
                print(f"File is already cached, returning the cached file for txid {txid}...")
                async with aiofiles.open(cache[txid], mode='rb') as f:
                    decoded_response = await f.read()
                return decoded_response, original_file_name_string
            print(f'Now attempting to download the file from Cascade API for txid {txid}...')
            async with httpx.AsyncClient() as client:
                response = await client.get(request_url, headers=headers, timeout=300.0)
            parsed_response = response.json()
            print(f'Got response from Cascade API for txid {txid}')
            if parsed_response['file'] is None:
                error_string = 'No file was returned from the Cascade API!'
                print(error_string)
                return error_string, original_file_name_string
            decoded_response = base64.b64decode(parsed_response['file'])
            print(f'Saving the file to cache for txid {txid}...')
            cache_file_path = os.path.join(cache_dir, txid)
            async with aiofiles.open(cache_file_path, mode='wb') as f:
                await f.write(decoded_response)
            cache[txid] = cache_file_path # Update LRU cache
            total_size = 0
            for f in cache.values(): # Check if the cache is full
                stat_result = await aio_stat(f)
                total_size += stat_result.st_size
            if total_size > cache.maxsize: # Cache is full, remove the least recently used item from the cache
                lru_txid, _ = cache.popitem()
                os.remove(os.path.join(cache_dir, lru_txid))
                print(f'Removed txid {lru_txid} from cache!')
                print(f'Successfully decoded response from Cascade API for txid {txid}!')
    else:
        decoded_response = f'The file for the Cascade ticket with registration txid {txid} is not publicly accessible!'
        print(decoded_response)
    return decoded_response, original_file_name_string


async def populate_database_with_all_sense_data_func():
    tickets_obj = await get_all_pastel_blockchain_tickets_func()
    sense_ticket_dict = json.loads(tickets_obj['action'])
    sense_ticket_df = pd.DataFrame(sense_ticket_dict).T
    sense_ticket_df_filtered = sense_ticket_df[sense_ticket_df['action_type'] == 'sense'].drop_duplicates(subset=['txid'])
    list_of_sense_registration_ticket_txids = sense_ticket_df_filtered['txid'].values.tolist()
    list_of_known_bad_txids_to_skip = ['296df4ad6ac126794d0981ad04a2ed6b42364659a7d8a13c40ebf397744001c5',
                                       'ce1dd0a4f42050445a81fb6dbc5f4fd3a63af4b611fd9e05c25d2eb1e4beefab']
    list_of_sense_registration_ticket_txids = [x for x in list_of_sense_registration_ticket_txids if x not in list_of_known_bad_txids_to_skip]
    for current_txid in list_of_sense_registration_ticket_txids[::-1]:
        try:
            current_sense_data, is_cached_response__parsed = await get_parsed_sense_results_by_registration_ticket_txid_func(current_txid)
            current_raw_sense_data, is_cached_response__raw = await get_raw_sense_results_by_registration_ticket_txid_func(current_txid)
            current_most_similar_images_data, is_cached_response__most_similar_images = await get_sense_results_top_10_most_similar_images_by_registration_ticket_txid_func(current_txid)
            
            if is_cached_response__parsed and is_cached_response__raw and is_cached_response__most_similar_images:
                break
            else:
                print(f'Processing sense registration ticket with TXID: {current_txid}')
        except Exception as e:
            pass


async def run_populate_database_with_all_sense_data_func(background_tasks: BackgroundTasks = Depends):
    background_tasks.add_task(await populate_database_with_all_sense_data_func())
    return {"message": 'Started background task to populate database with all sense data...'}


async def get_parsed_sense_results_by_image_file_hash_func(image_file_hash: str) -> Optional[List[OpenAPISenseData]]:
    async with db_session.create_async_session() as session: #First check if we already have the results in our local sqlite database:
        query = select(OpenAPISenseData).filter(OpenAPISenseData.hash_of_candidate_image_file == image_file_hash)
        result = await session.execute(query)
    results_already_in_local_db = result.scalars()
    list_of_results = list({r for r in results_already_in_local_db})
    if results_already_in_local_db is not None:
        return list_of_results
    else: #If we don't have the results in our local sqlite database, then we need to download them from the Sense API:
        error_string = 'Cannot find a sense registration ticket with that image file hash-- it might still be processing or it might not exist!'
        print(error_string)
        return error_string


async def get_raw_sense_results_by_image_file_hash_func(image_file_hash: str) ->  Optional[List[OpenAPISenseData]]:
    async with db_session.create_async_session() as session: #First check if we already have the results in our local sqlite database:
        query = select(OpenAPIRawSenseData).filter(OpenAPIRawSenseData.hash_of_candidate_image_file == image_file_hash)
        result = await session.execute(query)
    results_already_in_local_db = result.scalars()
    list_of_results = list({r for r in results_already_in_local_db})
    if results_already_in_local_db is not None:
        return list_of_results
    else: #If we don't have the results in our local sqlite database, then we need to download them from the Sense API:
        error_string = 'Cannot find a sense registration ticket with that image file hash-- it might still be processing or it might not exist!'
        print(error_string)
        return error_string
    

async def get_parsed_sense_results_by_pastel_id_of_submitter_func(pastel_id_of_submitter: str) ->  Optional[List[OpenAPISenseData]]:
    async with db_session.create_async_session() as session: #First check if we already have the results in our local sqlite database:
        query = select(OpenAPISenseData).filter(OpenAPISenseData.pastel_id_of_submitter == pastel_id_of_submitter)
        result = await session.execute(query)
    results_already_in_local_db = result.scalars()
    list_of_results = list({r for r in results_already_in_local_db})
    if results_already_in_local_db is not None:
        return list_of_results
    else: #If we don't have the results in our local sqlite database, then we need to download them from the Sense API:
        error_string = 'Cannot find any sense registration tickets for that PastelID -- they might still be processing or they might not exist!'
        print(error_string)
        return error_string


async def get_raw_sense_results_by_pastel_id_of_submitter_func(pastel_id_of_submitter: str) -> Optional[List[OpenAPIRawSenseData]]:
    async with db_session.create_async_session() as session: #First check if we already have the results in our local sqlite database:
        query = select(OpenAPIRawSenseData).filter(OpenAPIRawSenseData.pastel_id_of_submitter == pastel_id_of_submitter)
        result = await session.execute(query)
    results_already_in_local_db = result.scalars()
    list_of_results = list({r for r in results_already_in_local_db})
    if results_already_in_local_db is not None:
        return list_of_results
    else: #If we don't have the results in our local sqlite database, then we need to download them from the Sense API:
        error_string = 'Cannot find any sense registration tickets for that PastelID -- they might still be processing or they might not exist!'
        print(error_string)
        return error_string
    

async def get_parsed_sense_results_by_pastel_block_hash_when_request_submitted_func(pastel_block_hash_when_request_submitted: str) ->  Optional[List[OpenAPISenseData]]:
    async with db_session.create_async_session() as session: #First check if we already have the results in our local sqlite database:
        query = select(OpenAPISenseData).filter(OpenAPISenseData.pastel_block_hash_when_request_submitted == pastel_block_hash_when_request_submitted)
        result = await session.execute(query)
    results_already_in_local_db = result.scalars()
    list_of_results = list({r for r in results_already_in_local_db})
    if results_already_in_local_db is not None:
        return list_of_results
    else: #If we don't have the results in our local sqlite database, then we need to download them from the Sense API:
        error_string = 'Cannot find any sense registration tickets for that PastelID -- they might still be processing or they might not exist!'
        print(error_string)
        return error_string


async def get_raw_sense_results_by_pastel_block_hash_when_request_submitted_func(pastel_block_hash_when_request_submitted: str) -> Optional[List[OpenAPIRawSenseData]]:
    async with db_session.create_async_session() as session: #First check if we already have the results in our local sqlite database:
        query = select(OpenAPIRawSenseData).filter(OpenAPIRawSenseData.pastel_block_hash_when_request_submitted == pastel_block_hash_when_request_submitted)
        result = await session.execute(query)
    results_already_in_local_db = result.scalars()
    list_of_results = list({r for r in results_already_in_local_db})
    if results_already_in_local_db is not None:
        return list_of_results
    else: #If we don't have the results in our local sqlite database, then we need to download them from the Sense API:
        error_string = 'Cannot find any sense registration tickets for that PastelID -- they might still be processing or they might not exist!'
        print(error_string)
        return error_string


async def get_parsed_sense_results_by_pastel_block_height_when_request_submitted_func(pastel_block_height_when_request_submitted: str) ->  Optional[List[OpenAPISenseData]]:
    async with db_session.create_async_session() as session: #First check if we already have the results in our local sqlite database:
        query = select(OpenAPISenseData).filter(OpenAPISenseData.pastel_block_height_when_request_submitted == pastel_block_height_when_request_submitted)
        result = await session.execute(query)
    results_already_in_local_db = result.scalars()
    list_of_results = list({r for r in results_already_in_local_db})
    if results_already_in_local_db is not None:
        return list_of_results
    else: #If we don't have the results in our local sqlite database, then we need to download them from the Sense API:
        error_string = 'Cannot find any sense registration tickets for that PastelID -- they might still be processing or they might not exist!'
        print(error_string)
        return error_string


async def get_raw_sense_results_by_pastel_block_height_when_request_submitted_func(pastel_block_height_when_request_submitted: str) -> Optional[List[OpenAPIRawSenseData]]:
    async with db_session.create_async_session() as session: #First check if we already have the results in our local sqlite database:
        query = select(OpenAPIRawSenseData).filter(OpenAPIRawSenseData.pastel_block_height_when_request_submitted == pastel_block_height_when_request_submitted)
        result = await session.execute(query)
    results_already_in_local_db = result.scalars()
    list_of_results = list({r for r in results_already_in_local_db})
    if results_already_in_local_db is not None:
        return list_of_results
    else: #If we don't have the results in our local sqlite database, then we need to download them from the Sense API:
        error_string = 'Cannot find any sense registration tickets for that PastelID -- they might still be processing or they might not exist!'
        print(error_string)
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
    current_datetime_utc = datetime.datetime.utcnow()
    current_datetime_utc_string = current_datetime_utc.strftime("%Y-%m-%d %H:%M:%S")
    timestamp = int(datetime.datetime.timestamp(current_datetime_utc))
    response = {'total_number_of_registered_sense_fingerprints': fingerprint_counter, 'as_of_datetime_utc_string': current_datetime_utc_string, 'as_of_timestamp': timestamp}
    return response


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
    current_datetime_utc = datetime.datetime.utcnow()
    current_datetime_utc_string = current_datetime_utc.strftime("%Y-%m-%d %H:%M:%S")
    timestamp = int(datetime.datetime.timestamp(current_datetime_utc))
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
            timestamp=datetime.datetime.fromtimestamp(block_data['time'])
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
        print(msg.format(time=round(runtime, 2)))


def compute_elapsed_time_in_minutes_between_two_datetimes_func(start_datetime, end_datetime):
    time_delta = (end_datetime - start_datetime)
    total_seconds_elapsed = time_delta.total_seconds()
    total_minutes_elapsed = total_seconds_elapsed / 60
    return total_minutes_elapsed


def compute_elapsed_time_in_minutes_since_start_datetime_func(start_datetime):
    end_datetime = datetime.datetime.now()
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
        print('Validation Error: ' + str(e))
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
        print('Problem running pastel-cli command!')
        current_pastel_block_number = ''
    if isinstance(current_pastel_block_number, int):
        if current_pastel_block_number > 100000:
            pasteld_running_correctly = 1
            print('Pasteld is running correctly!')
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
        print('Pastelup is already installed!')
        print('Running pastelup install command...')
        try:
            command_result = os.system(command_string)
            if not command_result:
                print('Pastelup install command appears to have run successfully!')
        except:
            print('Error running pastelup install command! Message: ' + str(command_result))
    else:
        print('Pastelup is not installed, trying to install it...')
        try:
            install_result = os.system(install_pastelup_script_command_string)
            if not install_result:
                print('Pastelup installed successfully!')
                print('Running pastelup install command...')
                command_result = os.system(command_string)
            else:
                print('Pastelup installation failed! Message: ' + str(install_result))
        except:
            print('Error running pastelup install command! Message: ' + str(install_result))
    return
            
#_______________________________________________________________________________________________________________________________

rpc_host, rpc_port, rpc_user, rpc_password, other_flags = get_local_rpc_settings_func()
rpc_connection = AsyncAuthServiceProxy("http://%s:%s@%s:%s"%(rpc_user, rpc_password, rpc_host, rpc_port))

