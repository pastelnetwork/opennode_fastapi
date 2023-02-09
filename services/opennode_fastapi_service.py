import base64
import sqlite3
import io
import datetime
from typing import List, Optional

import sqlalchemy as sa
from sqlalchemy import func, update, delete, desc
from sqlalchemy.future import select
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine, AsyncSession
from fastapi import BackgroundTasks, Depends, Body

from data import db_session
from data.opennode_fastapi import OpenNodeFastAPIRequests, OpenAPISenseData, OpenAPIRawSenseData
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
import logging
import urllib.parse as urlparse
import datetime
import statistics

try:
    import urllib.parse as urlparse
except ImportError:
    import urlparse
from tornado import gen, ioloop
from tornado.httpclient import AsyncHTTPClient, HTTPRequest, HTTPError



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
    

class AsyncAuthServiceProxy(object):
    def __init__(self, service_url, service_name=None, timeout=HTTP_TIMEOUT,
                reconnect_timeout=2, reconnect_amount=5, max_clients=100,
                max_buffer_size=104857600):
        """
        :arg service_url: in format "http://{user}:{password}@{host}:{port}"
        :arg service_name: TBD
        :arg timeout: TBD
        :arg reconnect_timeout: TBD
        :arg reconnect_amount: TBD
        :arg int max_clients: max_clients is the number of concurrent
            requests that can be in progress. Look tornado's docs for
            SimpleAsyncHTTPClient
        :arg string max_buffer_size: is the number of bytes that can be
            read by IOStream. It defaults to 100mb. Look tornado's docs for
            SimpleAsyncHTTPClient
        """

        self.__service_url = service_url
        self.__reconnect_timeout = reconnect_timeout
        self.__reconnect_amount = reconnect_amount or 1
        self.__service_name = service_name
        self.__url = urlparse.urlparse(service_url)
        self.__http_client = AsyncHTTPClient(max_clients=max_clients,
            max_buffer_size=max_buffer_size)
        self.__id_count = 0
        (user, passwd) = (self.__url.username, self.__url.password)
        try:
            user = user.encode('utf8')
        except AttributeError:
            pass
        try:
            passwd = passwd.encode('utf8')
        except AttributeError:
            pass
        authpair = user + b':' + passwd
        self.__auth_header = b'Basic ' + base64.b64encode(authpair)

    def __getattr__(self, name):
        if name.startswith('__') and name.endswith('__'):
            # Python internal stuff
            raise AttributeError
        if self.__service_name is not None:
            name = "%s.%s" % (self.__service_name, name)
        return AsyncAuthServiceProxy(self.__service_url, name)

    @gen.coroutine
    def __call__(self, *args):
        self.__id_count += 1

        postdata = json.dumps({'version': '1.1',
                               'method': self.__service_name,
                               'params': args,
                               'id': self.__id_count})
        headers = {
            'Host': self.__url.hostname,
            'User-Agent': USER_AGENT,
            'Authorization': self.__auth_header,
            'Content-type': 'application/json'
        }

        req = HTTPRequest(url=self.__service_url, method="POST",
            body=postdata,
            headers=headers)

        for i in range(self.__reconnect_amount):
            try:
                if i > 0:
                    log.warning("Reconnect try #{0}".format(i+1))
                response = yield self.__http_client.fetch(req)
                break
            except HTTPError:
                err_msg = 'Failed to connect to {0}:{1}'.format(
                    self.__url.hostname, self.__url.port)
                rtm = self.__reconnect_timeout
                if rtm:
                    err_msg += ". Waiting {0} seconds.".format(rtm)
                log.exception(err_msg)
                if rtm:
                    # io_loop = ioloop.IOLoop.current()
                    # yield gen.Task(io_loop.add_timeout, timedelta(seconds=rtm))
                    yield gen.sleep(rtm)
        else:
            log.error("Reconnect tries exceed.")
            return
        response = json.loads(response.body, parse_float=decimal.Decimal)

        if response['error'] is not None:
            raise JSONRPCException(response['error'])
        elif 'result' not in response:
            raise JSONRPCException({
                'code': -343, 'message': 'missing JSON-RPC result'})
        else:
            raise gen.Return(response['result'])
       
        
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
        list_of_ticket_types = ['id', 'nft', 'offer', 'accept', 'transfer', 'nft-collection', 'nft-collection-act', 'royalty', 'username', 'ethereumaddress', 'action', 'action-act']
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
        return results_already_in_local_db
    else: #If we don't have the results in our local sqlite database, then we need to download them from the Sense API:
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
                    if 'list_of_text_strings' in alternative_rare_on_internet_dict:
                        number_of_text_strings = len(alternative_rare_on_internet_dict['list_of_text_strings'])
                        number_of_img_alt_strings = len(alternative_rare_on_internet_dict['list_of_image_alt_strings'])
                        if number_of_text_strings != number_of_img_alt_strings: # Older versions of dd-service did not return text strings for each result, but rather all strings on the page. So we need to deal with that here because we can't turn it into a dataframe:
                            list_of_alt_strings = alternative_rare_on_internet_dict['list_of_image_alt_strings']
                            boolean_filter_for_favicons = [x.find('Favicon') == -1 for x in list_of_alt_strings]
                            list_of_alternative_rare_on_internet__original_urls = alternative_rare_on_internet_dict['list_of_image_src_strings']
                            alternative_rare_on_internet__b64_image_strings = str(['data:image/jpeg;base64,' + x for idx, x in enumerate(alternative_rare_on_internet_dict['list_of_images_as_base64']) if boolean_filter_for_favicons[idx]])
                            alternative_rare_on_internet__original_urls = str([x for idx, x in enumerate(alternative_rare_on_internet_dict['list_of_image_src_strings']) if boolean_filter_for_favicons[idx]])
                            alternative_rare_on_internet__result_titles = ""
                            alternative_rare_on_internet__number_of_similar_results = sum(boolean_filter_for_favicons)
                    elif 'list_of_image_src_strings' in alternative_rare_on_internet_dict:
                        alternative_rare_on_internet_df = pd.DataFrame.from_records(alternative_rare_on_internet_dict)
                        alternative_rare_on_internet__b64_image_strings = str(alternative_rare_on_internet_df['list_of_image_src_strings'].values.tolist())
                        alternative_rare_on_internet__original_urls = str(alternative_rare_on_internet_df['list_of_href_strings'].values.tolist())
                        alternative_rare_on_internet__result_titles = str(alternative_rare_on_internet_df['list_of_image_alt_strings'].values.tolist())
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
            sense_data.alternative_rare_on_internet__result_titles = alternative_rare_on_internet__result_titles
            corresponding_pastel_blockchain_ticket_data = await get_pastel_blockchain_ticket_func(txid)            
            sense_data.corresponding_pastel_blockchain_ticket_data = str(corresponding_pastel_blockchain_ticket_data)
            
            async with db_session.create_async_session() as session:
                session.add(sense_data)
                await session.commit()
            return sense_data


async def get_raw_sense_results_by_registration_ticket_txid_func(txid: str) -> OpenAPIRawSenseData:
    async with db_session.create_async_session() as session: #First check if we already have the results in our local sqlite database:
        query = select(OpenAPIRawSenseData).filter(OpenAPIRawSenseData.sense_registration_ticket_txid == txid)
        result = await session.execute(query)
    results_already_in_local_db = result.scalar_one_or_none()
    if results_already_in_local_db is not None:
        return results_already_in_local_db
    else: #If we don't have the results in our local sqlite database, then we need to download them from the Sense API:
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
            return raw_sense_data


async def populate_database_with_all_sense_data_func():
    tickets_obj = await get_all_pastel_blockchain_tickets_func()
    sense_ticket_dict = json.loads(tickets_obj['action'])
    sense_ticket_df = pd.DataFrame(sense_ticket_dict).T
    sense_ticket_df_filtered = sense_ticket_df[sense_ticket_df['action_type'] == 'sense'].drop_duplicates(subset=['txid'])
    list_of_sense_registration_ticket_txids = sense_ticket_df_filtered['txid'].values.tolist()
    list_of_known_bad_txids_to_skip = ['296df4ad6ac126794d0981ad04a2ed6b42364659a7d8a13c40ebf397744001c5',
                                       'ce1dd0a4f42050445a81fb6dbc5f4fd3a63af4b611fd9e05c25d2eb1e4beefab']
    list_of_sense_registration_ticket_txids = [x for x in list_of_sense_registration_ticket_txids if x not in list_of_known_bad_txids_to_skip]
    pbar = tqdm(list_of_sense_registration_ticket_txids)
    for idx, current_txid in enumerate(pbar):
        pbar.set_description(f'Processing sense registration ticket with TXID: {current_txid}')
        time.sleep(0.25)
        current_sense_data = await get_parsed_sense_results_by_registration_ticket_txid_func(current_txid)
        current_raw_sense_data = await get_raw_sense_results_by_registration_ticket_txid_func(current_txid)
    return list_of_sense_registration_ticket_txids


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
rpc_connection = AsyncAuthServiceProxy("http://%s:%s@%s:%s"%(rpc_user, rpc_password, rpc_host, rpc_port), timeout=2)

