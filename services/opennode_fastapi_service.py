import base64
import sqlite3
import io
import datetime
from typing import List, Optional

import sqlalchemy as sa
from sqlalchemy import func, update, delete, desc
from sqlalchemy.future import select
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine, AsyncSession

from data import db_session
from data.opennode_fastapi import OpenNodeFastAPIRequests
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
    rpcport = '9932'
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

