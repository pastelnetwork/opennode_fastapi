import base64
import json
import random
import sys
import io
from typing import Union, Dict, Any

import fastapi
from fastapi import BackgroundTasks, Depends, Response, Query
from fastapi.responses import JSONResponse, StreamingResponse, HTMLResponse
from fastapi_chameleon import template
from starlette.requests import Request
from starlette import status

from viewmodels.opennode_fastapi.data_viewmodel import DataViewModel
from data.opennode_fastapi import OpenNodeFastAPIRequests, ValidationError, ShowLogsIncrementalModel, LogLines
from services import opennode_fastapi_service
from fastapi import BackgroundTasks
from json import JSONEncoder
from datetime import datetime, timedelta
import aiohttp
import asyncio
import httpx
from pytz import timezone


router = fastapi.APIRouter()

from services.opennode_fastapi_service import *

tags_metadata = [
    {"name": "High-Level Methods", "description": "Endpoints that are not actually part of the Pastel RPC API, but operate at a higher level of abstraction."},
    {"name": "OpenAPI Methods", "description": "Endpoints that are interact with both the Pastel RPC API and also the Walletnode API to get information on Sense and Cascade."},
    {"name": "Blockchain Methods", "description": "Endpoints for retrieving blockchain data"},
    {"name": "Mining Methods", "description": "Endpoints for retrieving mining data"},
    {"name": "Ticket Methods", "description": "Endpoints for retrieving blockchain ticket data"},
    {"name": "Supernode Methods", "description": "Endpoints for retrieving Supernode data"},
    {"name": "Network Methods", "description": "Endpoints for retrieving network data"},
    {"name": "Raw Transaction Methods", "description": "Endpoints for working with raw transactions"},
    {"name": "Utility Methods", "description": "Endpoints for various utility functions"},
    {"name": "Control Methods", "description": "Endpoints for various control methods"},
]

class DateTimeEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()


@router.get('/getbestblockhash', tags=["Blockchain Methods"])
async def getbestblockhash() -> str:
    try:
        global rpc_connection
        response_json = await rpc_connection.getbestblockhash()
        return response_json
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)


@router.get('/getblockchaininfo', tags=["Blockchain Methods"])
async def getblockchaininfo() -> str:
    try:
        global rpc_connection
        response_json = await rpc_connection.getblockchaininfo()
        return str(response_json)
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)


@router.get('/gettxoutsetinfo', tags=["Blockchain Methods"])
async def gettxoutsetinfo() -> str:
    try:
        global rpc_connection
        response_json = await rpc_connection.gettxoutsetinfo()
        return str(response_json)
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)


@router.get('/getblockcount', tags=["Blockchain Methods"])
async def getblockcount():
    try:
        global rpc_connection
        response_json = await rpc_connection.getblockcount()
        return response_json
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)


@router.get('/getchaintips', tags=["Blockchain Methods"])
async def getchaintips():
    try:
        global rpc_connection
        response_json = await rpc_connection.getchaintips()
        return response_json
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)


@router.get('/getdifficulty', tags=["Blockchain Methods"])
async def getdifficulty():
    try:
        global rpc_connection
        response_json = await rpc_connection.getdifficulty()
        return response_json
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)


@router.get('/getmempoolinfo', tags=["Blockchain Methods"])
async def getmempoolinfo():
    try:
        global rpc_connection
        response_json = await rpc_connection.getmempoolinfo()
        return response_json
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)    


@router.get('/getrawmempool', tags=["Blockchain Methods"])
async def getrawmempool():
    try:
        global rpc_connection
        response_json = await rpc_connection.getrawmempool()
        return response_json
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)    


# @router.get('/getblockdeltas/{blockhash}', tags=["Blockchain Methods"])
# async def getblockdeltas(blockhash):
#     try:
#         global rpc_connection
#         response_json = await rpc_connection.getblockdeltas([blockhash])
#         return response_json
#     except ValidationError as ve:
#         return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
#     except Exception as x:
#         return fastapi.Response(content=str(x), status_code=500)


# @router.get('/getaddressmempool/{address}', tags=["Blockchain Methods"])
# async def getaddressmempool(address):
#     try:
#         global rpc_connection
#         response_json = await rpc_connection.getaddressmempool([address])
#         return response_json
#     except ValidationError as ve:
#         return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
#     except Exception as x:
#         return fastapi.Response(content=str(x), status_code=500)
    

@router.get('/getblock/{blockhash}', tags=["Blockchain Methods"])
async def getblock(blockhash: str):
    try:
        global rpc_connection
        response_json = await rpc_connection.getblock(blockhash)
        return response_json
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)


@router.get('/getblockheader/{blockhash}', tags=["Blockchain Methods"])
async def getblockheader(blockhash: str):
    try:
        global rpc_connection
        response_json = await rpc_connection.getblockheader(blockhash)
        return response_json
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)


@router.get('/getblockhash/{index}', tags=["Blockchain Methods"])
async def getblock(index: int):
    try:
        global rpc_connection
        response_json = await rpc_connection.getblockhash(index)
        return response_json
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)


@router.get('/gettxout/{txid}/{vout_value}', tags=["Blockchain Methods"])
async def gettxout(txid: str, vout_value: int, includemempool: bool = True):
    try:
        global rpc_connection
        response_json = await rpc_connection.gettxout(txid, vout_value, includemempool)
        return response_json
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)


@router.get('/gettxoutproof/{txid}', tags=["Blockchain Methods"])
async def gettxout(txid: str):
    try:
        global rpc_connection
        response_json = await rpc_connection.gettxoutproof([txid])
        return response_json
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)


@router.get('/verifytxoutproof/{proof_string}', tags=["Blockchain Methods"])
async def verifytxoutproof(proof_string: str):
    try:
        global rpc_connection
        response_json = await rpc_connection.verifytxoutproof(proof_string)
        return response_json
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)


@router.get('/getinfo', tags=["Control Methods"])
async def getinfo():
    try:
        global rpc_connection
        response_json = await rpc_connection.getinfo()
        return response_json
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)
    
    
@router.get('/getmemoryinfo', tags=["Control Methods"])
async def getmemoryinfo():
    try:
        global rpc_connection
        response_json = await rpc_connection.getmemoryinfo()
        return response_json
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)
    
    
@router.get('/getblocksubsidy/{block_height}', tags=["Mining Methods"])
async def getblock(block_height: int):
    try:
        global rpc_connection
        response_json = await rpc_connection.getblocksubsidy(block_height)
        return response_json
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)


@router.get('/getblocktemplate', tags=["Mining Methods"])
async def getblocktemplate():
    try:
        global rpc_connection
        response_json = await rpc_connection.getblocktemplate()
        return response_json
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)


@router.get('/getmininginfo', tags=["Mining Methods"])
async def getmininginfo():
    try:
        global rpc_connection
        response_json = await rpc_connection.getmininginfo()
        return response_json
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)


@router.get('/getnextblocksubsidy', tags=["Mining Methods"])
async def getnextblocksubsidy():
    try:
        global rpc_connection
        response_json = await rpc_connection.getnextblocksubsidy()
        return response_json
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)
                

@router.get('/getnetworksolps/{block_height}/{window_size_in_blocks}', tags=["Mining Methods"])
async def getblock(block_height: int, window_size_in_blocks: int):
    try:
        global rpc_connection
        response_json = await rpc_connection.getnetworksolps(window_size_in_blocks, block_height)
        return response_json
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)


@router.get('/getaddednodeinfo', tags=["Network Methods"])
async def getaddednodeinfo():
    try:
        global rpc_connection
        response_json = await rpc_connection.getaddednodeinfo(True)
        return response_json
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)
    

@router.get('/getnetworkinfo', tags=["Network Methods"])
async def getnetworkinfo():
    try:
        global rpc_connection
        response_json = await rpc_connection.getnetworkinfo()
        return response_json
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)
    

@router.get('/getpeerinfo', tags=["Network Methods"])
async def getpeerinfo():
    try:
        global rpc_connection
        response_json = await rpc_connection.getpeerinfo()
        return response_json
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)
        

@router.get('/getfeeschedule', tags=["Supernode Methods"])
async def getfeeschedule():
    try:
        global rpc_connection
        response_json = await rpc_connection.getfeeschedule()
        return response_json
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)


@router.get('/masternode/{command_string}', tags=["Supernode Methods"])
async def masternode(command_string: str):
    try:
        global rpc_connection
        response_json = await rpc_connection.masternode(command_string)
        return response_json
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)


@router.get('/getrawtransaction/{txid}', tags=["Raw Transaction Methods"])
async def getrawtransaction(txid: str):
    try:
        global rpc_connection
        response_json = await rpc_connection.getrawtransaction(txid)
        return response_json
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)
    

@router.get('/decoderawtransaction/{hexstring}', tags=["Raw Transaction Methods"])
async def decoderawtransaction(hexstring: str):
    try:
        global rpc_connection
        response_json = await rpc_connection.decoderawtransaction(hexstring)
        return response_json
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)


@router.get('/decodescript/{hexstring}', tags=["Raw Transaction Methods"])
async def decodescript(hexstring: str):
    try:
        global rpc_connection
        response_json = await rpc_connection.decodescript(hexstring)
        return response_json
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)


@router.get('/estimatefee/{nblocks}', tags=["Utility Methods"])
async def estimatefee(nblocks: int):
    try:
        global rpc_connection
        response_json = await rpc_connection.estimatefee(nblocks)
        return response_json
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)


@router.get('/validateaddress/{transparent_psl_address}', tags=["Utility Methods"])
async def validateaddress(transparent_psl_address: str):
    try:
        global rpc_connection
        response_json = await rpc_connection.validateaddress(transparent_psl_address)
        return response_json
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)


@router.get('/z_validateaddress/{shielded_psl_address}', tags=["Utility Methods"])
async def z_validateaddress(shielded_psl_address: str):
    try:
        global rpc_connection
        response_json = await rpc_connection.z_validateaddress(shielded_psl_address)
        return response_json
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)
                        
    
@router.get('/list_all_openapi_tickets/', tags=["Ticket Methods"])
@router.get('/list_all_openapi_tickets/{min_block_height}', tags=["Ticket Methods"])
async def list_all_openapi_tickets(min_block_height: Optional[str] = '0'):
    try:
        global rpc_connection
        response_json = await rpc_connection.tickets('list', 'action', 'all', min_block_height)
        return response_json
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)    

                        
@router.get('/list_only_activated_openapi_tickets/', tags=["Ticket Methods"])
@router.get('/list_only_activated_openapi_tickets/{min_block_height}', tags=["Ticket Methods"])
async def list_only_activated_openapi_tickets(min_block_height: Optional[str] = '0'):
    try:
        global rpc_connection
        response_json = await rpc_connection.tickets('list', 'action', 'active', min_block_height)
        return response_json
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)    
    
    
@router.get('/list_only_inactive_openapi_tickets/', tags=["Ticket Methods"])
@router.get('/list_only_inactive_openapi_tickets/{min_block_height}', tags=["Ticket Methods"])
async def list_only_inactive_openapi_tickets(min_block_height: Optional[str] = '0'):
    try:
        global rpc_connection
        response_json = await rpc_connection.tickets('list', 'action', 'inactive', min_block_height)
        return response_json
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)    
    
    
@router.get('/list_tickets_by_type/{ticket_type}', tags=["Ticket Methods"])
@router.get('/list_tickets_by_type/{ticket_type}/{min_block_height}', tags=["Ticket Methods"])
async def list_tickets_by_type(ticket_type: str, min_block_height: Optional[str] = '0'):
#Possible values for ticket_type: 'id', 'nft', 'offer', 'accept', 'transfer', 'collection', 'collection-act', 'royalty', 'username', 'ethereumaddress', 'action', 'action-act'
    try:
        global rpc_connection
        response_json = await rpc_connection.tickets('list', str(ticket_type), 'all', str(min_block_height))
        return response_json
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)


@router.get('/list_active_tickets_by_type/{ticket_type}', tags=["Ticket Methods"])
@router.get('/list_active_tickets_by_type/{ticket_type}/{min_block_height}', tags=["Ticket Methods"])
async def list_active_tickets_by_type(ticket_type: str, min_block_height: Optional[str] = '0'):
    try:
        global rpc_connection
        response_json = await rpc_connection.tickets('list', str(ticket_type), 'active', str(min_block_height))
        return response_json
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)
                            

@router.get('/list_inactive_tickets_by_type/{ticket_type}', tags=["Ticket Methods"])
@router.get('/list_inactive_tickets_by_type/{ticket_type}/{min_block_height}', tags=["Ticket Methods"])
async def list_inactive_tickets_by_type(ticket_type: str, min_block_height: Optional[str] = '0'):
    try:
        global rpc_connection
        response_json = await rpc_connection.tickets('list', str(ticket_type), 'inactive', str(min_block_height))
        return response_json
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)


@router.get('/find_action_ticket_by_pastelid/{pastelid}', tags=["Ticket Methods"])
async def find_action_ticket_by_pastelid(pastelid: str):
    try:
        global rpc_connection
        response_json = await rpc_connection.tickets('find', 'action', pastelid)
        return response_json
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)


@router.get('/find_action_activation_ticket_by_pastelid/{pastelid}', tags=["Ticket Methods"])
async def find_action_ticket_by_pastelid(pastelid: str):
    try:
        global rpc_connection
        response_json = await rpc_connection.tickets('find', 'action-act', pastelid)
        return response_json
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)
        

@router.get('/get_ticket_by_txid/{txid}', tags=["Ticket Methods"])
async def get_ticket_by_txid(txid: str):
    try:
        response_json = await get_pastel_blockchain_ticket_func(txid)
        return response_json
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)


@router.get('/get_all_ticket_data', tags=["High-Level Methods"])
async def get_all_ticket_data() -> str:
    try:
        response_json = await get_all_pastel_blockchain_tickets_func()
        return str(response_json)
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)
    
    
@router.get('/supernode_data', tags=["High-Level Methods"])
async def supernode_data() -> str:
    try:
        response_json = await check_supernode_list_func()
        return response_json
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)
    
    
@router.get('/get_network_storage_fees', tags=["High-Level Methods"])
async def get_network_storage_fees() -> str:
    try:
        response_json = await get_network_storage_fees_func()
        return response_json
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)
    
    
@router.get('/verify_message_with_pastelid/{pastelid_pubkey}/{message_to_verify}/{pastelid_signature_on_message}', tags=["High-Level Methods"])
async def verify_message_with_pastelid(pastelid_pubkey: str, message_to_verify: str, pastelid_signature_on_message: str) -> str:
    try:
        response_json = await verify_message_with_pastelid_func(pastelid_pubkey, message_to_verify, pastelid_signature_on_message)
        return response_json
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)


#Endpoint to respond with a PastelID file, given a desired password:
@router.get('/testnet_pastelid_file_dispenser/{desired_password}', tags=["High-Level Methods"])
async def testnet_pastelid_file_dispenser(desired_password: str):
    try:
        pastelid_pubkey, pastelid_data = await testnet_pastelid_file_dispenser_func(desired_password)
        #send pastelid_data to user with StreamResponse, with the filename being the pastelid_pubkey:
        response = StreamingResponse([pastelid_data], media_type="application/binary")
        response.headers["Content-Disposition"] = f"attachment; filename=ticket.{pastelid_pubkey}"
        return response
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)
    

@router.get('/get_parsed_dd_service_results_by_registration_ticket_txid/{txid}', tags=["OpenAPI Methods"])
async def get_parsed_dd_service_results_by_registration_ticket_txid(txid: str):
    try:
        parsed_dd_service_data, is_cached_response = await get_parsed_dd_service_results_by_registration_ticket_txid_func(txid)
        return parsed_dd_service_data
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)


@router.get('/get_raw_dd_service_results_by_registration_ticket_txid/{txid}', tags=["OpenAPI Methods"])
async def get_raw_dd_service_results_by_registration_ticket_txid(txid: str):
    try:
        raw_dd_service_data, is_cached_response = await get_raw_dd_service_results_by_registration_ticket_txid_func(txid)
        return raw_dd_service_data
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)

    
@router.get('/get_publicly_accessible_cascade_file_by_registration_ticket_txid/{txid}', tags=["OpenAPI Methods"])
async def get_publicly_accessible_cascade_file_by_registration_ticket_txid(txid: str):
    try:
        decoded_response, original_file_name_string = await download_publicly_accessible_cascade_file_by_registration_ticket_txid_func(txid)
        content_disposition_string = f"attachment; filename={original_file_name_string}"
        return StreamingResponse(io.BytesIO(decoded_response), media_type="application/octet-stream", headers={"Content-Disposition": content_disposition_string})
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)
    

@router.get('/get_parsed_dd_service_results_by_image_file_hash/{image_file_hash}', tags=["OpenAPI Methods"])
async def get_parsed_dd_service_results_by_image_file_hash(image_file_hash: str):
    try:
        parsed_dd_service_data = await get_parsed_dd_service_results_by_image_file_hash_func(image_file_hash)
        return parsed_dd_service_data
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)
   

@router.get('/get_raw_dd_service_results_by_image_file_hash/{image_file_hash}', tags=["OpenAPI Methods"])
async def get_raw_dd_service_results_by_image_file_hash(image_file_hash: str):
    try:
        raw_dd_service_data = await get_raw_dd_service_results_by_image_file_hash_func(image_file_hash)
        return raw_sense_data
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)


@router.get('/get_parsed_dd_service_results_by_pastel_id_of_submitter/{pastel_id_of_submitter}', tags=["OpenAPI Methods"])
async def get_parsed_dd_service_results_by_pastel_id_of_submitter(pastel_id_of_submitter: str):
    try:
        parsed_dd_service_data = await get_parsed_dd_service_results_by_pastel_id_of_submitter_func(pastel_id_of_submitter)
        return parsed_dd_service_data
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)


@router.get('/get_raw_dd_service_results_by_pastel_id_of_submitter/{pastel_id_of_submitter}', tags=["OpenAPI Methods"])
async def get_raw_dd_service_results_by_pastel_id_of_submitter(pastel_id_of_submitter: str):
    try:
        raw_dd_service_data = await get_raw_dd_service_results_by_pastel_id_of_submitter_func(pastel_id_of_submitter)
        return raw_dd_service_data
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)
    

@router.get('/get_parsed_dd_service_results_by_pastel_block_hash_when_request_submitted/{pastel_block_hash_when_request_submitted}', tags=["OpenAPI Methods"])
async def get_parsed_dd_service_results_by_pastel_block_hash_when_request_submitted(pastel_block_hash_when_request_submitted: str):
    try:
        parsed_dd_service_data = await get_parsed_dd_service_results_by_pastel_block_hash_when_request_submitted_func(pastel_block_hash_when_request_submitted)
        return parsed_dd_service_data
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)


@router.get('/get_raw_dd_service_results_by_pastel_block_hash_when_request_submitted/{pastel_block_hash_when_request_submitted}', tags=["OpenAPI Methods"])
async def get_raw_dd_service_results_by_pastel_block_hash_when_request_submitted(pastel_block_hash_when_request_submitted: str):
    try:
        raw_dd_service_data = await get_raw_dd_service_results_by_pastel_block_hash_when_request_submitted_func(pastel_block_hash_when_request_submitted)
        return raw_dd_service_data
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)
    

@router.get('/get_parsed_dd_service_results_by_pastel_block_height_when_request_submitted/{pastel_block_height_when_request_submitted}', tags=["OpenAPI Methods"])
async def get_parsed_dd_service_results_by_pastel_block_height_when_request_submitted(pastel_block_height_when_request_submitted: str):
    try:
        parsed_dd_service_data = await get_parsed_dd_service_results_by_pastel_block_height_when_request_submitted_func(pastel_block_height_when_request_submitted)
        return parsed_dd_service_data
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)


@router.get('/get_raw_dd_service_results_by_pastel_block_height_when_request_submitted/{pastel_block_height_when_request_submitted}', tags=["OpenAPI Methods"])
async def get_raw_dd_service_results_by_pastel_block_height_when_request_submitted(pastel_block_height_when_request_submitted: str):
    try:
        raw_dd_service_data = await get_raw_dd_service_results_by_pastel_block_height_when_request_submitted_func(pastel_block_height_when_request_submitted)
        return raw_dd_service_data
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)
    

@router.get('/get_current_total_number_of_registered_sense_fingerprints/', tags=["OpenAPI Methods"])
async def get_current_total_number_of_registered_sense_fingerprints():
    try:
        fingerprint_counter = await get_current_total_number_of_registered_sense_fingerprints_func()
        return fingerprint_counter
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500) 


@router.get('/get_current_total_number_and_size_and_average_size_of_registered_cascade_files/', tags=["OpenAPI Methods"])
async def get_current_total_number_and_size_and_average_size_of_registered_cascade_files():
    try:
        response = await get_current_total_number_and_size_and_average_size_of_registered_cascade_files_func()
        return response
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500) 

    
@router.get('/get_usernames_from_pastelid/{pastelid}', tags=["OpenAPI Methods"])
async def get_usernames_from_pastelid(pastelid : str):
    try:
        response = await get_usernames_from_pastelid_func(pastelid)
        return JSONResponse(content={"pastelid_query": pastelid, "matching_usernames": response})
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)     


@router.get('/get_all_registration_ticket_txids_corresponding_to_a_collection_ticket_txid/{collection_ticket_txid}', tags=["OpenAPI Methods"])
async def get_all_registration_ticket_txids_corresponding_to_a_collection_ticket_txid(collection_ticket_txid : str):
    try:
        response = await get_all_registration_ticket_txids_corresponding_to_a_collection_ticket_txid_func(collection_ticket_txid)
        return response
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)     


@router.get('/get_pastelid_from_username/{username}', tags=["OpenAPI Methods"])
async def get_pastelid_from_username(username : str):
    try:
        response = await get_pastelid_from_username_func(username)
        return JSONResponse(content={"username_query": username, "matching_pastelid": response})
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)     

    
@router.get('/populate_database_with_all_dd_service_data', tags=["OpenAPI Methods"])
async def populate_database_with_all_dd_service_data(background_tasks: BackgroundTasks):
    try:
        background_tasks.add_task(populate_database_with_all_dd_service_data_func)
        return 'Started background task to populate database with all sense data...'
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)


@router.get('/run_bulk_test_cascade/{num_downloads}', tags=["OpenAPI Methods"])
async def run_bulk_test_cascade(num_downloads: int = Query(5, description="Number of concurrent Cascade downloads to launch for test. Default is 5.")):
    try:
        combined_output_dict = await bulk_test_cascade_func(num_downloads)
        return combined_output_dict
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)


@router.get("/show_logs_incremental/{minutes}/{last_position}", response_model=ShowLogsIncrementalModel)
def show_logs_incremental(minutes: int, last_position: int):
    new_logs = []
    now = datetime.now(timezone('UTC'))  # get current time, make it timezone-aware
    try:
        with open("opennode_fastapi_log.txt", "r") as f:
            f.seek(last_position)  # seek to `last_position`
            while True:
                line = f.readline()
                if line == "":  # if EOF
                    break
                if line.strip() == "":
                    continue
                try:  # Try to parse the datetime
                    log_datetime_str = line.split(" - ")[0]  # assuming the datetime is at the start of each line
                    log_datetime = datetime.strptime(log_datetime_str, "%Y-%m-%d %H:%M:%S,%f")  # parse the datetime string to a datetime object
                    log_datetime = log_datetime.replace(tzinfo=timezone('UTC'))  # set the datetime object timezone to UTC to match `now`
                    if now - log_datetime > timedelta(minutes=minutes):  # if the log is older than `minutes` minutes from now
                        continue  # ignore the log and continue with the next line
                except ValueError:
                    pass  # If the line does not start with a datetime, ignore the ValueError and process the line anyway
                new_logs.append(highlight_rules_func(line.rstrip('\n')))  # add the highlighted log to the list and strip any newline at the end
            last_position = f.tell()  # get the last position
        new_logs_as_string = "<br>".join(new_logs)  # joining with <br> directly
    except FileNotFoundError:
        new_logs_as_string = ""
        last_position = 0
    return {"logs": new_logs_as_string, "last_position": last_position}  # also return the last position


@router.get("/show_logs/{minutes}", response_class=HTMLResponse)
def show_logs(minutes: int = 5):
    # read the entire log file and generate HTML with logs up to `minutes` minutes from now
    with open("opennode_fastapi_log.txt", "r") as f:
        lines = f.readlines()
    logs = []
    now = datetime.now(timezone('UTC'))  # get current time, make it timezone-aware
    for line in lines:
        if line.strip() == "":
            continue
        if line[0].isdigit():
            try:  # Try to parse the datetime
                log_datetime_str = line.split(" - ")[0]  # assuming the datetime is at the start of each line
                log_datetime = datetime.strptime(log_datetime_str, "%Y-%m-%d %H:%M:%S,%f")  # parse the datetime string to a datetime object
                log_datetime = log_datetime.replace(tzinfo=timezone('UTC'))  # set the datetime object timezone to UTC to match `now`
                if now - log_datetime <= timedelta(minutes=minutes):  # if the log is within `minutes` minutes from now
                    continue  # ignore the log and continue with the next line
            except ValueError:
                pass  # If the line does not start with a datetime, ignore the ValueError and process the line anyway                        
            logs.append(highlight_rules_func(line.rstrip('\n')))  # add the highlighted log to the list and strip any newline at the end
    logs_as_string = "<br>".join(logs)  # joining with <br> directly
    logs_as_string_newlines_rendered = logs_as_string.replace("\n", "<br>")
    logs_as_string_newlines_rendered_font_specified = """
    <html>
    <head>
    <link href="https://fonts.googleapis.com/css2?family=Fira+Code:wght@400;500&display=swap" rel="stylesheet">
    <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0-beta3/css/all.min.css" rel="stylesheet">
    <script>
    var logContainer;
    var lastLogs = `{0}`;
    var shouldScroll = true;
    var userScroll = false;
    var lastPosition = 0;
    var minutes = {1};
    function fetchLogs() {{
        if (typeof minutes !== 'undefined' && typeof lastPosition !== 'undefined') {{
            fetch('/show_logs_incremental/' + minutes + '/' + lastPosition)
            .then(response => response.json())
            .then(data => {{
                if (logContainer) {{
                    var div = document.createElement('div');
                    div.innerHTML = data.logs;
                    if (div.innerHTML) {{
                        lastLogs += div.innerHTML;
                        lastPosition = data.last_position;
                    }}
                    logContainer.innerHTML = lastLogs;
                    if (shouldScroll) {{
                        logContainer.scrollTop = logContainer.scrollHeight;
                    }}
                }}
            }});
        }}
    }}
    function checkScroll() {{
        if(logContainer.scrollTop + logContainer.clientHeight < logContainer.scrollHeight) {{
            userScroll = true;
            shouldScroll = false;
        }} else {{
            userScroll = false;
        }}
        if (!userScroll) {{
            // If the user is not manually scrolling, re-enable auto-scrolling after a delay
            setTimeout(function(){{ shouldScroll = true; }}, 10000);
        }}
    }}
    window.onload = function() {{
        let p = document.getElementsByTagName('p');
        for(let i = 0; i < p.length; i++) {{
            let color = window.getComputedStyle(p[i]).getPropertyValue('color');
            p[i].style.textShadow = `0 0 5px ${{color}}, 0 0 10px ${{color}}, 0 0 15px ${{color}}, 0 0 20px ${{color}}`;
        }}
        document.querySelector('#copy-button').addEventListener('click', function() {{
            var text = document.querySelector('#log-container').innerText;
            navigator.clipboard.writeText(text).then(function() {{
                console.log('Copying to clipboard was successful!');
            }}, function(err) {{
                console.error('Could not copy text: ', err);
            }});
        }});
        document.querySelector('#download-button').addEventListener('click', function() {{
            var text = document.querySelector('#log-container').innerText;
            var element = document.createElement('a');
            element.setAttribute('href', 'data:text/plain;charset=utf-8,' + encodeURIComponent(text));
            element.setAttribute('download', 'pastel_gateway_verification_monitor_log__' + new Date().toISOString() + '.txt');
            element.style.display = 'none';
            document.body.appendChild(element);
            element.click();
            document.body.removeChild(element);
        }});
    }}
    document.addEventListener('DOMContentLoaded', (event) => {{
        logContainer = document.getElementById('log-container');
        logContainer.innerHTML = lastLogs;
        logContainer.addEventListener('scroll', checkScroll);
        fetchLogs();
        setInterval(fetchLogs, 1000);  // Fetch the logs every 1 second
    }});
    </script>
    </head>        
    <style>
    .log-container {{
        scroll-behavior: smooth;
        background-color: #2b2b2b; /* dark gray */
        color: #d3d3d3; /* light gray */
        background-image: linear-gradient(rgba(0,0,0,0.1) 1px, transparent 1px), linear-gradient(90deg, rgba(0,0,0,0.1) 1px, transparent 1px);
        background-size: 100% 10px, 10px 100%;
        background-position: 0 0, 0 0;
        animation: scan 1s linear infinite;
        @keyframes scan {{
            0% {{
                background-position: 0 0, 0 0;
            }}
            100% {{
                background-position: -10px -10px, -10px -10px;
            }}
        }}
        font-size: 14px;
        font-family: monospace;
        padding: 10px;
        height: 100vh;
        margin: 0;
        box-sizing: border-box;
        overflow: auto;
    }}
    .icon-button {{
        position: fixed;
        right: 10px;
        margin: 10px;
        background-color: #555;
        color: white;
        border: none;
        cursor: pointer;
        border-radius: 50%;
        width: 60px;
        height: 60px;
        font-size: 30px;
        display: flex;
        align-items: center;
        justify-content: center;
        text-decoration: none;
    }}
    #copy-button {{
        bottom: 80px;  // Adjust this value as needed
    }}
    #download-button {{
        bottom: 10px;
    }}
    </style>
    <body>
    <pre id="log-container" class="log-container"></pre>
    <button id="copy-button" class="icon-button"><i class="fas fa-copy"></i></button>
    <button id="download-button" class="icon-button"><i class="fas fa-download"></i></button>
    </body>
    </html>""".format(logs_as_string_newlines_rendered, minutes)
    return logs_as_string_newlines_rendered_font_specified


@router.get("/show_logs", response_class=HTMLResponse)
def show_logs_default():
    return show_logs(5)
