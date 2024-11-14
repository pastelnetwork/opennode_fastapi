# /views/opennode_fastapi.py

import services.opennode_fastapi_service as service_funcs
import io
import os
import fastapi
import json
import tempfile
import pandas as pd
import asyncio
from typing import List
from fastapi import HTTPException, BackgroundTasks, Query, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import JSONResponse, StreamingResponse, HTMLResponse, FileResponse
from data.opennode_fastapi import ValidationError, ShowLogsIncrementalModel
from json import JSONEncoder
from datetime import datetime, timedelta
from pytz import timezone
from typing import Optional, Callable, Any

router = fastapi.APIRouter()

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def send_personal_message(self, message: str, websocket: WebSocket):
        await websocket.send_text(message)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            await connection.send_text(message)

manager = ConnectionManager()

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
    {"name": "Insight Explorer Methods", "description": "Endpoints for Insight Explorer related data"},
    {"name": "Price Methods", "description": "Endpoints for retrieving PSL Market Data"},
]


async def handle_exceptions(rpc_method: Callable[..., Any], *args, **kwargs) -> Any:
    try:
        response = await rpc_method(*args, **kwargs)  # Await the coroutine
        if isinstance(response, tuple):
            return response
        return response
    except ValidationError as ve:
        raise HTTPException(status_code=ve.status_code, detail=ve.error_msg)
    except Exception as x:
        raise HTTPException(status_code=500, detail=str(x))

class DateTimeEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()

@router.get('/getbestblockhash', tags=["Blockchain Methods"])
async def getbestblockhash():
    global rpc_connection
    return await handle_exceptions(rpc_connection.getbestblockhash)

@router.get('/getblockchaininfo', tags=["Blockchain Methods"])
async def getblockchaininfo():
    global rpc_connection
    return await handle_exceptions(rpc_connection.getblockchaininfo)

@router.get('/gettxoutsetinfo', tags=["Blockchain Methods"])
async def gettxoutsetinfo():
    global rpc_connection
    return await handle_exceptions(rpc_connection.gettxoutsetinfo)

@router.get('/getblockcount', tags=["Blockchain Methods"])
async def getblockcount():
    global rpc_connection  
    return await handle_exceptions(rpc_connection.getblockcount)

@router.get('/getchaintips', tags=["Blockchain Methods"])
async def getchaintips():
    global rpc_connection
    return await handle_exceptions(rpc_connection.getchaintips)

@router.get('/getdifficulty', tags=["Blockchain Methods"])
async def getdifficulty():
    global rpc_connection
    return await handle_exceptions(rpc_connection.getdifficulty)

@router.get('/getmempoolinfo', tags=["Blockchain Methods"])
async def getmempoolinfo():
    global rpc_connection
    return await handle_exceptions(rpc_connection.getmempoolinfo)

@router.get('/getrawmempool', tags=["Blockchain Methods"])
async def getrawmempool():
    global rpc_connection
    return await handle_exceptions(rpc_connection.getrawmempool)

@router.get('/getblock/{blockhash}', tags=["Blockchain Methods"])
async def getblock(blockhash: str):
    global rpc_connection
    return await handle_exceptions(rpc_connection.getblock, blockhash)

@router.get('/getblockheader/{blockhash}', tags=["Blockchain Methods"])
async def getblockheader(blockhash: str):
    global rpc_connection
    return await handle_exceptions(rpc_connection.getblockheader, blockhash)

@router.get('/getblockhash/{index}', tags=["Blockchain Methods"])
async def getblockhash(index: int):
    global rpc_connection
    return await handle_exceptions(rpc_connection.getblockhash, index)

@router.get('/gettxout/{txid}/{vout_value}', tags=["Blockchain Methods"])
async def gettxout(txid: str, vout_value: int, includemempool: bool = True):
    global rpc_connection
    return await handle_exceptions(rpc_connection.gettxout, txid, vout_value, includemempool)

@router.get('/gettxoutproof/{txid}', tags=["Blockchain Methods"])
async def gettxoutproof(txid: str):
    global rpc_connection
    return await handle_exceptions(rpc_connection.gettxoutproof, [txid])

@router.get('/verifytxoutproof/{proof_string}', tags=["Blockchain Methods"])
async def verifytxoutproof(proof_string: str):
    global rpc_connection
    return await handle_exceptions(rpc_connection.verifytxoutproof, proof_string)

@router.get('/getinfo', tags=["Control Methods"])
async def getinfo():
    global rpc_connection
    return await handle_exceptions(rpc_connection.getinfo)

@router.get('/getmemoryinfo', tags=["Control Methods"])
async def getmemoryinfo():
    global rpc_connection
    return await handle_exceptions(rpc_connection.getmemoryinfo)

@router.get('/getblocksubsidy/{block_height}', tags=["Mining Methods"])
async def getblocksubsidy(block_height: int):
    global rpc_connection
    return await handle_exceptions(rpc_connection.getblocksubsidy, block_height)

@router.get('/getblocktemplate', tags=["Mining Methods"])
async def getblocktemplate():
    global rpc_connection
    return await handle_exceptions(rpc_connection.getblocktemplate)

@router.get('/getmininginfo', tags=["Mining Methods"])
async def getmininginfo():
    global rpc_connection
    return await handle_exceptions(rpc_connection.getmininginfo)

@router.get('/getnextblocksubsidy', tags=["Mining Methods"])
async def getnextblocksubsidy():
    global rpc_connection
    return await handle_exceptions(rpc_connection.getnextblocksubsidy)

@router.get('/getnetworksolps/{block_height}/{window_size_in_blocks}', tags=["Mining Methods"])
async def getnetworksolps(block_height: int, window_size_in_blocks: int):
    global rpc_connection
    return await handle_exceptions(rpc_connection.getnetworksolps, window_size_in_blocks, block_height)

@router.get('/getaddednodeinfo', tags=["Network Methods"])
async def getaddednodeinfo():
    global rpc_connection
    return await handle_exceptions(rpc_connection.getaddednodeinfo, True)

@router.get('/getnetworkinfo', tags=["Network Methods"])
async def getnetworkinfo():
    global rpc_connection
    return await handle_exceptions(rpc_connection.getnetworkinfo)

@router.get('/getpeerinfo', tags=["Network Methods"])
async def getpeerinfo():
    global rpc_connection
    return await handle_exceptions(rpc_connection.getpeerinfo)

@router.get('/getfeeschedule', tags=["Supernode Methods"])
async def getfeeschedule():
    global rpc_connection
    return await handle_exceptions(rpc_connection.getfeeschedule)

@router.get('/masternode/{command_string}', tags=["Supernode Methods"])
async def masternode(command_string: str):
    global rpc_connection
    return await handle_exceptions(rpc_connection.masternode, command_string)
@router.get('/getrawtransaction/{txid}', tags=["Raw Transaction Methods"])
async def getrawtransaction(txid: str):
    """
    Get raw transaction data by transaction ID.
    Includes full transaction details with proper value decoding.
    
    Args:
        txid (str): The transaction ID
        
    Returns:
        dict: Complete transaction data with decoded values
    """
    try:
        # Get raw transaction with verbose=1 for full decoded data
        tx_data = await handle_exceptions(rpc_connection.getrawtransaction, txid, 1)
        
        # Process vout values if present
        if 'vout' in tx_data:
            for output in tx_data['vout']:
                if 'valuePat' in output:
                    # Convert patoshis to PSL
                    output['value'] = float(output['valuePat']) / 100000000
                    
        return tx_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving transaction: {str(e)}")

@router.get('/decoderawtransaction/{hexstring}', tags=["Raw Transaction Methods"])
async def decoderawtransaction(hexstring: str):
    """
    Decode a raw transaction hex string.
    Ensures proper value decoding for all transaction outputs.
    
    Args:
        hexstring (str): The transaction hex string to decode
        
    Returns:
        dict: Decoded transaction data with proper values
    """
    try:
        decoded = await handle_exceptions(rpc_connection.decoderawtransaction, hexstring)
        
        # Process vout values
        if 'vout' in decoded:
            for output in decoded['vout']:
                if 'valuePat' in output:
                    # Convert patoshis to PSL
                    output['value'] = float(output['valuePat']) / 100000000
                    
        return decoded
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error decoding transaction: {str(e)}")

@router.post('/sendrawtransaction', tags=["Raw Transaction Methods"])
async def send_raw_transaction(
    hex_string: Optional[str] = None,
    allow_high_fees: bool = False,
    request: Request = None
):
    """
    Submit raw transaction (serialized, hex-encoded) to local node and network.
    Accepts hex_string either as query parameter or in request body.
    """
    try:
        # Try to get hex_string from query params first, then body
        if not hex_string and request:
            body = await request.json()
            hex_string = body.get('hex_string')
        
        if not hex_string:
            raise HTTPException(status_code=400, detail="hex_string is required")

        # First decode the transaction to verify structure and amounts
        decoded = await handle_exceptions(rpc_connection.decoderawtransaction, hex_string)
        
        # Submit transaction
        txid = await handle_exceptions(rpc_connection.sendrawtransaction, hex_string, allow_high_fees)
        
        return {
            "txid": txid,
            "decoded_tx": decoded,
            "message": "Transaction broadcast to network",
            "status": "success",
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get('/gettransactionconfirmations/{txid}', tags=["Raw Transaction Methods"])
async def get_transaction_confirmations(txid: str):
    """
    Get detailed confirmation status for a transaction.
    
    Args:
        txid (str): The transaction ID to check
        
    Returns:
        dict: Detailed confirmation status
    """
    try:
        # Get full transaction data
        tx_data = await handle_exceptions(rpc_connection.getrawtransaction, txid, 1)
        
        confirmations = tx_data.get('confirmations', 0)
        block_hash = tx_data.get('blockhash', None)
        block_time = tx_data.get('blocktime', None)
        
        return {
            "txid": txid,
            "confirmations": confirmations,
            "confirmed": confirmations > 0,
            "block_hash": block_hash,
            "block_time": datetime.fromtimestamp(block_time).isoformat() if block_time else None,
            "in_mempool": confirmations == 0 and tx_data.get('time', 0) > 0,
            "status": "confirmed" if confirmations > 0 else "pending",
            "check_time": datetime.utcnow().isoformat()
        }
    except Exception as e:
        # If transaction not found, it's not in mempool or blockchain
        if "No information" in str(e):
            return {
                "txid": txid,
                "confirmations": 0,
                "confirmed": False,
                "status": "not_found",
                "check_time": datetime.utcnow().isoformat()
            }
        raise HTTPException(status_code=500, detail=f"Error checking confirmation status: {str(e)}")

@router.post('/createrawtransaction', tags=["Raw Transaction Methods"])
async def create_raw_transaction(
    inputs: List[dict], 
    outputs: dict,
    locktime: Optional[int] = 0,
    expiry_height: Optional[int] = None
):
    """
    Create a raw transaction with enhanced validation and proper amount handling.
    
    Args:
        inputs (List[dict]): Transaction inputs (txid, vout, sequence)
        outputs (dict): Transaction outputs (address: amount)
        locktime (int): Transaction locktime
        expiry_height (int): Transaction expiry height
        
    Returns:
        dict: Created transaction data
    """
    try:
        # Validate inputs
        for inp in inputs:
            if not all(k in inp for k in ['txid', 'vout']):
                raise HTTPException(status_code=400, detail="Invalid input format")
            if len(inp['txid']) != 64:
                raise HTTPException(status_code=400, detail="Invalid txid length")

        # Validate and process outputs
        processed_outputs = {}
        for address, amount in outputs.items():
            if not isinstance(amount, (int, float, str)):
                raise HTTPException(status_code=400, detail=f"Invalid amount for address {address}")
            try:
                processed_outputs[address] = float(amount)
            except ValueError:
                raise HTTPException(status_code=400, detail=f"Invalid amount format for address {address}")

        # Create transaction
        hex_string = await handle_exceptions(
            service_funcs.create_raw_transaction_func,
            inputs,
            processed_outputs,
            locktime,
            expiry_height
        )

        # Decode the created transaction for verification
        decoded = await handle_exceptions(rpc_connection.decoderawtransaction, hex_string)
        
        return {
            "hex": hex_string,
            "decoded": decoded,
            "locktime": locktime,
            "expiry_height": expiry_height,
            "created_at": datetime.utcnow().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating transaction: {str(e)}")

@router.get('/decodescript/{hexstring}', tags=["Raw Transaction Methods"])
async def decodescript(hexstring: str):
    """
    Decode a script hex string.
    
    Args:
        hexstring (str): The script hex string to decode
        
    Returns:
        dict: Decoded script data
    """
    try:
        return await handle_exceptions(rpc_connection.decodescript, hexstring)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error decoding script: {str(e)}")

@router.get('/estimatefee/{nblocks}', tags=["Utility Methods"])
async def estimatefee(nblocks: int):
    global rpc_connection
    return await handle_exceptions(rpc_connection.estimatefee, nblocks)

@router.get('/validateaddress/{transparent_psl_address}', tags=["Utility Methods"])
async def validateaddress(transparent_psl_address: str):
    global rpc_connection
    return await handle_exceptions(rpc_connection.validateaddress, transparent_psl_address)

@router.get('/z_validateaddress/{shielded_psl_address}', tags=["Utility Methods"])
async def z_validateaddress(shielded_psl_address: str):
    global rpc_connection
    return await handle_exceptions(rpc_connection.z_validateaddress, shielded_psl_address)

# Ticket Methods
@router.get('/list_all_openapi_tickets/', tags=["Ticket Methods"])
@router.get('/list_all_openapi_tickets/{min_block_height}', tags=["Ticket Methods"])
async def list_all_openapi_tickets(min_block_height: Optional[str] = '0'):
    global rpc_connection
    return await handle_exceptions(rpc_connection.tickets, 'list', 'action', 'all', min_block_height)

@router.get('/list_only_activated_openapi_tickets/', tags=["Ticket Methods"])
@router.get('/list_only_activated_openapi_tickets/{min_block_height}', tags=["Ticket Methods"])
async def list_only_activated_openapi_tickets(min_block_height: Optional[str] = '0'):
    global rpc_connection
    return await handle_exceptions(rpc_connection.tickets, 'list', 'action', 'active', min_block_height)

@router.get('/list_only_inactive_openapi_tickets/', tags=["Ticket Methods"])
@router.get('/list_only_inactive_openapi_tickets/{min_block_height}', tags=["Ticket Methods"])
async def list_only_inactive_openapi_tickets(min_block_height: Optional[str] = '0'):
    global rpc_connection
    return await handle_exceptions(rpc_connection.tickets, 'list', 'action', 'inactive', min_block_height)

@router.get('/list_tickets_by_type/{ticket_type}', tags=["Ticket Methods"])
@router.get('/list_tickets_by_type/{ticket_type}/{min_block_height}', tags=["Ticket Methods"])
async def list_tickets_by_type(ticket_type: str, min_block_height: Optional[str] = '0'):
    global rpc_connection
    return await handle_exceptions(rpc_connection.tickets, 'list', ticket_type, 'all', min_block_height)

@router.get('/list_active_tickets_by_type/{ticket_type}', tags=["Ticket Methods"])
@router.get('/list_active_tickets_by_type/{ticket_type}/{min_block_height}', tags=["Ticket Methods"])
async def list_active_tickets_by_type(ticket_type: str, min_block_height: Optional[str] = '0'):
    global rpc_connection
    return await handle_exceptions(rpc_connection.tickets, 'list', ticket_type, 'active', min_block_height)

@router.get('/list_inactive_tickets_by_type/{ticket_type}', tags=["Ticket Methods"])
@router.get('/list_inactive_tickets_by_type/{ticket_type}/{min_block_height}', tags=["Ticket Methods"])
async def list_inactive_tickets_by_type(ticket_type: str, min_block_height: Optional[str] = '0'):
    global rpc_connection
    return await handle_exceptions(rpc_connection.tickets, 'list', ticket_type, 'inactive', min_block_height)

@router.get('/find_action_ticket_by_pastelid/{pastelid}', tags=["Ticket Methods"])
async def find_action_ticket_by_pastelid(pastelid: str):
    global rpc_connection
    return await handle_exceptions(rpc_connection.tickets, 'find', 'action', pastelid)

@router.get('/find_action_activation_ticket_by_pastelid/{pastelid}', tags=["Ticket Methods"])
async def find_action_activation_ticket_by_pastelid(pastelid: str):
    global rpc_connection
    return await handle_exceptions(rpc_connection.tickets, 'find', 'action-act', pastelid)

@router.get('/get_ticket_by_txid/{txid}', tags=["Ticket Methods"])
async def get_ticket_by_txid(txid: str):
    return await handle_exceptions(service_funcs.get_pastel_blockchain_ticket_func, txid)

@router.get('/tickets/contract/list/{ticket_type_identifier}/{starting_block_height}', tags=["Ticket Methods"])
async def list_contract_tickets(ticket_type_identifier: str, starting_block_height: int = 0):
    return await handle_exceptions(service_funcs.list_contract_tickets_func, ticket_type_identifier, starting_block_height)

@router.get('/tickets/contract/find/{key}', tags=["Ticket Methods"])
async def find_contract_ticket(key: str):
    return await handle_exceptions(service_funcs.find_contract_ticket_func, key)

@router.get('/tickets/contract/get/{txid}', tags=["Ticket Methods"])
async def get_contract_ticket(txid: str, decode_properties: bool = True):
    return await handle_exceptions(service_funcs.get_contract_ticket_func, txid, decode_properties)

@router.get('/tickets/id/is_registered/{pastel_id}', tags=["Ticket Methods"])
async def is_pastel_id_registered(pastel_id: str):
    return await handle_exceptions(service_funcs.is_pastel_id_registered_func, pastel_id)

@router.get('/tickets/id/list/{filter}/{minheight}', tags=["Ticket Methods"])
async def list_pastel_id_tickets(filter: str = "mine", minheight: int = None):
    return await handle_exceptions(service_funcs.list_pastel_id_tickets_func, filter, minheight)

# Endpoint for detailed_logs from storage challenges metrics with default count
@router.get('/get_storage_challenges_metrics/detailed_logs/{count}', tags=["Supernode Methods"])
async def get_storage_challenges_metrics_detailed_logs(count: int = 25):  # Default count set to 25
    return await handle_exceptions(service_funcs.get_storage_challenges_metrics_func, 'detailed_log', count)

# Endpoint for summary_stats from storage challenges metrics
@router.get('/get_storage_challenges_metrics/summary_stats', tags=["Supernode Methods"])
async def get_storage_challenges_metrics_summary_stats():
    return await handle_exceptions(service_funcs.get_storage_challenges_metrics_func, 'summary_stats')

# Endpoint for detailed_logs from self-healing metrics, with results count
@router.get('/get_self_healing_metrics/detailed_logs/{count}', tags=["Supernode Methods"])
async def get_self_healing_metrics_detailed_logs(count: int = 25): # Default count set to 25
    return await handle_exceptions(service_funcs.get_self_healing_metrics_func, 'detailed_log', count)

# Endpoint for summary_stats from self-healing metrics
@router.get('/get_self_healing_metrics/summary_stats', tags=["Supernode Methods"])
async def get_self_healing_metrics_summary_stats():
    return await handle_exceptions(service_funcs.get_self_healing_metrics_func, 'summary_stats')

# High-Level Methods
@router.get('/get_all_ticket_data', tags=["High-Level Methods"])
async def get_all_ticket_data():
    return await handle_exceptions(service_funcs.get_all_pastel_blockchain_tickets_func)

@router.get('/supernode_data', tags=["High-Level Methods"])
async def supernode_data():
    return await handle_exceptions(service_funcs.check_supernode_list_func)

@router.get('/supernode_data_csv', tags=["High-Level Methods"])
async def supernode_data_csv():
    supernode_data_json = await handle_exceptions(service_funcs.check_supernode_list_func)
    supernode_data_dict = json.loads(supernode_data_json)
    keys = supernode_data_dict.keys()
    values = [value for value in supernode_data_dict.values()]
    supernode_data_dict_df = pd.DataFrame(values)
    supernode_data_dict_df['txid-vout'] = keys
    supernode_data_dict_df.set_index('txid-vout', inplace=True)
    current_time = pd.Timestamp.utcnow()  # Get the current UTC time as a Timestamp
    supernode_data_dict_df['lastseentime_datetime'] = current_time - pd.to_timedelta(supernode_data_dict_df['activeseconds'], unit='s')
    supernode_data_dict_df['lastseentime_datetime'] = supernode_data_dict_df['lastseentime_datetime'].dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    supernode_data_dict_df['ip_address'] = supernode_data_dict_df['ipaddress:port'].apply(lambda x: x.split(':')[0])
    temp_file, temp_file_path = tempfile.mkstemp(suffix=".csv")
    with os.fdopen(temp_file, 'w') as tmp:
        supernode_data_dict_df.to_csv(tmp)    
    current_datetime_string = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    return FileResponse(path=temp_file_path, media_type='application/octet-stream', filename=f"pastel_supernode_data__{current_datetime_string}.csv")

@router.get('/get_network_storage_fees', tags=["High-Level Methods"])
async def get_network_storage_fees():
    return await handle_exceptions(service_funcs.get_network_storage_fees_func)

@router.get('/verify_message_with_pastelid/{pastelid_pubkey}/{message_to_verify}/{pastelid_signature_on_message}', tags=["High-Level Methods"])
async def verify_message_with_pastelid(pastelid_pubkey: str, message_to_verify: str, pastelid_signature_on_message: str):
    return await handle_exceptions(service_funcs.verify_message_with_pastelid_func, pastelid_pubkey, message_to_verify, pastelid_signature_on_message)

#Endpoint to respond with a PastelID file, given a desired password:
@router.get('/testnet_pastelid_file_dispenser/{desired_password}', tags=["High-Level Methods"])
async def testnet_pastelid_file_dispenser(desired_password: str):
    pastelid_pubkey, pastelid_data = await handle_exceptions(service_funcs.testnet_pastelid_file_dispenser_func, desired_password)
    response = StreamingResponse([pastelid_data], media_type="application/binary")
    response.headers["Content-Disposition"] = f"attachment; filename=ticket.{pastelid_pubkey}"
    return response

@router.get('/get_parsed_dd_service_results_by_registration_ticket_txid/{txid}', tags=["OpenAPI Methods"])
async def get_parsed_dd_service_results_by_registration_ticket_txid(txid: str):
    return await handle_exceptions(service_funcs.get_parsed_dd_service_results_by_registration_ticket_txid_func, txid)

@router.get('/get_raw_dd_service_results_by_registration_ticket_txid/{txid}', tags=["OpenAPI Methods"])
async def get_raw_dd_service_results_by_registration_ticket_txid(txid: str):
    return await handle_exceptions(service_funcs.get_raw_dd_service_results_by_registration_ticket_txid_func, txid)

@router.get('/get_publicly_accessible_cascade_file_by_registration_ticket_txid/{txid}', tags=["OpenAPI Methods"])
async def get_publicly_accessible_cascade_file_by_registration_ticket_txid(txid: str):
    decoded_response, original_file_name_string = await handle_exceptions(service_funcs.download_publicly_accessible_cascade_file_by_registration_ticket_txid_func, txid)
    if isinstance(decoded_response, bytes):
        content_disposition_string = f"attachment; filename={original_file_name_string}"
        return StreamingResponse(io.BytesIO(decoded_response), media_type="application/octet-stream", headers={"Content-Disposition": content_disposition_string})
    else: #Handle error case (generally if the file is not publicly accessible)
        return JSONResponse(content={"message": decoded_response}, status_code=400)

@router.get('/get_parsed_dd_service_results_by_image_file_hash/{image_file_hash}', tags=["OpenAPI Methods"])
async def get_parsed_dd_service_results_by_image_file_hash(image_file_hash: str):
    return await handle_exceptions(service_funcs.get_parsed_dd_service_results_by_image_file_hash_func, image_file_hash)

@router.get('/get_raw_dd_service_results_by_image_file_hash/{image_file_hash}', tags=["OpenAPI Methods"])
async def get_raw_dd_service_results_by_image_file_hash(image_file_hash: str):
    return await handle_exceptions(service_funcs.get_raw_dd_service_results_by_image_file_hash_func, image_file_hash)

@router.get('/get_parsed_dd_service_results_by_pastel_id_of_submitter/{pastel_id_of_submitter}', tags=["OpenAPI Methods"])
async def get_parsed_dd_service_results_by_pastel_id_of_submitter(pastel_id_of_submitter: str):
    return await handle_exceptions(service_funcs.get_parsed_dd_service_results_by_pastel_id_of_submitter_func, pastel_id_of_submitter)
@router.get('/get_parsed_dd_service_results_by_pastel_block_hash_when_request_submitted/{pastel_block_hash_when_request_submitted}', tags=["OpenAPI Methods"])
async def get_parsed_dd_service_results_by_pastel_block_hash_when_request_submitted(pastel_block_hash_when_request_submitted: str):
    return await handle_exceptions(service_funcs.get_parsed_dd_service_results_by_pastel_block_hash_when_request_submitted_func, pastel_block_hash_when_request_submitted)

@router.get('/get_raw_dd_service_results_by_pastel_block_hash_when_request_submitted/{pastel_block_hash_when_request_submitted}', tags=["OpenAPI Methods"])
async def get_raw_dd_service_results_by_pastel_block_hash_when_request_submitted(pastel_block_hash_when_request_submitted: str):
    return await handle_exceptions(service_funcs.get_raw_dd_service_results_by_pastel_block_hash_when_request_submitted_func, pastel_block_hash_when_request_submitted)

@router.get('/get_parsed_dd_service_results_by_pastel_block_height_when_request_submitted/{pastel_block_height_when_request_submitted}', tags=["OpenAPI Methods"])
async def get_parsed_dd_service_results_by_pastel_block_height_when_request_submitted(pastel_block_height_when_request_submitted: str):
    return await handle_exceptions(service_funcs.get_parsed_dd_service_results_by_pastel_block_height_when_request_submitted_func, pastel_block_height_when_request_submitted)

@router.get('/get_raw_dd_service_results_by_pastel_block_height_when_request_submitted/{pastel_block_height_when_request_submitted}', tags=["OpenAPI Methods"])
async def get_raw_dd_service_results_by_pastel_block_height_when_request_submitted(pastel_block_height_when_request_submitted: str):
    return await handle_exceptions(service_funcs.get_raw_dd_service_results_by_pastel_block_height_when_request_submitted_func, pastel_block_height_when_request_submitted)

@router.get('/get_current_total_number_of_registered_sense_fingerprints/', tags=["OpenAPI Methods"])
async def get_current_total_number_of_registered_sense_fingerprints():
    return await handle_exceptions(service_funcs.get_current_total_number_of_registered_sense_fingerprints_func)

@router.get('/get_current_total_number_and_size_and_average_size_of_registered_cascade_files/', tags=["OpenAPI Methods"])
async def get_current_total_number_and_size_and_average_size_of_registered_cascade_files():
    # return await handle_exceptions(service_funcs.get_current_total_number_and_size_and_average_size_of_registered_cascade_files_func)
    return await service_funcs.get_current_total_number_and_size_and_average_size_of_registered_cascade_files_func()

@router.get('/get_usernames_from_pastelid/{pastelid}', tags=["OpenAPI Methods"])
async def get_usernames_from_pastelid(pastelid : str):
    response = await handle_exceptions(service_funcs.get_usernames_from_pastelid_func, pastelid)
    return JSONResponse(content={"pastelid_query": pastelid, "matching_usernames": response})

@router.get('/get_all_registration_ticket_txids_corresponding_to_a_collection_ticket_txid/{collection_ticket_txid}', tags=["OpenAPI Methods"])
async def get_all_registration_ticket_txids_corresponding_to_a_collection_ticket_txid(collection_ticket_txid : str):
    return await handle_exceptions(service_funcs.get_all_registration_ticket_txids_corresponding_to_a_collection_ticket_txid_func, collection_ticket_txid)

@router.get('/get_pastelid_from_username/{username}', tags=["OpenAPI Methods"])
async def get_pastelid_from_username(username : str):
    response = await handle_exceptions(service_funcs.get_pastelid_from_username_func, username)
    return JSONResponse(content={"username_query": username, "matching_pastelid": response})

@router.get('/populate_database_with_all_dd_service_data', tags=["OpenAPI Methods"])
async def populate_database_with_all_dd_service_data(background_tasks: BackgroundTasks):
    try:
        background_tasks.add_task(service_funcs.populate_database_with_all_dd_service_data_func)
        return 'Started background task to populate database with all sense data...'
    except Exception as x:
        return fastapi.Response(content=str(x), status_code=500)

@router.get('/run_bulk_test_cascade/{num_downloads}', tags=["OpenAPI Methods"])
async def run_bulk_test_cascade(num_downloads: int):
    if os.environ.get("RUN_BACKGROUND_TASKS") == "1":
        return await handle_exceptions(service_funcs.bulk_test_cascade_func, num_downloads)

@router.get("/get_address_mempool", tags=["Insight Explorer Methods"])
async def get_address_mempool_endpoint(addresses: str):
    """
    Returns all mempool deltas for the specified addresses.

    This endpoint retrieves information about the changes in the mempool for the given addresses.
    It provides details such as the transaction ID, index, amount (patoshis), timestamp, previous
    transaction ID (if spending), and previous output index (if spending).

    Note: The `getaddressmempool` RPC method is disabled by default. To enable it, you need to restart
    `pasteld` with the `-insightexplorer` command-line options, or add the
    following lines to the `pastel.conf` file:
    ```
    insightexplorer=1
    ```

    Args:
        addresses (str): A comma-separated list of base58check encoded addresses.

    Returns:
        BlockDeltasResponse: A list of mempool deltas for the specified addresses.

    Raises:
        HTTPException: If an error occurs while processing the request.

    Examples:
        >>> from httpx import get
        >>> response = get("http://localhost:8000/get_address_mempool?addresses=tPp3pfmLi57S8qoccfWnn2o4tXyoQ23wVSp,tQm5Z5X7ZRnD5cLH5jUPCzLWQRKEDcVP9tC")
        >>> response.json()
        {
            "data": [
                {
                    "address": "tPp3pfmLi57S8qoccfWnn2o4tXyoQ23wVSp",
                    "txid": "1a2b3c...",
                    "index": 0,
                    "patoshis": 1000,
                    "timestamp": 1623456789,
                    "prevtxid": "4d5e6f...",
                    "prevout": 1
                }
            ]
        }
    """
    address_list = addresses.split(",")
    return await handle_exceptions(service_funcs.get_address_mempool_func, {"addresses": address_list})

@router.get("/get_block_deltas/{block_hash}", tags=["Insight Explorer Methods"])
async def get_block_deltas_endpoint(block_hash: str):
    """
    Returns the transaction IDs and indexes where outputs are spent for a given block.

    This endpoint retrieves the block deltas for a specified block hash. It provides information
    about the transactions within the block, including the transaction IDs, indexes, inputs (addresses,
    amounts, previous transaction IDs, and previous output indexes), and outputs (addresses, amounts,
    and indexes).

    Note: The `getblockdeltas` RPC method is disabled by default. To enable it, you need to restart
    `pasteld` with the `-insightexplorer` command-line options, or add the
    following lines to the `pastel.conf` file:
    ```
    insightexplorer=1
    ```

    Args:
        block_hash (str): The block hash.

    Returns:
        BlockDeltasResponse: The block deltas for the specified block hash.

    Raises:
        HTTPException: If an error occurs while processing the request.

    Examples:
        >>> from httpx import get
        >>> response = get("http://localhost:8000/get_block_deltas/00227e566682aebd6a7a5b772c96d7a999cadaebeaf1ce96f4191a3aad58b00b")
        >>> response.json()
        {
            "data": {
                "hash": "00227e566682aebd6a7a5b772c96d7a999cadaebeaf1ce96f4191a3aad58b00b",
                "confirmations": 5,
                "size": 250,
                "height": 1000,
                "version": 4,
                "merkleroot": "1a2b3c...",
                "deltas": [
                    {
                        "txid": "4d5e6f...",
                        "index": 0,
                        "inputs": [
                            {
                                "address": "tPp3pfmLi57S8qoccfWnn2o4tXyoQ23wVSp",
                                "patoshis": -1000,
                                "index": 0,
                                "prevtxid": "7a8b9c...",
                                "prevout": 2
                            }
                        ],
                        "outputs": [
                            {
                                "address": "tQm5Z5X7ZRnD5cLH5jUPCzLWQRKEDcVP9tC",
                                "patoshis": 500,
                                "index": 0
                            },
                            {
                                "address": "tPp3pfmLi57S8qoccfWnn2o4tXyoQ23wVSp",
                                "patoshis": 500,
                                "index": 1
                            }
                        ]
                    }
                ],
                "time": 1623456789,
                "mediantime": 1623456780,
                "nonce": "1a2b3c...",
                "bits": "1d00ffff",
                "difficulty": 1,
                "chainwork": "0000000000000000000000000000000000000000000000000000000000000002",
                "previousblockhash": "1a2b3c...",
                "nextblockhash": "4d5e6f..."
            }
        }
    """
    return await handle_exceptions(service_funcs.get_block_deltas_func, block_hash)

@router.get("/get_address_txids", tags=["Insight Explorer Methods"])
async def get_address_txids_endpoint(addresses: List[str] = Query(...), start: Optional[int] = None, end: Optional[int] = None):
    """
    Returns the transaction ids for given transparent addresses within the given (inclusive)
    block height range, default is the full blockchain.

    Returned txids are in the order they appear in blocks, which
    ensures that they are topologically sorted (i.e. parent txids will appear before child txids).

    Args:
        addresses (List[str], query parameter): A list of base58check encoded addresses.
        start (int, optional, query parameter): The start block height. Default is None.
        end (int, optional, query parameter): The end block height. Default is None.

    Returns:
        List[str]: A list of transaction ids.

    Examples:
        >>> from httpx import get
        >>> response = get("http://localhost:8000/get_address_txids?addresses=PtczsZ91Bt3oDPDQotzUsrx1wjmsFVgf28n&start=1000&end=2000")
        >>> response.json()
        [
            "txid1",
            "txid2",
            ...
        ]
    """
    params = {"addresses": addresses, "start": start, "end": end}
    return await handle_exceptions(service_funcs.get_address_txids_func, params)

@router.get("/get_address_balance", tags=["Insight Explorer Methods"])
async def get_address_balance_endpoint(addresses: List[str] = Query(...)):
    """
    Returns the balance for addresses.

    Args:
        addresses (List[str], query parameter): A list of base58check encoded addresses.

    Returns:
        dict: A dictionary containing the balance and received amounts.
            balance (str): The current balance in patoshis.
            received (str): The total number of patoshis received (including change).

    Examples:
        >>> from httpx import get
        >>> response = get("http://localhost:8000/get_address_balance?addresses=tmYXBYJj1K7vhejSec5osXK2QsGa5MTisUQ")
        >>> response.json()
        {
            "balance": "1000",
            "received": "2000"
        }
    """
    params = {"addresses": addresses}
    return await handle_exceptions(service_funcs.get_address_balance_func, params)

@router.get("/get_address_deltas", tags=["Insight Explorer Methods"])
async def get_address_deltas_endpoint(addresses: List[str] = Query(...), start: Optional[int] = None, end: Optional[int] = None, chainInfo: bool = False):
    """
    Returns all changes for an address.

    Returns information about all changes to the given transparent addresses within the given (inclusive)
    block height range, default is the full blockchain.

    Args:
        addresses (List[str], query parameter): A list of base58check encoded addresses.
        start (int, optional, query parameter): The start block height. Default is None.
        end (int, optional, query parameter): The end block height. Default is None.
        chainInfo (bool, optional, query parameter): Include chain info in results, only applies if start and end specified. Default is false.

    Returns:
        List[dict] or dict: A list of address deltas or a dictionary containing address deltas and chain info.
            patoshis (int): The difference of patoshis.
            txid (str): The related txid.
            index (int): The related input or output index.
            height (int): The block height.
            address (str): The base58check encoded address.

    Examples:
        >>> from httpx import get
        >>> response = get("http://localhost:8000/get_address_deltas?addresses=tmYXBYJj1K7vhejSec5osXK2QsGa5MTisUQ&start=1000&end=2000&chainInfo=true")
        >>> response.json()
        {
            "deltas": [
                {
                    "patoshis": 1000,
                    "txid": "txid1",
                    "index": 0,
                    "height": 1500,
                    "address": "tmYXBYJj1K7vhejSec5osXK2QsGa5MTisUQ"
                },
                ...
            ],
            "start": {
                "hash": "start_block_hash",
                "height": 1000
            },
            "end": {
                "hash": "end_block_hash",
                "height": 2000
            }
        }
    """
    params = {"addresses": addresses, "start": start, "end": end, "chainInfo": chainInfo}
    return await handle_exceptions(service_funcs.get_address_deltas_func, params)

@router.get("/get_address_utxos", tags=["Insight Explorer Methods"])
async def get_address_utxos_endpoint(addresses: List[str] = Query(...), chainInfo: bool = False):
    """
    Returns all unspent outputs for an address.

    Args:
        addresses (List[str], query parameter): A list of base58check encoded addresses.
        chainInfo (bool, optional, query parameter): Include chain info with results. Default is false.

    Returns:
        List[dict] or dict: A list of unspent outputs or a dictionary containing unspent outputs and chain info.
            address (str): The address base58check encoded.
            txid (str): The output txid.
            height (int): The block height.
            outputIndex (int): The output index.
            script (str): The script hex encoded.
            patoshis (int): The number of patoshis of the output.

    Examples:
        >>> from httpx import get
        >>> response = get("http://localhost:8000/get_address_utxos?addresses=tmYXBYJj1K7vhejSec5osXK2QsGa5MTisUQ&chainInfo=true")
        >>> response.json()
        {
            "utxos": [
                {
                    "address": "tmYXBYJj1K7vhejSec5osXK2QsGa5MTisUQ",
                    "txid": "txid1",
                    "height": 1500,
                    "outputIndex": 0,
                    "script": "script_hex",
                    "patoshis": 1000
                },
                ...
            ],
            "hash": "block_hash",
            "height": 2000
        }
    """
    params = {"addresses": addresses, "chainInfo": chainInfo}
    return await handle_exceptions(service_funcs.get_address_utxos_func, params)

@router.get("/get_spent_info", tags=["Insight Explorer Methods"])
async def get_spent_info_endpoint(txid: str, index: int):
    """
    Returns the txid and index where an output is spent.

    Args:
        txid (str, query parameter): The hex string of the transaction id.
        index (int, query parameter): The vout (output) index.

    Returns:
        dict: A dictionary containing the spending transaction id and input index.
            txid (str): The transaction id.
            index (int): The spending (vin, input) index.

    Examples:
        >>> from httpx import get
        >>> response = get("http://localhost:8000/get_spent_info?txid=33990288fb116981260be1de10b8c764f997674545ab14f9240f00346333b780&index=4")
        >>> response.json()
        {
            "txid": "spending_txid",
            "index": 0
        }
    """
    params = {"txid": txid, "index": index}
    return await handle_exceptions(service_funcs.get_spent_info_func, params)

@router.get("/get_block_hashes", tags=["Insight Explorer Methods"])
async def get_block_hashes_endpoint(high: int, low: int, noOrphans: Optional[bool] = None, logicalTimes: Optional[bool] = None):
    """
    Returns array of hashes of blocks within the timestamp range provided,
    greater or equal to low, less than high.

    Args:
        high (int, query parameter): The newer block timestamp.
        low (int, query parameter): The older block timestamp.
        noOrphans (bool, optional, query parameter): Will only include blocks on the main chain. Default is None.
        logicalTimes (bool, optional, query parameter): Will include logical timestamps with hashes. Default is None.

    Returns:
        List[str] or List[dict]: An array of block hashes or an array of objects containing block hashes and logical timestamps.

    Examples:
        >>> from httpx import get
        >>> response = get("http://localhost:8000/get_block_hashes?high=1558141697&low=1558141576")
        >>> response.json()
        [
            "block_hash_1",
            "block_hash_2",
            ...
        ]

        >>> response = get("http://localhost:8000/get_block_hashes?high=1558141697&low=1558141576&noOrphans=false&logicalTimes=true")
        >>> response.json()
        [
            {
                "blockhash": "block_hash_1",
                "logicalts": 1558141580
            },
            ...
        ]
    """
    options = {"noOrphans": noOrphans, "logicalTimes": logicalTimes}
    return await handle_exceptions(service_funcs.get_block_hashes_func, high, low, options)

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
                new_logs.append(service_funcs.highlight_rules_func(line.rstrip('\n')))  # add the highlighted log to the list and strip any newline at the end
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
            logs.append(service_funcs.highlight_rules_func(line.rstrip('\n')))  # add the highlighted log to the list and strip any newline at the end
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

@router.get('/psl/average_price', tags=["Price Methods"])
async def get_average_psl_price():
    try:
        average_price = await service_funcs.fetch_current_psl_market_price()
        return average_price
    except ValueError as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/websocket_info", response_class=HTMLResponse, tags=["Price Methods"])
async def websocket_info():
    html_content = """
    <html>
    <head>
        <title>WebSocket Endpoint Documentation</title>
    </head>
    <body>
        <h1>WebSocket Endpoint: /ws/psl/price</h1>
        <p>This endpoint provides real-time PSL price updates.</p>
        <h2>How to Use:</h2>
        <p>Connect to the WebSocket endpoint using a WebSocket client.</p>
        <pre>
        ws = new WebSocket("wss://opennode-fastapi.pastel.network/ws/psl/price");
        ws.onmessage = function(event) {
            console.log("Price update: ", event.data);
        };
        </pre>
        <p>The server sends the current PSL price every 5 seconds.</p>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)

@router.websocket("/ws/psl/price")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            try:
                price = await service_funcs.fetch_current_psl_market_price()
                await manager.send_personal_message(f"{price}", websocket)
            except ValueError as e:
                await manager.send_personal_message(f"Error: {str(e)}", websocket)
            await asyncio.sleep(5)  # Refresh every 5 seconds
    except WebSocketDisconnect:
        manager.disconnect(websocket)

# if __name__ == "__main__":
rpc_host, rpc_port, rpc_user, rpc_password, other_flags = service_funcs.get_local_rpc_settings_func()
rpc_connection = service_funcs.AsyncAuthServiceProxy(f"http://{rpc_user}:{rpc_password}@{rpc_host}:{rpc_port}")
