import asyncio
import base64
import decimal
import json
import os
import warnings
from httpx import AsyncClient, Limits, Timeout
import urllib.parse as urlparse
import logging
import shutil
import multiprocessing
from logging.handlers import RotatingFileHandler, QueueHandler, QueueListener
from multiprocessing import Manager, Process
from multiprocessing.managers import BaseManager

# Pip install aiohttp httpx

# Configure the logger
def setup_logger():
    logger = logging.getLogger("pastel_rpc_client")
    if logger.handlers:
        return logger

    old_logs_dir = 'old_logs'
    if not os.path.exists(old_logs_dir):
        os.makedirs(old_logs_dir)

    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    log_file_path = 'pastel_rpc_client.log'
    log_queue = multiprocessing.Queue(-1)  # multiprocessing-safe queue

    fh = RotatingFileHandler(log_file_path, maxBytes=10*1024*1024, backupCount=5)
    fh.setFormatter(formatter)

    def namer(default_log_name):
        return os.path.join(old_logs_dir, os.path.basename(default_log_name))

    def rotator(source, dest):
        shutil.move(source, dest)

    fh.namer = namer
    fh.rotator = rotator
    queue_handler = QueueHandler(log_queue)
    queue_handler.setFormatter(formatter)
    logger.addHandler(queue_handler)

    listener = QueueListener(log_queue, fh)
    listener.start()

    logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)
    logging.getLogger('httpx').setLevel(logging.WARNING)

    return logger, log_queue

logger, log_queue = setup_logger()

loop = asyncio.get_event_loop()
warnings.filterwarnings('ignore')
USER_AGENT = "AuthServiceProxy/0.1"
HTTP_TIMEOUT = 180
        
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
        except Exception as e:
            logger.error(f"Error occurred in JSONRPCException: {e}")
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
        logging.getLogger('httpx').setLevel(logging.WARNING)

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
                        logger.warning(f"Reconnect try #{i+1}")
                        sleep_time = self.reconnect_timeout * (2 ** i)
                        logger.info(f"Waiting for {sleep_time} seconds before retrying.")
                        await asyncio.sleep(sleep_time)
                    response = await self.client.post(
                        self.service_url, headers=headers, data=postdata)
                    break
                except Exception as e:
                    logger.error(f"Error occurred in __call__: {e}")
                    err_msg = f"Failed to connect to {self.url.hostname}:{self.url.port}"
                    rtm = self.reconnect_timeout
                    if rtm:
                        err_msg += f". Waiting {rtm} seconds."
                    logger.exception(err_msg)
            else:
                logger.error("Reconnect tries exceeded.")
                return
            response_json = response.json()
            if response_json['error'] is not None:
                raise JSONRPCException(response_json['error'])
            elif 'result' not in response_json:
                raise JSONRPCException({
                    'code': -343, 'message': 'missing JSON-RPC result'})
            else:
                return response_json['result']
        
async def get_current_pastel_block_height_func(rpc_connection):
    best_block_hash = await rpc_connection.getbestblockhash()
    best_block_details = await rpc_connection.getblock(best_block_hash)
    curent_block_height = best_block_details['height']
    return curent_block_height

def extract_total_block_reward(block_data):
    total_reward = 0.0
    reward_transactions = [block_data['tx'][0]] # Identify transactions that are part of the block reward
    for tx in reward_transactions:
        # Sum the values of all outputs in the transaction
        total_reward += sum(out['value'] for out in tx['vout'])
    return total_reward

# Worker process function
def worker_process(queue, rpc_connection_details, return_dict):
    # Setup logger for each worker
    logger = logging.getLogger("pastel_rpc_client_worker")
    logger.addHandler(logging.handlers.QueueHandler(log_queue))
    # Setup RPC connection
    rpc_connection = AsyncAuthServiceProxy(f"http://{rpc_connection_details['user']}:{rpc_connection_details['password']}@{rpc_connection_details['host']}:{rpc_connection_details['port']}")
    local_total_supply = 0.0
    while True:
        block_height = queue.get()
        if block_height is None:
            # Sentinel value received, terminate process
            break
        try:
            # Process block
            block_hash = asyncio.run(rpc_connection.getblockhash(block_height))
            block_data = asyncio.run(rpc_connection.getblock(block_hash, 2))
            coinbase_value = extract_total_block_reward(block_data)
            local_total_supply += coinbase_value
            logger.info(f"Processed block {block_height} with coinbase value {coinbase_value} PSL")
        except Exception as e:
            logger.error(f"Error processing block at height {block_height}: {e}")
    return_dict[block_height] = local_total_supply

async def calculate_total_coin_supply():
    global rpc_connection
    total_supply = 0.0
    # Get current block height
    current_block_height = await get_current_pastel_block_height_func(rpc_connection)
    # Iterate over all blocks
    for height in range(current_block_height + 1):
        try:
            # Get block hash for the current height
            block_hash = await rpc_connection.getblockhash(height)
            # Retrieve the full block data with verbosity level 2
            block_data = await rpc_connection.getblock(block_hash, 2)
            coinbase_value = extract_total_block_reward(block_data)
            logger.info(f"Block {height} coinbase value: {coinbase_value} PSL")
            total_supply += coinbase_value
            logger.info(f"Total Coin Supply: {total_supply} PSL at block {height}")
        except Exception as e:
            logger.error(f"Error processing block at height {height}: {e}")
            continue
    return total_supply

# Main function
def main():
    # Obtain RPC connection settings
    rpc_host, rpc_port, rpc_user, rpc_password, _ = get_local_rpc_settings_func()
    rpc_connection_details = {'host': rpc_host, 'port': rpc_port, 'user': rpc_user, 'password': rpc_password}

    # Set up a manager, a work queue, and a dictionary for results
    BaseManager.register('get_queue', lambda: log_queue)
    with Manager() as manager:
        work_queue = manager.Queue()
        return_dict = manager.dict()

        # Start worker processes
        num_workers = 4  # Adjust as needed
        workers = [Process(target=worker_process, args=(work_queue, rpc_connection_details, return_dict)) for _ in range(num_workers)]
        for worker in workers:
            worker.start()

        # Distribute work
        rpc_connection = AsyncAuthServiceProxy(f"http://{rpc_connection_details['user']}:{rpc_connection_details['password']}@{rpc_connection_details['host']}:{rpc_connection_details['port']}")
        current_block_height = asyncio.run(get_current_pastel_block_height_func(rpc_connection))
        for height in range(current_block_height + 1):
            work_queue.put(height)

        # Add sentinel values to stop workers
        for _ in range(num_workers):
            work_queue.put(None)

        # Wait for all workers to finish
        for worker in workers:
            worker.join()

        # Aggregate results
        total_supply = sum(return_dict.values())
        logger.info(f"Total Coin Supply: {total_supply} PSL")

# Run the main function
if __name__ == "__main__":
    main()