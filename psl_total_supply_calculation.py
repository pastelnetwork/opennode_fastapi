import asyncio
import base64
import decimal
import json
import time
import os
import warnings
from httpx import AsyncClient, Limits, Timeout
try:
    import urllib.parse as urlparse
except ImportError:
    import urlparse
import logging
import queue
from multiprocessing import Manager, Process, Queue
from logging.handlers import RotatingFileHandler, QueueHandler, QueueListener

logger = logging.getLogger("psl_total_supply_calculation")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ExcludeHTTPRequestsFilter(logging.Filter):
    def filter(self, record):
        # Exclude log records that contain the HTTP request pattern
        return "HTTP Request: POST" not in record.getMessage()

def setup_main_logger():
    logger = logging.getLogger("psl_total_supply_calculation")
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    log_file_path = 'psl_total_supply_calculation.log'
    fh = RotatingFileHandler(log_file_path, maxBytes=10*1024*1024, backupCount=5)
    fh.setFormatter(formatter)
    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    queue = Queue()
    queue_handler = QueueHandler(queue)
    listener = QueueListener(queue, fh, sh)
    logger.addHandler(queue_handler)
    listener.start()
    return logger, queue, listener

logger, log_queue, listener = setup_main_logger()

number_of_cpus = os.cpu_count()
loop = asyncio.get_event_loop()
warnings.filterwarnings('ignore')

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
        
async def get_current_pastel_block_height_func():
    global rpc_connection
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

async def calculate_total_coin_supply():
    global rpc_connection
    total_supply = 0.0
    # Get current block height
    current_block_height = await get_current_pastel_block_height_func()
    # Iterate over all blocks
    for height in range(current_block_height + 1):
        try:
            # Get block hash for the current height
            block_hash = await rpc_connection.getblockhash(height)
            # Retrieve the full block data with verbosity level 2
            
            block_data = await rpc_connection.getblock(block_hash, 2)
            coinbase_value = extract_total_block_reward(block_data)
            print(f"Block {height} coinbase value: {coinbase_value} PSL")
            total_supply += coinbase_value
            print(f"Total Coin Supply: {total_supply} PSL at block {height}")
        except Exception as e:
            logger.error(f"Error processing block at height {height}: {e}")
            continue
    return total_supply

def setup_logger_multiprocessing(queue):
    logger = logging.getLogger()
    logger.handlers = []  # Clear existing handlers
    logger.setLevel(logging.INFO)
    queue_handler = logging.handlers.QueueHandler(queue)
    logger.addHandler(queue_handler)

def listener_process(log_queue):
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    fh = RotatingFileHandler('psl_total_supply_calculation.log', maxBytes=10*1024*1024, backupCount=5)
    fh.setFormatter(formatter)
    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    logger = logging.getLogger()
    logger.addHandler(fh)
    logger.addHandler(sh)
    while True:
        try:
            record = log_queue.get()
            if record is None:  # End signal
                break
            if isinstance(record, logging.LogRecord):
                logger.handle(record)
            else:
                block_number, message = record
                logger.info(message)
        except Exception as e:
            logger.error(f"Error in listener_process: {e}")

async def process_blocks(start_block, end_block, shared_dict, log_dict, log_buffer_lock, log_queue):
    setup_logger_multiprocessing(log_queue)
    for height in range(start_block, end_block):
        try:
            block_hash = await rpc_connection.getblockhash(height)
            block_data = await rpc_connection.getblock(block_hash, 2)
            coinbase_value = extract_total_block_reward(block_data)
            shared_dict[height] = coinbase_value
            with log_buffer_lock:
                log_dict[height] = f"Block {height} coinbase value: {coinbase_value} PSL"
        except Exception as e:
            shared_dict[height] = None
            with log_buffer_lock:
                log_dict[height] = f"Error processing block at height {height}: {e}"

def process_block_range(start_block, end_block, result_queue):
    for height in range(start_block, end_block):
        try:
            block_hash = rpc_connection.getblockhash(height)  # Synchronous call in this context
            block_data = rpc_connection.getblock(block_hash, 2)  # Synchronous call in this context
            coinbase_value = extract_total_block_reward(block_data)

            result_queue.put((height, coinbase_value))
        except Exception as e:
            logger.error(f"Error processing block at height {height}: {e}")
            result_queue.put((height, None))  # Indicate failure for this block

def flush_log_dict(ordered_log_dict, log_queue, manager, flush_interval=10):
    while True:
        time.sleep(flush_interval)
        try:
            if not log_queue.empty():
                continue  # Skip flush if new logs are incoming
            with manager.Lock():
                for block_num in sorted(ordered_log_dict.keys()):
                    log_queue.put((block_num, ordered_log_dict[block_num]))
                    del ordered_log_dict[block_num]
        except Exception as e:
            logger.error(f"Error flushing log dictionary: {e}")
                

def start_process_block_range(start_block, end_block, shared_dict, log_queue):
    asyncio.run(process_block_range(start_block, end_block, shared_dict, log_queue))

def parallel_block_processing(current_block_height, num_workers, result_queue):
    chunk_size = max(1, (current_block_height + 1) // num_workers)
    processes = []

    for i in range(num_workers):
        start_block = i * chunk_size
        end_block = min(start_block + chunk_size, current_block_height + 1)
        p = Process(target=process_block_range, args=(start_block, end_block, result_queue))
        processes.append(p)
        p.start()

    for p in processes:
        p.join()

    total_coin_supply = 0
    while not result_queue.empty():
        _, coinbase_value = result_queue.get()
        if coinbase_value is not None:
            total_coin_supply += coinbase_value

    return total_coin_supply

def worker(task_queue, result_queue):
    while not task_queue.empty():
        try:
            block_number = task_queue.get_nowait()
        except queue.Empty:
            break

        try:
            # Process the block (You might need to adjust this part based on your blockchain interaction)
            block_hash = rpc_connection.getblockhash(block_number)  # This should be an async call
            block_data = rpc_connection.getblock(block_hash, 2)  # This should be an async call
            coinbase_value = extract_total_block_reward(block_data)

            result_queue.put((block_number, coinbase_value))
        except Exception as e:
            logger.error(f"Error processing block at height {block_number}: {e}")

def ordered_logging(log_queue, total_blocks):
    block_logs = {}
    next_block_to_log = 0
    seen_blocks = set()  # Track processed blocks to avoid duplicates
    while next_block_to_log < total_blocks:
        if not log_queue.empty():
            block_number, message = log_queue.get()
            if block_number in seen_blocks:
                continue  # Skip duplicates
            seen_blocks.add(block_number)
            block_logs[block_number] = message
            while next_block_to_log in block_logs:
                logger.info(block_logs.pop(next_block_to_log))
                next_block_to_log += 1
                
def worker_process(block_queue, result_queue, rpc_connection):
    while True:
        try:
            block_number = block_queue.get_nowait()
            # Fetch block data using rpc_connection, which should be process-safe
            block_hash = rpc_connection.getblockhash(block_number)  # Synchronous call
            block_data = rpc_connection.getblock(block_hash, 2)  # Synchronous call
            coinbase_value = extract_total_block_reward(block_data)
            result_queue.put(coinbase_value)
        except queue.Empty:
            break
        except Exception as e:
            logger.error(f"Error processing block at height {block_number}: {e}")

def calculate_total_coin_supply_parallel(current_block_height):
    manager = Manager()
    block_queue = manager.Queue()
    result_queue = manager.Queue()

    for block_number in range(current_block_height + 1):
        block_queue.put(block_number)

    workers = [Process(target=worker_process, args=(block_queue, result_queue, rpc_connection))
               for _ in range(number_of_cpus)]

    for worker in workers:
        worker.start()

    for worker in workers:
        worker.join()

    total_supply = 0
    while not result_queue.empty():
        total_supply += result_queue.get()

    return total_supply

rpc_host, rpc_port, rpc_user, rpc_password, other_flags = get_local_rpc_settings_func()
rpc_connection = AsyncAuthServiceProxy(f"http://{rpc_user}:{rpc_password}@{rpc_host}:{rpc_port}")
use_parallel_processing = 1

async def main():
    global rpc_connection
    rpc_connection = AsyncAuthServiceProxy(f"http://{rpc_user}:{rpc_password}@{rpc_host}:{rpc_port}")
    current_block_height = await get_current_pastel_block_height_func()
    total_blocks = current_block_height + 1
    log_queue = Queue()
    logging_process = Process(target=ordered_logging, args=(log_queue, total_blocks))
    logging_process.start()
    if use_parallel_processing:
        print("Using parallel processing.")
        total_supply = calculate_total_coin_supply_parallel(current_block_height)
    else:
        print("Using sequential processing.")
        total_supply = await calculate_total_coin_supply()
    print(f"Total Coin Supply: {total_supply} PSL")
    log_queue.put_nowait(None)  # Signal to end logging
    logging_process.join()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())