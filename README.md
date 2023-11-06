# OpenNode FastAPI for Pastel Network

## Overview

The OpenNode FastAPI serves as an interface to the Pastel Network, providing a set of RESTful endpoints to interact with the Pastel Blockchain. It is designed to facilitate access to blockchain data, network statistics, mining information, and utility methods for both developers and users within the ecosystem.

### Features

- **Blockchain Methods**: Access blockchain data such as blocks, transactions, and blockchain info.
- **Control Methods**: Retrieve network and node information.
- **Mining Methods**: Obtain mining-related data, including block subsidy and mining info.
- **Network Methods**: Methods to get network stats, peer info, and node-specific information.
- **Supernode Methods**: Interact with Masternodes for enhanced network functionalities.
- **Raw Transaction Methods**: Work with raw transactions and decode transaction details.
- **Utility Methods**: Utility functions such as address validation and fee estimation.
- **Ticket Methods**: List and search for various blockchain tickets as defined by the Pastel Network.
- **High-Level Methods**: High-level data retrieval like total registered NFTs, storage fees, and more.
- **OpenAPI Methods**: Fetch and populate data related to the decentralized storage layer and more.

## Installation

1. Clone the repository:

```sh
git clone https://github.com/pastelnetwork/opennode_fastapi
cd opennode_fastapi
```

2. Install the required dependencies:

```sh
python3 -m venv venv
source venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install wheel
pip install -r requirements.txt
```

## Running the API

To launch the FastAPI server:

```sh
uvicorn main:app --host 0.0.0.0 --port 8000
```

This will start the API server, making it accessible at `http://localhost:8000`.

## Usage

You can interact with the API through the provided Swagger UI by navigating to `http://localhost:8000` in your web browser, or by using `curl` commands as described in the Swagger documentation.

### Examples

- **Get Best Block Hash**:

```sh
curl -X 'GET' 'http://localhost:8000/getbestblockhash' -H 'accept: application/json'
```

- **Get Blockchain Info**:

```sh
curl -X 'GET' 'http://localhost:8000/getblockchaininfo' -H 'accept: application/json'
```

... and so on for the other endpoints.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request for any bugs, features, or improvements.

## License

MIT License.

---

Remember to replace `[repository-url]` and `[repository-name]` with your actual repository URL and name. Also, adjust the running port and host if needed. Ensure that you include a license for the project to inform users of how they can legally use and contribute to the project.
