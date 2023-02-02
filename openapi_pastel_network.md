# openapi.pastel.network
**Pastel’s OpenAPI Gateway provides Web3 developers with easy, robust, and reliable access to the Pastel Network and its underlying decentralized protocols via a lightweight,** ***centralized*** **service. For more information on how the OpenAPI Gateway works, please see our docs** [**here**](https://openapi.pastel.network/docs)**.**


******Clarifying Notes:**
*It’s important to understand the difference between concepts that exist in Pastel’s core decentralized blockchain protocol versus concepts that exist only in the centralized OpenAPI Gateway system. We urge* *users to review the following:*

***Core*** ***Pastel*** *********Decentralized*** ***Protocol Concepts:***

- **Sense** and **Cascade** are both operations that exist in the core Pastel blockchain protocol; Cascade is for permanent file storage, while Sense is for near-duplicate media file detection and fingerprinting (for now, only images are supported, but soon Sense will also support videos and audio files, and eventually also text documents, 3D files, and more). 


- **OpenAPI** is also part of the core protocol, and is used to distinguish native Pastel NFTs (which by default have the files stored in Cascade and the media files submitted to Sense) from NFTs that have been created on other blockchains such as Ethereum or Polygon, but which make use of Pastel for Cascade and Sense.


- Sense and Cascade operations are recorded to the blockchain via Pastel **Tickets**. The data for a given Ticket is stored in the corresponding Registration Ticket Transaction ID (`registration_ticket_txid`), which is a regular PSL transaction that can be found using the [Pastel Explorer website](https://explorer.pastel.network/) or directly in the pastel client software. It is generated when the Ticket is included in a mined block in the underlying blockchain.


- As the blockchain is immutable and “append only”, we determine whether a ticket has been finalized and fully accepted permanently in the blockchain with another ticket, referred to as the Activation Ticket TXID (`activation_ticket_txid`). There is a 1-to-1 correspondence between Registration Tickets and their associated Activation Tickets. Note: Activation Tickets are an implementation detail that that can be abstracted away when using the OpenAPI Gateway.


- For Cascade operations, users supply a `stored_file`, which can be identified explicitly by the corresponding `registration_ticket_txid` for that Cascade operation, or implicitly by using the `stored_file_sha256_hash`. As Cascade allows multiple users to store an identical file -  or the same user to store an identical file multiple times - using the `stored_file_sha256_hash` can lead to ambiguity in the case of multiple copies of the same file. This can be resolved by looking at additional information, such as the `block_height` or `pastelid` associated with the Cascade operation. While any user can identify the associated `registration_ticket` for a given Cascade operation, which includes metadata about the file, only the file owner can retrieve the actual underlying file (unless the `make_publicly_available`  attribute is specified at the time the file is first stored in Cascade).


- For Sense operations, users supply an `m``edia``_file`, which can be identified explicitly by the corresponding `registration_ticket_txid` for that Sense operation, or implicitly by using the `m``edia``_file_sha256_hash`. As Sense allows multiple users to store an identical same file - or the same user to store an identical file multiple times - using the `media``_file_sha256_hash` can lead to ambiguity in the case of multiple copies of the same media file. This can be resolved by looking at additional information, such as the `block_height` or `pastelid` associated with the Sense operation.
    - Any user can identify the associated `registration_ticket` for a given Sense operation, which includes Pastel-related metadata about the Sense operation, and any user can see the associated `sense_output_file` , which is the real underlying content of the Sense operation, containing a JSON file the contains various fields, such as the Pastel Rareness Score and NSFW scores of the media file (e.g., image file). 
    - Note that some of the important content, such as the table of the top 10 most similar previously registered images on Pastel, and the tables summarizing the “Rare on the Internet” analysis, are included in the Sense `raw_output_file` in the form of [Zstandard](https://github.com/facebook/zstd) compressed, base64 encoded strings, which must be decoded and decompressed before they can be effectively used.
    - As an additional convenience for users, the OpenAPI Gateway also allows users to download a `parsed_output_file`, where all fields are already fully decoded and decompressed, which makes the results easier to work with.


***Centralized OpenAPI Gateway Concepts:***

Note: The **OpenAPI Gateway** is ***not*** part of the core blockchain protocol; rather, it is a fully centralized service that has been made available to partners and users to make the process of integrating projects with Pastel faster and easier. Ultimately, all operations performed using the OpenAPI Gateway are eventually reflected in Pastel’s underlying decentralized blockchain protocol; the difference is the process for how these operations are submitted hand handled. 

Note: It is possible to use the **OpenAPI** features of Pastel without using the OpenAPI Gateway service, either by running your own privately hosted version of the Gateway Service (example client setup available [here](https://github.com/pastelnetwork/openapi-deployment)) or by performing the underlying operations directly using Pastel’s decentralized software (e.g., [pasteld](https://github.com/pastelnetwork/pastel), [gonode](https://github.com/pastelnetwork/gonode), etc.). 

**Gateway Requests**
A  `gateway_request` allows users to submit via OpenAPI Gateway a set of one or more files to be stored in Cascade or one or more media files (e.g., images) to be submitted to Sense.

Each `gateway_request` has a corresponding `gateway_request_id` that uniquely identifies it globally. The `gateway_request_id` is available to the user of the OpenAPI Gateway the moment a `gateway_request` is submitted (i.e., users do not need to wait a while for a request to propagate to the underlying Pastel blockchain itself).

The following items are returned by the Gateway for each `gateway_request`:

- `current_status` of the request of type:
    - `gateway_request_pending`
    - `gateway_request_successful`
    - `gateway_request_failed`
- A set of `status_messages` which provide the user with fine-grained information about the state of a given gateway_request and include the following:
    - “The gateway_request has been received by OpenAPI Gateway and is pending submission to the Pastel Network”
    - “The gateway_request has been submitted to the Pastel Network and is currently being processed.”
    - “The gateway_request has been successfully processed by the Pastel Network and the corresponding Registration Ticket has been included in block <block_height> with registration transaction id <registration_ticket_txid>”
    - “The gateway_request has been successfully finalized and activated on the Pastel Network at block <block_height> with activation transaction id <activation_ticket_txid>”
    - *In the case of a Cascade* `gateway_request`*, there will also be an additional status message: “*The file has been successfully stored (pinned) in IPFS, and can be retrieved with the following identifier: /ipfs/<ipfs_identifier>”

**Gateway Results**
A `gateway_result` refers to the output generated from a `gateway_request`, which contains various pieces of metadata. 

A single `gateway_request` can generate multiple `gateway_result` objects, and each `gateway_result` has a corresponding `gateway_result_id` that uniquely identifies it globally. 

Users can obtain a list of `gateway_result_id` objects from the corresponding `gateway_request_id`.

The following metadata fields are returned by the OpenAPI Gateway from a `gateway_result`; some fields are only included depending on the `gateway_request_id` type:

- File Name
- File Type
- Gateway Result ID
- Datetime Created
- Datetime Last Updated
- Gateway Request Status
- Retry Attempt Number
- Status Message
- Registration Ticket TXID
- Activation Ticket TXID
- IPFS Link (another backup to Cascade)
- AWS Link (another backup to Cascade)
~~~~    

Note: Information for any `gateway_result_id` will only be provided if the `current_status` is `gateway_request_successful`; if `current_status` is `gateway_request_failed`, then the OpenAPI Gateway will automatically resubmit the request for the user. If the `current_status` is `gateway_request_pending` or  `gateway_request_failed`, then the user will receive a placeholder informing them that results are `pending`.


----------


# OpenAPI Gateway Endpoints

**Login**
**…**
**Users**
**…**
**API_Keys**
…

**Cascade**
**…**

*OpenAPI Gateway Related Cascade Endpoints:*


    POST /api/v1/cascade/

Submit a Cascade OpenAPI gateway_request for the current user.


    GET /api/v1/cascade/gateway_requests

Get all Cascade OpenAPI gateway_requests for the current user.


    GET /api/v1/cascade/gateway_requests/{gateway_request_id}

Get an individual Cascade gateway_request by its the gateway_request_id.


    GET /api/v1/cascade/gateway_results

Get all Cascade gateway_results for the current user.


    GET /api/v1/cascade/gateway_results/{gateway_result_id}

Get an individual Cascade gateway_result by its result_id.


    GET /api/v1/cascade/stored_file/{gateway_result_id}

Get the underlying Cascade stored_file from the corresponding gateway_result_id.


    GET /api/v1/cascade/all_files_from_request/{gateway_request_id}

Get the set of underlying Cascade stored_files from the corresponding gateway_request_id.


    GET /api/v1/cascade/pastel_ticket_data_from_request_id/{gateway_request_id}

Get the the set of Pastel Cascade ticket data objects (metadata) from the blockchain corresponding to a particular gateway_request_id.


*Pastel Blockchain Related Cascade Endpoints:*


    GET /api/v1/cascade/file_from_registration_txid/{registration_ticket_txid}

Get the underlying Cascade stored_file from the corresponding Cascade Registration Ticket Transaction ID. (Note: Only available if the user owns the Cascade file or the `make_publically_accessible` flag is set to True for that Cascade operation.)


    GET /api/v1/cascade/pastel_ticket_data/{registration_ticket_txid}

Get the Pastel Ticket metadata corresponding to a Cascade Registration Ticket Transaction ID. (Note: Available to any user and also visible on the [Pastel Explorer site](https://explorer.pastel.network/)).


    GET /api/v1/cascade/pastel_ticket_data_from_stored_file_hash/{stored_file_sha256_hash}

Get the the set of Pastel Cascade ticket data objects (metadata) from the blockchain corresponding to a particular stored_file_sha256_hash. Contains pastel_block_number and pastel_id in case there are multiple results for the same stored_file_sha256_hash.


----------

**Sense**

*OpenAPI Gateway Related Sense Endpoints:*


    POST /api/v1/sense/

Submit a Sense OpenAPI gateway_request for the current user.


    GET /api/v1/sense/gateway_requests

Get all Sense OpenAPI gateway_requests for the current user.


    GET /api/v1/sense/gateway_requests/{gateway_request_id}

Get an individual Sense gateway_request by its the gateway_request_id.


    GET /api/v1/sense/gateway_results

Get all Sense gateway_results for the current user.


    GET /api/v1/sense/gateway_results/{gateway_result_id}

Get an individual Sense gateway_result by its result_id.


    GET /api/v1/sense/all_raw_output_files_from_request/{gateway_request_id}

Get the set of underlying Sense raw_outputs_files from the corresponding gateway_request_id.


    GET /api/v1/sense/all_parsed_output_files_from_request/{gateway_request_id}

Get the set of underlying Sense parsed_outputs_files from the corresponding gateway_request_id.


    GET /api/v1/sense/pastel_ticket_data_from_request_id/{gateway_request_id}

Get the the set of Pastel Sense ticket data objects (metadata) from the blockchain corresponding to a particular gateway_request_id.


*Pastel Blockchain Related Sense Endpoints:*


    GET /api/v1/sense/raw_output_file_from_registration_txid/{registration_ticket_txid}

Get the underlying Sense raw_output_file from the corresponding Sense Registration Ticket Transaction ID. (Note: Available to any user and also visible on the [Pastel Explorer site](https://explorer.pastel.network/)).


    GET /api/v1/sense/parsed_output_file_from_registration_txid/{registration_ticket_txid}

Get the underlying Sense parsed_output_file from the corresponding Sense Registration Ticket Transaction ID. (Note: Available to any user and also visible on the [Pastel Explorer site](https://explorer.pastel.network/)).


    GET /api/v1/sense/raw_output_file_from_pastel_id/{pastel_id_of_user}

Get a list of the Sense raw_output_files for the given pastel_id. (Note: Available to any user and also visible on the [Pastel Explorer site](https://explorer.pastel.network/)).


    GET /api/v1/sense/parsed_output_file_from_pastel_id/{pastel_id_of_user}

Get a list of the Sense parsed_output_files for the given pastel_id. (Note: Available to any user and also visible on the [Pastel Explorer site](https://explorer.pastel.network/)).


    GET /api/v1/sense/pastel_ticket_data/{registration_ticket_txid}

Get the Pastel Ticket metadata corresponding to a Sense Registration Ticket Transaction ID. (Note: Available to any user and also visible on the [Pastel Explorer site](https://explorer.pastel.network/)).


    GET /api/v1/sense/pastel_ticket_data_from_stored_file_hash/{media_file_sha256_hash}

Get the the set of Pastel Sense ticket data (metadata, not the underlying stored file) from the blockchain corresponding to a particular media_file_sha256_hash; Contains block number and pastel_id in case there are multiple results for the same media_file_sha256_hash.


----------

******Notes to Alexey:**

Modifications to functionality:

- If `current_status` is `gateway_request_failed`, then we should automatically re-submit the `gateway_request` to the network without requiring the user to do so; is it already doing that given this `retry_num` field? https://github.com/pastelnetwork/openapi/blob/3d3cbe69eaf3a1398d53f2c9e10f246b4bfd85bb/backend/app/app/models/base_ticket.py#L31
- Add Websocket
- Discuss: Do we need to do S3 / AWS?
- 

Samples:


1. **result: User should be able to get detailed result information from the result id or when getting information on requests:**

Example method:


    GET /api/v1/cascade/gateway_results/{gateway_result_id}

Example output JSON for each underlying result either queried independently or as part of its overall request:


         {
            "file": "...",
            "file_type": "...",
            "result_id": "...",
            "datetime_created": "HH:MM:SS" //datetime request generated, 24 hour UTC
            "datetime_updated": "HH:MM:SS" //datetime result data last updated / when status changes etc., 24 hour UTC
            "current_status": "string", //update with new statuses as defined above
            "retry_atttempt": "int", //should only appear if failed and retrying
            "status_message": "string", //update with new satus messages as defined above
            "registration_txid": "...", //Should be prepopulated and null until available
            "activation_txid": "...", //Should be prepopulated and null until available
            "ipfs_status": "...", // Include information on status of IPFS, separate from internal Pastel status
            "ipfs_links": "...", //should be multiple link(s), as in case of Sense there is the file itself and the dd_output; Should be prepopulated and null until available
            "aws_links": "..." //Should be prepopulated and null until available
          },


2. **Cascade: User should be able to obtain the more detailed data on the file from the Reg TXID.** 

Example method:


    GET /api/v1/cascade/pastel_ticket_data/{registration_ticket_txid}

Example output:


    {
      "pastel_id_of_user": "",
      "storag_fee": "",  
      "block_height_called": "...", //block height when reg ticket was submitted
      "block_hash_when_request_called": "",
      "dateime_called": "YYYY-MM-DD hh:mm:ss",
      "block_height_confirmed": "...", //block height when reg ticket was confirmed
      "activation_txid": "...",    
      "version": "",  
      "signatures":{"mn1":{},"mn2":{},"mn3":{}},
      "principal":{},
    }


3. **Sense: User should be able to obtain the detailed parsed output file from the Reg TXID.** 

Example method:


    GET /api/v1/sense/parsed_output_file_from_registration_txid/{registration_ticket_txid}

Existing endpoint from Jeff:


    [{
        "alternative_nsfw_scores": 
        "alternative_rare_on_internet__b64_image_strings": 
        "alternative_rare_on_internet__number_of_similar_results":
        "alternative_rare_on_internet__original_urls":
        "alternative_rare_on_internet__result_titles":
        "corresponding_pastel_blockchain_ticket_data":
        "dupe_detection_system_version":
        "hash_of_candidate_image_file": 
        "image_fingerprint_of_candidate_image_file": 
        "internet_rareness__b64_image_strings_of_in_page_matches": 
        "internet_rareness__date_strings_of_in_page_matches": 
        "internet_rareness__earliest_available_date_of_internet_results":
        "internet_rareness__min_number_of_exact_matches_in_page": 
        "internet_rareness__original_urls_of_in_page_matches": 
        "internet_rareness__result_titles_of_in_page_matches": 
        "is_likely_dupe": 
        "is_pastel_openapi_request": 
        "is_rare_on_internet": 
        "open_nsfw_score": 
        "overall_rareness_score":
        "pastel_block_hash_when_request_submitted": 
        "pastel_block_height_when_request_submitted": 
        "pastel_id_of_submitter": 
        "pct_of_top_10_most_similar_with_dupe_prob_above_25pct": 
        "pct_of_top_10_most_similar_with_dupe_prob_above_33pct": 
        "pct_of_top_10_most_similar_with_dupe_prob_above_50pct": 
        "sense_registration_ticket_txid":
        "utc_timestamp_when_request_submitted": 
    }]


