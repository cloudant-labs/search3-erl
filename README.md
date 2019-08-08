Search3 - Erlang
=====

This is the erlang component of a new text search application built on top
of FoundationDB. The java component resides at:

https://github.com/cloudant-labs/search3-java

The two components communicate via gRPC. This erlang application is a client
and sends messages to the java component which acts as the server. Messages
are sent via generated stubs using https://github.com/tsloughter/grpcbox.

FoundationDB, search3-erl, and search3-java all must be up in order for text
search to work.

Currently, we are in the beta version, and we support:

1) Basic Field Search
2) Sorting by string and number.
3) Bookmarks
4) Limit
5) Group Search
6) Different default Analyzers and per_field Analyzers

Code layout:

* `search3.proto` - This is the grpc proto file. We use this to generate
    search_client.
* `search3_httpd` - This handles the _search endpoint
* `search3_epi` - Hooks into couch_epi plugin 
* `search3_httpd` - Handles _search endpoint
* `search3_httpd_handlers` - Httpd handler for search3_epi
* `search3_indexer` - All indexing capabilities reside in this module.
* `search3_jobs` - Hooks indexing into the new couch_jobs system.
* `search3_response` - Parses grpc message from server and formats it into
    json.
* `search3_rpc` - Layer on top of search_client, which is the generated stub
    code from grpcbox.
* `search3_sup` - Generic supervisor for search3_worker_manager
* `search3_util` - Utility functions
* `search3_worker_manager` - spawns search3_indexer processes for indexing
