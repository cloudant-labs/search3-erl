-module(search3_httpd_handlers).

-export([
    url_handler/1,
    db_handler/1,
    design_handler/1
]).

url_handler(<<"_search_analyze">>) -> fun search3_httpd:handle_analyze_req/1;
url_handler(_) -> no_match.

design_handler(<<"_search">>) -> fun search3_httpd:handle_search_req/3;
design_handler(_) -> no_match.

db_handler(_) -> no_match.

