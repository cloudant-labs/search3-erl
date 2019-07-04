-module(search3_httpd).

-export([handle_search_req/3]).

-include("search3.hrl").
-include_lib("couch/include/couch_db.hrl").

-import(chttpd, [
    send_method_not_allowed/2,
    send_json/2,
    send_json/3,
    send_error/2
]).

% TODO:
% 1) Group Search
% 2) Pass in Bookmarks

handle_search_req(Req, Db, DDoc) ->
    handle_search_req(Req, Db, DDoc, 0, 500).

handle_search_req(#httpd{method=Method, path_parts=[_, _, _, _, IndexName]}=Req
                  ,Db, DDoc, _, _)
  when Method == 'GET'; Method == 'POST' ->
    DbName = fabric2_db:name(Db),
    QueryArgs = parse_index_params(Req, Db),
    validate_search_restrictions(Db, DDoc, QueryArgs),
    {Bookmark, Matches, Hits} = search3_query:run_query(Db, DDoc,
        IndexName, QueryArgs),
    Hits1 = hits_to_json(DbName, false, Hits),
    Bookmark1 = bookmark_to_json(Bookmark),
    send_json(Req, 200, {[
        {total_rows, Matches},
        {bookmark, {Bookmark1}},
        {rows, Hits1}
]});

handle_search_req(#httpd{path_parts=[_, _, _, _, _]}=Req, _Db, _DDoc, _RetryCount, _RetryPause) ->
    send_method_not_allowed(Req, "GET,POST");
handle_search_req(Req, _Db, _DDoc, _RetryCount, _RetryPause) ->
    send_error(Req, {bad_request, "path not recognized"}).

parse_index_params(#httpd{method='GET'}=Req, Db) ->
    IndexParams = lists:flatmap(fun({K, V}) -> parse_index_param(K, V) end,
        chttpd:qs(Req)),
    parse_index_params(IndexParams, Db);
parse_index_params(#httpd{method='POST'}=Req, Db) ->
    {JsonBody} = chttpd:json_body_obj(Req),
    QSEntry = case chttpd:qs_value(Req, "partition") of
        undefined -> [];
        StrVal -> [{<<"partition">>, ?l2b(StrVal)}]
    end,
    IndexParams = lists:flatmap(fun({K, V}) ->
        parse_json_index_param(K, V)
    end, QSEntry ++ JsonBody),
    parse_index_params(IndexParams, Db);
parse_index_params(IndexParams, _) ->
    DefaultLimit = list_to_integer(config:get("search3", "limit", "25")),
    Args = #index_query_args{limit=DefaultLimit},
    lists:foldl(fun({K, V}, Args2) ->
        validate_index_query(K, V, Args2)
    end, Args, IndexParams).

validate_index_query(q, Value, Args) ->
    Args#index_query_args{q=Value};
validate_index_query(stale, Value, Args) ->
    Args#index_query_args{stale=Value};
validate_index_query(limit, Value, Args) ->
    Args#index_query_args{limit=Value};
validate_index_query(include_docs, Value, Args) ->
    Args#index_query_args{include_docs=Value};
validate_index_query(include_fields, Value, Args) ->
    Args#index_query_args{include_fields=Value};
validate_index_query(bookmark, Value, Args) ->
    Args#index_query_args{bookmark=Value};
validate_index_query(sort, Value, Args) ->
    Args#index_query_args{sort=Value}.

parse_index_param("", _) ->
    [];
parse_index_param("q", Value) ->
    [{q, ?l2b(Value)}];
parse_index_param("query", Value) ->
    [{q, ?l2b(Value)}];
parse_index_param("bookmark", Value) ->
    [{bookmark, ?l2b(Value)}];
parse_index_param("sort", Value) ->
    [{sort, ?JSON_DECODE(Value)}];
parse_index_param("limit", Value) ->
    [{limit, ?JSON_DECODE(Value)}].

parse_json_index_param(<<"q">>, Value) ->
    [{q, Value}];
parse_json_index_param(<<"query">>, Value) ->
    [{q, Value}];
parse_json_index_param(<<"bookmark">>, Value) ->
    [{bookmark, Value}];
parse_json_index_param(<<"sort">>, Value) ->
    [{sort, Value}];
parse_json_index_param(<<"limit">>, Value) ->
    [{limit, Value}].

validate_search_restrictions(_Db, _DDoc, Args) ->
    #index_query_args{
        q = Query
    } = Args,
    case Query of
        undefined ->
            Msg1 = <<"Query must include a 'q' or 'query' argument">>,
            throw({query_parse_error, Msg1});
        _ ->
            ok
    end.

hits_to_json(_DbName, _IncludeDocs, Hits) ->
    ConvertHitsFun = fun
        (#{fields := Fields, id := Id, order := Order}) ->
            Order1 = order_to_json(Order),
            {[{fields, Fields}, {id, Id}, {order, {Order1}}]}
    end,
    ConvertedHits = lists:map(ConvertHitsFun, Hits),
    {[{hits, ConvertedHits}]}.

bookmark_to_json(Bookmark) ->
    #{order := Order} = Bookmark,
    order_to_json(Order).

order_to_json(Order) ->
    GetOrderFun = fun (Ord) ->
        #{value := Val} = Ord,
        Val
    end,
    lists:map(GetOrderFun, Order).
