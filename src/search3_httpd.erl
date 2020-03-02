-module(search3_httpd).

-export([handle_search_req/3,
    handle_analyze_req/1,
    handle_cleanup_req/2]).

-include("search3.hrl").
-include_lib("couch/include/couch_db.hrl").

-import(chttpd, [
    send_method_not_allowed/2,
    send_json/2,
    send_json/3,
    send_error/2,
    validate_ctype/2
]).

handle_search_req(Req, Db, DDoc) ->
    handle_search_req(Req, Db, DDoc, 0, 500).

handle_search_req(#httpd{method=Method, path_parts=[_, _, _, _, IndexName]}=Req
                  ,Db, DDoc, _, _)
  when Method == 'GET'; Method == 'POST' ->
    QueryArgs = #index_query_args{
        include_docs = IncludeDocs
    } = parse_index_params(Req, Db),
    validate_search_restrictions(Db, DDoc, QueryArgs),
    case search3_query:run_query(Db, DDoc, IndexName, QueryArgs) of
        {group_search, Matches, Groups} ->
            GroupsJson = search3_response:groups_to_json(Db, IncludeDocs,
                Groups),
            send_json(Req, 200, {[
                {total_rows, Matches},
                {groups, GroupsJson}
            ]});
        {search, Bookmark, Matches, Hits, Counts0, Ranges0} ->
            Hits1 = search3_response:hits_to_json(Db, IncludeDocs, Hits),
            Bookmark1 = search3_response:bookmark_to_json(Bookmark),
            Counts = case Counts0 of
                undefined ->
                    [];
                _ ->
                    [{counts, search3_response:facets_to_json(Counts0)}]
            end,
            Ranges = case Ranges0 of
                undefined ->
                    [];
                _ ->
                    [{ranges, search3_response:facets_to_json(Ranges0)}]
            end,
            send_json(Req, 200, {[
                {total_rows, Matches},
                {bookmark, Bookmark1},
                {rows, Hits1}
            ] ++ Counts ++ Ranges
            })
    end;

handle_search_req(#httpd{path_parts=[_, _, _, _, _]}=Req, _Db, _DDoc,
        _RetryCount, _RetryPause) ->
    send_method_not_allowed(Req, "GET,POST");
handle_search_req(Req, _Db, _DDoc, _RetryCount, _RetryPause) ->
    send_error(Req, {bad_request, "path not recognized"}).

handle_analyze_req(#httpd{method='GET'}=Req) ->
    Analyzer = couch_httpd:qs_value(Req, "analyzer"),
    Text = couch_httpd:qs_value(Req, "text"),
    analyze(Req, Analyzer, Text);
handle_analyze_req(#httpd{method='POST'}=Req) ->
    chttpd:validate_ctype(Req, "application/json"),
    {Fields} = chttpd:json_body_obj(Req),
    Analyzer = couch_util:get_value(<<"analyzer">>, Fields),
    Text = couch_util:get_value(<<"text">>, Fields),
    analyze(Req, Analyzer, Text);
handle_analyze_req(Req) ->
    send_method_not_allowed(Req, "GET,POST").

handle_cleanup_req(#httpd{method='POST'}=Req, Db) ->
    chttpd:validate_ctype(Req, "application/json"),
    Tries = config:get_integer("search3", "index_cleanup_attempts", 10),
    ok = search3_cleanup:clear_unreachable_indexes(Db, Tries),
    send_json(Req, 202, {[{ok, true}]});
handle_cleanup_req(Req, _Db) ->
    send_method_not_allowed(Req, "POST").

analyze(Req, Analyzer, Text) ->
    validate_string_or_object(Analyzer),
    validate_string(Text),
    Response = search3_rpc:analyze(Analyzer, Text),
    case search3_response:handle_analyze_response(Response) of
        {ok, Tokens} ->
            send_json(Req, 200, {[{tokens, Tokens}]});
        {error, Reason} ->
            send_error(Req, Reason)
    end.

validate_string_or_object(Analyzer) ->
    case Analyzer of
        undefined ->
            throw({bad_request, "analyzer parameter is mandatory"});
        A1 when is_list(A1) ->
            ok;
        A2 when is_binary(A2) ->
            ok;
        {[_|_]} ->
            ok;
        {[]} ->
            throw({bad_request, "analyzer parameter must be not be empty object"});
        _ ->
            throw({bad_request, "analyzer parameter must be a string or an object"})
    end.

validate_string(Text) ->
    case Text of
        undefined ->
            throw({bad_request, "text parameter is mandatory"});
        T1 when is_list(T1) ->
            ok;
        T2 when is_binary(T2) ->
            ok;
        _ ->
            throw({bad_request, "text parameter must be a string"})
    end.

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
    [{limit, ?JSON_DECODE(Value)}];
parse_index_param("include_fields", Value) ->
    [{include_fields, ?JSON_DECODE(Value)}];
parse_index_param("include_docs", Value) ->
    [{include_docs, parse_bool_param("include_docs", Value)}];
parse_index_param("group_field", Value) ->
    [{group_field, ?l2b(Value)}];
parse_index_param("group_sort", Value) ->
    [{group_sort, ?JSON_DECODE(Value)}];
parse_index_param("group_limit", Value) ->
    [{group_limit, parse_positive_int_param("group_limit", Value,
        "max_group_limit", "200")}];
parse_index_param("counts", Value) ->
    [{counts, ?JSON_DECODE(Value)}];
parse_index_param("ranges", Value) ->
    [{ranges, ?JSON_DECODE(Value)}];
parse_index_param("drilldown", Value) ->
    [{drilldown, ?JSON_DECODE(Value)}];
parse_index_param("highlight_fields", Value) ->
    [{highlight_fields, ?JSON_DECODE(Value)}];
parse_index_param("highlight_pre_tag", Value) ->
    [{highlight_pre_tag, ?JSON_DECODE(Value)}];
parse_index_param("highlight_post_tag", Value) ->
    [{highlight_post_tag, ?JSON_DECODE(Value)}];
parse_index_param("highlight_number", Value) ->
    [{highlight_number, validate_positive_int("highlight_number", Value)}];
parse_index_param("highlight_size", Value) ->
    [{highlight_size, validate_positive_int("highlight_size", Value)}];
parse_index_param(Key, Value) ->
    [{extra, {Key, Value}}].

parse_json_index_param(<<"q">>, Value) ->
    [{q, Value}];
parse_json_index_param(<<"query">>, Value) ->
    [{q, Value}];
parse_json_index_param(<<"bookmark">>, Value) ->
    [{bookmark, Value}];
parse_json_index_param(<<"sort">>, Value) ->
    [{sort, Value}];
parse_json_index_param(<<"limit">>, Value) ->
    [{limit, Value}];
parse_json_index_param(<<"include_fields">>, Value) ->
    [{include_fields, Value}];
parse_json_index_param(<<"include_docs">>, Value) when is_boolean(Value) ->
    [{include_docs, Value}];
parse_json_index_param(<<"group_field">>, Value) ->
    [{group_field, Value}];
parse_json_index_param(<<"group_sort">>, Value) ->
    [{group_sort, Value}];
parse_json_index_param(<<"group_limit">>, Value) ->
    [{group_limit, parse_positive_int_param("group_limit", Value,
        "max_group_limit", "200")}];
parse_json_index_param(<<"counts">>, Value) ->
    [{counts, Value}];
parse_json_index_param(<<"ranges">>, Value) ->
    [{ranges, Value}];
parse_json_index_param(<<"drilldown">>, Value) ->
    [{drilldown, Value}];
parse_json_index_param(<<"highlight_fields">>, Value) ->
    [{highlight_fields, Value}];
parse_json_index_param(<<"highlight_pre_tag">>, Value) ->
    [{highlight_pre_tag, Value}];
parse_json_index_param(<<"highlight_pos_tag">>, Value) ->
    [{highlight_post_tag, Value}];
parse_json_index_param(<<"highlight_number">>, Value) ->
    [{highlight_number, validate_positive_int("highlight_number", Value)}];
parse_json_index_param(<<"highlight_size">>, Value) ->
    [{highlight_size, validate_positive_int("highlight_size", Value)}];
parse_json_index_param(Key, Value) ->
    [{extra, {Key, Value}}].

parse_bool_param(_, Val) when is_boolean(Val) ->
    Val;
parse_bool_param(_, "true") -> true;
parse_bool_param(_, "false") -> false;
parse_bool_param(Name, Val) ->
    Msg = io_lib:format("Invalid value for ~s: ~p", [Name, Val]),
    throw({query_parse_error, ?l2b(Msg)}).

parse_int_param(_, Val) when is_integer(Val) ->
    Val;
parse_int_param(Name, Val) ->
    case (catch list_to_integer(Val)) of
    IntVal when is_integer(IntVal) ->
        IntVal;
    _ ->
        Msg = io_lib:format("Invalid value for ~s: ~p", [Name, Val]),
        throw({query_parse_error, ?l2b(Msg)})
    end.

parse_positive_int_param(Name, Val, Prop, Default) ->
    MaximumVal = list_to_integer(
        config:get("search3", Prop, Default)),
    case parse_int_param(Name, Val) of
    IntVal when IntVal > MaximumVal ->
        Fmt = "Value for ~s is too large, must not exceed ~p",
        Msg = io_lib:format(Fmt, [Name, MaximumVal]),
        throw({query_parse_error, ?l2b(Msg)});
    IntVal when IntVal > 0 ->
        IntVal;
    IntVal when IntVal =< 0 ->
        Fmt = "~s must be greater than zero",
        Msg = io_lib:format(Fmt, [Name]),
        throw({query_parse_error, ?l2b(Msg)});
    _ ->
        Fmt = "Invalid value for ~s: ~p",
        Msg = io_lib:format(Fmt, [Name, Val]),
        throw({query_parse_error, ?l2b(Msg)})
    end.

validate_positive_int(Name, Val) ->
    case parse_int_param(Name, Val) of
    IntVal when IntVal > 0 ->
        IntVal;
    IntVal when IntVal =< 0 ->
        Fmt = "~s must be greater than zero",
        Msg = io_lib:format(Fmt, [Name]),
        throw({query_parse_error, ?l2b(Msg)});
    _ ->
        Fmt = "Invalid value for ~s: ~p",
        Msg = io_lib:format(Fmt, [Name, Val]),
        throw({query_parse_error, ?l2b(Msg)})
    end.

validate_index_query(q, Value, Args) ->
    Args#index_query_args{q=Value};
validate_index_query(stale, Value, Args) ->
    Args#index_query_args{stale=Value};
validate_index_query(limit, Value, Args) ->
    Args#index_query_args{limit=Value};
validate_index_query(group_field, Value, #index_query_args{grouping=Grouping}=Args) ->
    Args#index_query_args{grouping=Grouping#grouping{by=Value, new_api=true}};
validate_index_query(group_sort, Value, #index_query_args{grouping=Grouping}=Args) ->
    Args#index_query_args{grouping=Grouping#grouping{sort=Value}};
validate_index_query(group_limit, Value, #index_query_args{grouping=Grouping}=Args) ->
    Args#index_query_args{grouping=Grouping#grouping{limit=Value}};
validate_index_query(counts, Value, Args) ->
    Args#index_query_args{counts=Value};
validate_index_query(ranges, Value, Args) ->
    Args#index_query_args{ranges=Value};
validate_index_query(drilldown, Value, Args) ->
    DrillDown = Args#index_query_args.drilldown,
    Args#index_query_args{drilldown=[Value|DrillDown]};
validate_index_query(include_docs, Value, Args) ->
    Args#index_query_args{include_docs=Value};
validate_index_query(include_fields, Value, Args) ->
    Args#index_query_args{include_fields=Value};
validate_index_query(bookmark, Value, Args) ->
    Args#index_query_args{bookmark=Value};
validate_index_query(sort, Value, Args) ->
    Args#index_query_args{sort=Value};
validate_index_query(highlight_fields, Value, Args) ->
    Args#index_query_args{highlight_fields=Value};
validate_index_query(highlight_pre_tag, Value, Args) ->
    Args#index_query_args{highlight_pre_tag=Value};
validate_index_query(highlight_post_tag, Value, Args) ->
    Args#index_query_args{highlight_post_tag=Value};
validate_index_query(highlight_number, Value, Args) ->
    Args#index_query_args{highlight_number=Value};
validate_index_query(highlight_size, Value, Args) ->
    Args#index_query_args{highlight_size=Value};
validate_index_query(extra, _Value, Args) ->
    Args.

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