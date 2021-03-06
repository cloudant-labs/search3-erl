-module(search3_response).

-export([
    bookmark_to_json/1,
    groups_to_json/3,
    hits_to_json/3,
    facets_to_json/1,

    handle_response/2,
    handle_search_response/2,
    handle_analyze_response/1
]).

hits_to_json(Db, IncludeDocs, Hits) ->
    ConvertHitsFun = fun
        (#{fields := Fields, id := Id, order := Order,
                highlights := Highlights}) ->
            Order1 = order_to_json(Order),
            Fields1 = fields_to_json(Fields),
            Highlights1 = highlights_to_json(Highlights),
            if IncludeDocs ->
                Doc = search3_util:get_doc(Db, Id),
                if Highlights =:= [] ->
                    {[{fields, {Fields1}}, {id, Id}, {order, Order1},
                        Doc]};
                true ->
                    {[{fields, {Fields1}}, {id, Id}, {order, Order1},
                         {highlights, {Highlights1}}, Doc]}
                end;
            true ->
                if Highlights =:= [] ->
                    {[{fields, {Fields1}}, {id, Id}, {order, Order1}]};
                true ->
                    {[{fields, {Fields1}}, {id, Id}, {order, Order1},
                        {highlights, {Highlights1}}]}
                end
            end
    end,
    lists:map(ConvertHitsFun, Hits).

groups_to_json(Db, IncludeDocs, Groups) when is_list(Groups) ->
    ConvertGroupFun = fun
        (#{by := By, matches := Matches, hits := Hits}) ->
            Hits1 = hits_to_json(Db, IncludeDocs, Hits),
            {[{by, By}, {total_rows, Matches}, {rows, Hits1}]}
    end,
    lists:map(ConvertGroupFun, Groups).

fields_to_json([]) ->
    [];
fields_to_json(Fields) when is_list(Fields) ->
    ConvertFieldsFun = fun
        (#{name := Name, value := Value}) ->
            #{value := {_Type, BinValue}} = Value,
            {Name, BinValue}
    end,
    lists:map(ConvertFieldsFun, Fields).

bookmark_to_json(<<>>) ->
    [];
bookmark_to_json(Bookmark) ->
    #{order := Order} = Bookmark,
    Bin = term_to_binary(extract_order(Order)),
    couch_util:encodeBase64Url(Bin).

extract_order(Order) ->
    ExtractOrderFun = fun (Ord) ->
        #{value := Val} = Ord,
        Val
    end,
    lists:map(ExtractOrderFun, Order).

order_to_json(Order) ->
    OrderFun = fun
        (#{value := Val}) ->
            {_Type, Val1} = Val,
            Val1;
        (Ord) when map_size(Ord) == 0 ->
            null
    end,
    lists:map(OrderFun, Order).

facets_to_json(Counts) ->
    CountsList = maps:to_list(Counts),
    CountsFun = fun
        ({K, V}) when is_binary(K), is_map(V) ->
            #{counts := SubCounts} = V,
            SubCountsList = maps:to_list(SubCounts),
            {K, {SubCountsList}}
    end,
    {lists:map(CountsFun, CountsList)}.

highlights_to_json([]) ->
    [];
highlights_to_json(Fields) when is_list(Fields) ->
    ConvertHighLightsFun = fun
        (#{fieldname := Name, highlights := Highlights}) ->
            {Name, Highlights}
    end,
    lists:map(ConvertHighLightsFun, Fields).

handle_search_response({ok, #{groups := Groups, matches := Matches,
        session := Session}, _}, BuildSession) ->
    verify_same_session(BuildSession, Session),
    {group_search, Matches, Groups};
handle_search_response({ok, Response, _Header}, BuildSession) ->
    #{
        matches := Matches,
        hits := Hits,
        session := Session
    } = Response,
    verify_same_session(BuildSession, Session),
    Bookmark = maps:get(bookmark, Response, <<>>),
    Counts = maps:get(counts, Response, undefined),
    Ranges = maps:get(ranges, Response, undefined),
    {search, Bookmark, Matches, Hits, Counts, Ranges};
handle_search_response({error, Error}, _) ->
    handle_error_response({error, Error}).

handle_analyze_response({ok,  #{tokens := Tokens}, _}) ->
    {ok, Tokens};
handle_analyze_response({error, Error}) ->
    handle_error_response({error, Error}).

% Handles generic responses for now to simply verify session is the same
% Can update later for more specific handling
handle_response({ok, #{session := Session} = Response, _},
        CurrentSession) ->
    VSession = verify_same_session(CurrentSession, Session),
    {VSession, Response};
handle_response({error, Error}, _) ->
    handle_error_response({error, Error}).


handle_error_response({error, {'CLIENT_ERROR', Reason}}) ->
    throw({bad_request, Reason});
handle_error_response({error, {'SESSION_MISMATCH', _Reason}}) ->
    throw(session_mismatch);
handle_error_response({error, {'SERVER_ERROR', Reason}}) ->
    erlang:error({server_error, Reason});
handle_error_response({error, {'UNKNOWN', Reason}}) ->
    erlang:error({unknown, Reason});
handle_error_response(Error) ->
    erlang:error(Error).

% Session Verification
verify_same_session(<<>>, Received) ->
    Received;
verify_same_session(Sent, Received) when is_binary(Sent), is_binary(Received) ->
    case Sent =:= Received of
        true -> Received;
        _ -> throw(session_mismatch)
    end;
verify_same_session(_, _) ->
    throw(session_mismatch).
