-module(search3_response).

-export([
    bookmark_to_json/1,
    groups_to_json/3,
    hits_to_json/3
]).

hits_to_json(Db, IncludeDocs, Hits) ->
    ConvertHitsFun = fun
        (#{fields := Fields, id := Id, order := Order}) ->
            Order1 = order_to_json(Order),
            Fields1 = fields_to_json(Fields),
            if IncludeDocs ->
                Doc = search3_util:get_doc(Db, Id),
                {[{fields, Fields1}, {id, Id}, {order, {Order1}}, Doc]};
            true ->
                {[{fields, Fields1}, {id, Id}, {order, {Order1}}]}
            end
    end,
    ConvertedHits = lists:map(ConvertHitsFun, Hits),
    {[{hits, ConvertedHits}]}.

groups_to_json(Db, IncludeDocs, Groups) when is_list(Groups) ->
    ConvertGroupFun = fun
        (#{by := By, matches := Matches, hits := Hits}) ->
            {Hits1} = hits_to_json(Db, IncludeDocs, Hits),
            {[{by, By}, {matches, Matches} | Hits1]}
    end,
    lists:map(ConvertGroupFun, Groups).

fields_to_json([]) ->
    [];
fields_to_json(Fields) when is_list(Fields) ->
    ConvertFieldsFun = fun
        (#{name := Name, value := Value}) ->
            #{value := {_Type, BinValue}} = Value,
            {[{Name, BinValue}]}
    end,
    lists:map(ConvertFieldsFun, Fields).

bookmark_to_json(<<>>) ->
    [];
bookmark_to_json(Bookmark) ->
    #{order := Order} = Bookmark,
    Bin = term_to_binary(order_to_json(Order)),
    couch_util:encodeBase64Url(Bin).

order_to_json(Order) ->
    GetOrderFun = fun (Ord) ->
        #{value := Val} = Ord,
        Val
    end,
    lists:map(GetOrderFun, Order).
