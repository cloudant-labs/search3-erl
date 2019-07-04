% abstraction layer on top of gRPC generated module
-module(search3_rpc).

-include_lib("couch/include/couch_db.hrl").
-include("search3.hrl").

-export([
    get_update_seq/1, 
    delete_index/1,
    info_index/1,
    set_update_seq/2,
    update_index/3,
    search_index/2
    ]).

get_update_seq(Index) ->
    Prefix = get_index_prefix(Index),
    {ok, SeqMap, _} = search_client:get_update_sequence(Prefix),
    #{seq := Seq} = SeqMap,
    Seq.

% Not Tested
delete_index(Index) ->
    Prefix = get_index_prefix(Index),
    search_client:delete(Prefix).

% Not Tested
info_index(Index) ->
    Prefix = get_index_prefix(Index),
    search_client:info(Prefix).


set_update_seq(Index, Seq) ->
    Prefix = get_index_prefix(Index),
    search_client:set_update_sequence(#{index => Prefix, seq => Seq}).


update_index(Index, Id, Fields) ->
    Prefix = get_index_prefix(Index),
    Fields1 = make_fields_map(Fields),
    search_client:update_document(#{index => Prefix, id => Id, fields => Fields1}).

search_index(Index, Query) ->
    Prefix = get_index_prefix(Index),
    Query1 = binary_to_list(Query),
    search_client:search(#{index => Prefix, query => Query}).


% TODO:
% 1) Read operations
% rpc GroupSearch(GroupSearchRequest) returns (GroupSearchResponse);

%% Internal

get_index_prefix(#index{dbname = DbName, sig = Signature}) ->
    Prefix= <<DbName/binary, Signature/binary>>,
    #{prefix => Prefix}.


make_fields_map(Fields) when is_list(Fields) ->
    % need to figure what the third element holds and if we need to hold it
    FieldsMapFun = fun
        ({Name, Value, _}) when is_binary(Value) ->
            #{name => Name, value => #{value => {string, binary_to_list(Value)}}};
        ({Name, Value, _}) when is_number(Value) ->
            #{name => Name, value => #{value => {double, Value}}};
        ({Name, Value, _}) when is_boolean(Value) ->
            #{name => Name, value => #{value => {bool, Value}}}
    end,
    lists:map(FieldsMapFun, Fields).
