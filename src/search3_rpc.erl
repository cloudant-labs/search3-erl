% abstraction layer on top of gRPC generated module
-module(search3_rpc).

-include_lib("couch/include/couch_db.hrl").
-include("search3.hrl").

-export([
    get_update_seq/1, 
    delete_index/1,
    info_index/1,
    update_index/5,
    search_index/2
    ]).

get_update_seq(Index) ->
    {ok, Response, _Headers} = info_index(Index),
    #{
        committed_seq := CommittedSeq
    } = Response,
    case CommittedSeq of
        <<>> -> 0;
        Seq -> Seq
    end.

% Not Tested
delete_index(Index) ->
    Prefix = get_index_prefix(Index),
    search_client:delete(Prefix).

% Not Tested
info_index(Index) ->
    Prefix = get_index_prefix(Index),
    search_client:info(Prefix).

update_index(Index, Id, Seq, PurgeSeq, Fields) ->
    Prefix = get_index_prefix(Index),
    Fields1 = make_fields_map(Fields),
    search_client:update_document(#{index => Prefix, id => Id,
        seq => #{seq => Seq}, purge_seq => #{seq => PurgeSeq}, fields => Fields1}).

search_index(Index, QueryArgs) ->
    Prefix = get_index_prefix(Index),
    Msg = construct_search_msg(Prefix, QueryArgs),
    search_client:search(Msg).


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

construct_search_msg(Prefix, #index_query_args{}=QueryArgs) ->
    #index_query_args{
        q = Query,
        limit = Limit,
        bookmark = Bookmark,
        stale = Stale,
        sort = Sort,
        partition = Partition,
        counts = Counts,
        ranges = Ranges,
        drilldown = DrillDown,
        include_fields = IncludeFields
    } = QueryArgs,
    Query1 = binary_to_list(Query),
    couch_log:notice("Bookmark ~p ", [Bookmark]),
    SortArg = construct_sort_msg(Sort),
    Bookmark1 = construct_bookmark_msg(Bookmark),
    #{
        index => Prefix,
        query => Query1,
        limit => Limit,
        % TODO: test later
        bookmark => Bookmark1,
        stale => Stale,
        sort => SortArg
        % this even an option anymore?
        % partition => Partition
        % Test these individually
        % counts => Counts
        % ranges => Ranges
        % drilldown => DrillDown,
        % include_fields => IncludeFields
    }.

construct_sort_msg(relevance) ->
    #{};
construct_sort_msg(SortArg) when is_binary(SortArg) ->
    #{fields => [SortArg]};
construct_sort_msg(SortArg) when is_list(SortArg) ->
    #{fields => SortArg};
construct_sort_msg(_) ->
    #{}.

construct_bookmark_msg(Bookmark) when is_list(Bookmark) ->
    Float = lists:nth(1, Bookmark),
    Int = lists:nth(2, Bookmark),
    #{order => [#{value => {float, Float}}, #{value => {int, Int}}]};
construct_bookmark_msg(_) ->
    #{order => []}.
