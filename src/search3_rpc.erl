% abstraction layer on top of gRPC generated module
-module(search3_rpc).

-include_lib("couch/include/couch_db.hrl").
-include("search3.hrl").

-export([
    get_update_seq/1, 
    delete_index/4,
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

delete_index(Index, Id, Seq, PurgeSeq) ->
    Prefix = get_index_prefix(Index),
    search_client:delete_document(#{index => Prefix, id => Id,
        seq => #{seq => Seq}, purge_seq => #{seq => PurgeSeq}}).

info_index(Index) ->
    Prefix = get_index_prefix(Index),
    search_client:info(Prefix).

update_index(Index, Id, Seq, PurgeSeq, Fields) ->
    Prefix = get_index_prefix(Index),
    Fields1 = make_fields_map(Fields),
    search_client:update_document(#{index => Prefix, id => Id,
        seq => #{seq => Seq}, purge_seq => #{seq => PurgeSeq}, fields => Fields1}).

search_index(Index, QueryArgs) ->
    #index_query_args{grouping = Grouping} = QueryArgs,
    Prefix = get_index_prefix(Index),
    case Grouping#grouping.by of
        nil ->
            Msg = construct_search_msg(Prefix, QueryArgs),
            search_client:search(Msg);
        _ ->
            Msg2 = construct_group_msg(Prefix, QueryArgs),
            search_client:group_search(Msg2)
    end.

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
    SortArg = construct_sort_msg(Sort),
    Msg = #{
        index => Prefix,
        query => Query1,
        limit => Limit,
        % TODO: test later
        stale => Stale,
        sort => SortArg
        % this even an option anymore?
        % partition => Partition
        % Test these individually
        % counts => Counts
        % ranges => Ranges
        % drilldown => DrillDown,
        % include_fields => IncludeFields
    },

    % Need to figure out the actual default value for Bookmark
    case construct_bookmark_msg(Bookmark) of
        #{} -> ok;
        Bookmark1 -> maps:put(bookmark, Bookmark1, Msg)
    end,
    Msg.

construct_group_msg(Prefix, #index_query_args{}=QueryArgs) ->
    #index_query_args{
        q = Query,
        limit = Limit,
        bookmark = Bookmark,
        stale = Stale,
        grouping = Grouping
    } = QueryArgs,
    #grouping{
        by = GroupBy,
        offset = GroupOffSet,
        limit = GroupLimit,
        sort = GroupSort
    } = Grouping,
    Query1 = binary_to_list(Query),
    SortArg = construct_sort_msg(GroupSort),
    Msg = #{
        index => Prefix,
        query => Query1,
        limit => Limit,
        stale => Stale,
        group_by => GroupBy,
        group_offset => GroupOffSet,
        group_limit => GroupLimit,
        group_sort => SortArg
    }.

construct_sort_msg(SortArg) when is_binary(SortArg) ->
    #{fields => [SortArg]};
construct_sort_msg(SortArg) when is_list(SortArg) ->
    #{fields => SortArg};
construct_sort_msg(_) ->
    #{}.

construct_bookmark_msg(Bookmark) when is_binary(Bookmark) ->
    Unpacked = binary_to_term(couch_util:decodeBase64Url(Bookmark)),
    Float = lists:nth(1, Unpacked),
    Int = lists:nth(2, Unpacked),
    #{order => [#{value => Float}, #{value => Int}]};
% the default value would be us starting at the beginning
construct_bookmark_msg(_) ->
    #{}.
