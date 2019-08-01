% abstraction layer on top of grpcbox generated module search_client.erl

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
    IndexMsg = construct_index_msg(Index),
    Msg = #{index => IndexMsg, id => Id, seq => #{seq => Seq},
        purge_seq => #{seq => PurgeSeq}},
    search_client:delete_document(Msg).

info_index(Index) ->
    IndexMsg = construct_index_msg(Index),
    search_client:info(IndexMsg).

update_index(Index, Id, Seq, PurgeSeq, Fields) ->
    IndexMsg = construct_index_msg(Index),
    Fields1 = make_fields_map(Fields),
    Msg = #{index => IndexMsg, id => Id, seq => #{seq => Seq},
        purge_seq => #{seq => PurgeSeq},fields => Fields1},
    search_client:update_document(Msg).

search_index(Index, QueryArgs) ->
    #index_query_args{grouping = Grouping} = QueryArgs,
    IndexMsg = construct_index_msg(Index),
    case Grouping#grouping.by of
        nil ->
            Msg = construct_search_msg(IndexMsg, QueryArgs),
            couch_log:notice("Msg search ~p", [Msg]),
            search_client:search(Msg);
        _ ->
            GroupMsg = construct_group_msg(IndexMsg, QueryArgs),
            search_client:group_search(GroupMsg)
    end.

%% Internal

construct_index_msg(#index{dbname = DbName, sig = Signature,
        analyzer = <<"standard">>}) ->
    Prefix= <<DbName/binary, Signature/binary>>,
    #{prefix => Prefix};
construct_index_msg(#index{dbname = DbName, sig = Signature,
        analyzer = Analyzer}) when is_binary(Analyzer) ->
    Prefix= <<DbName/binary, Signature/binary>>,
    #{prefix => Prefix, default => #{name => Analyzer}};
construct_index_msg(#index{dbname = DbName, sig = Signature,
        analyzer = {Analyzer}}) ->
    Prefix= <<DbName/binary, Signature/binary>>,
    case construct_analyzer_spec(Analyzer) of
        #{name := <<"perfield">>, stopwords := Stopwords} ->
            Fields = construct_per_fields(Analyzer),
            Default = construct_default(Analyzer, Stopwords),
            #{prefix => Prefix, default => Default, per_field => Fields};
        AnalyzerSpec ->
            couch_log:notice("AnalyzerSpec ~p ", [AnalyzerSpec]),
            #{prefix => Prefix, default => AnalyzerSpec}
    end.

construct_analyzer_spec(Analyzer) ->
    {_, Name} = lists:keyfind(<<"name">>, 1, Analyzer),
    Stopwords = case lists:keyfind(<<"stopwords">>, 1, Analyzer) of
        false ->
            [];
        {_, Words} ->
            Words
    end,
    #{name => Name, stopwords => Stopwords}.

construct_per_fields(Analyzer) ->
    Fields1 = case lists:keyfind(<<"fields">>, 1, Analyzer) of
        false ->
            [];
        {_, {Fields}} ->
            Fields
    end,
    MakeMapFun = fun
        ({Field, PerFieldAnalyzer}, Map) when is_binary(PerFieldAnalyzer) ->
            maps:put(Field, #{name => PerFieldAnalyzer}, Map);
        ({Field, {PerFieldAnalyzer}}, Map) ->
            AnalyzerSpec = construct_analyzer_spec(PerFieldAnalyzer),
            maps:put(Field, AnalyzerSpec, Map)
    end,
    lists:foldl(MakeMapFun, #{}, Fields1).

construct_default(Analyzer, Stopwords) ->
    Default = case lists:keyfind(<<"default">>, 1, Analyzer) of
        false ->
            <<"standard">>;
        {<<"default">>, Def} ->
            Def
    end,
    #{name => Default, stopwords => Stopwords}.

make_fields_map(Fields) when is_list(Fields) ->
    FieldsMapFun = fun
        ({Name, Value, {Options}}) ->
            M1 = #{name => Name, value => fields_value(Value)},
            Fields1 = options_list(Options),
            M2 = maps:from_list(Fields1),
            maps:merge(M1, M2)
    end,
    lists:map(FieldsMapFun, Fields).

fields_value(Value) when is_binary(Value) ->
    #{value => {string, Value}};
fields_value(Value) when is_number(Value) ->
    #{value => {double, Value}};
fields_value(Value) when is_boolean(Value) ->
    #{value => {bool, Value}}.

% This function is required we need to convert <<"stored">>, <<"facet">>,
% <<"analzyed">> into atoms for grpc.
options_list(Options) when is_list(Options) ->
    [{list_to_existing_atom(?b2l(Opt)), Val} || {Opt, Val} <- Options].

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
        stale => Stale,
        sort => SortArg
    },
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

% the default value would be us starting at the beginning
construct_bookmark_msg(nil) ->
    #{};
construct_bookmark_msg(Bookmark) when is_binary(Bookmark) ->
    Unpacked = binary_to_term(couch_util:decodeBase64Url(Bookmark)),
    Float = lists:nth(1, Unpacked),
    Int = lists:nth(2, Unpacked),
    #{order => [#{value => Float}, #{value => Int}]};
construct_bookmark_msg(_) ->
    throw({bad_request, "Invalid bookmark parameter supplied"}).
