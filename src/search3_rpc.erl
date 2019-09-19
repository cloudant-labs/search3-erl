% abstraction layer on top of grpcbox generated module search_client.erl

-module(search3_rpc).

-include_lib("couch/include/couch_db.hrl").
-include("search3.hrl").

-export([
    get_update_seq/1, 
    delete_index/4,
    info_index/1,
    update_index/5,
    search_index/2,
    set_update_seq/3,
    get_channel/0
    ]).

get_update_seq(Index) ->
    {Session, Resp} = info_index(Index),
    PendingSeq = maps:get(pending_seq, Resp, <<>>),
    CommittedSeq = maps:get(committed_seq, Resp, <<>>),

    UpdateSeq = case {PendingSeq, CommittedSeq} of
        {<<>>, <<>>} -> 0;
        {<<>>, C} when is_map(C) -> maps:get(seq, C);
        {P, _} when is_map(P) -> maps:get(seq, P);
        {_, _} -> 0
    end,
    {Session, UpdateSeq}.

set_update_seq(#index{session = Session} = Index, Seq, _PurgeSeq) ->
    IndexMsg = construct_index_msg(Index),
    Msg = #{
        index => IndexMsg,
        seq => #{seq => Seq}
    },
    Resp = search_client:set_update_sequence(Msg, get_channel()),
    search3_response:handle_response(Resp, Session).

delete_index(#index{session = Session} = Index, Id, Seq, PurgeSeq) ->
    IndexMsg = construct_index_msg(Index),
    Msg = #{
        index => IndexMsg,
        id => Id,
        seq => #{seq => Seq},
        purge_seq => #{seq => PurgeSeq}
    },
    Resp = search_client:delete_document(Msg, get_channel()),
    search3_response:handle_response(Resp, Session).

info_index(#index{session = Session} = Index) ->
    IndexMsg = construct_index_msg(Index),
    Resp = search_client:info(IndexMsg, get_channel()),
    search3_response:handle_response(Resp, Session).

update_index(#index{session = Session} = Index, Id, Seq, PurgeSeq, Fields) ->
    IndexMsg = construct_index_msg(Index),
    Fields1 = make_fields_map(Fields),
    Msg = #{
        index => IndexMsg,
        id => Id,
        seq => #{seq => Seq},
        purge_seq => #{seq => PurgeSeq},
        fields => Fields1
    },
    Resp = search_client:update_document(Msg, get_channel()),
    search3_response:handle_response(Resp, Session).

search_index(Index, QueryArgs) ->
    #index_query_args{grouping = Grouping} = QueryArgs,
    IndexMsg = construct_index_msg(Index),
    case Grouping#grouping.by of
        nil ->
            Msg = construct_search_msg(IndexMsg, QueryArgs),
            search_client:search(Msg, get_channel());
        _ ->
            GroupMsg = construct_group_msg(IndexMsg, QueryArgs),
            search_client:group_search(GroupMsg, get_channel())
    end.

%% Internal

construct_index_msg(#index{dbname = DbName, sig = Signature,
        analyzer = <<"standard">>, session = Session}) ->
    Prefix= <<DbName/binary, Signature/binary>>,
    #{prefix => Prefix, session => Session};
construct_index_msg(#index{dbname = DbName, sig = Signature,
        analyzer = Analyzer, session = Session}) when is_binary(Analyzer) ->
    Prefix= <<DbName/binary, Signature/binary>>,
    #{prefix => Prefix, session => Session, default => #{name => Analyzer}};
construct_index_msg(#index{dbname = DbName, sig = Signature,
        analyzer = {Analyzer}, session= Session}) ->
    Prefix= <<DbName/binary, Signature/binary>>,
    case construct_analyzer_spec(Analyzer) of
        #{name := <<"perfield">>, stopwords := Stopwords} ->
            Fields = construct_per_fields(Analyzer),
            Default = construct_default(Analyzer, Stopwords),
            #{prefix => Prefix, session => Session,
                default => Default, per_field => Fields};
        AnalyzerSpec ->
            #{prefix => Prefix, session => Session, default => AnalyzerSpec}
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
            Applied = apply_options(#{name => Name, value => fields_value(Value)},
                Options),
            Applied
    end,
    lists:map(FieldsMapFun, Fields).

fields_value(Value) when is_binary(Value) ->
    #{value => {string, Value}};
fields_value(Value) when is_number(Value) ->
    #{value => {double, Value}};
fields_value(Value) when is_boolean(Value) ->
    #{value => {bool, Value}}.

% This function is required we need to convert <<"store">>, <<"facet">>,
% <<"analzyed">> into atoms for grpc.
apply_options(Field, []) ->
    Field;
apply_options(Field, [{<<"analyzed">>, Value} | RestOptions]) ->
    apply_options(maps:put(analyzed, Value, Field), RestOptions);
apply_options(Field, [{<<"store">>, <<"yes">>} | RestOptions]) ->
    apply_options(maps:put(store, true, Field), RestOptions);
apply_options(Field, [{<<"store">>, <<"no">>} | RestOptions]) ->
    apply_options(maps:put(store, false, Field), RestOptions);
apply_options(Field, [{<<"store">>, Value} | RestOptions]) ->
    apply_options(maps:put(store, Value, Field), RestOptions);
apply_options(Field, [{<<"facet">>, Value} | RestOptions]) ->
    apply_options(maps:put(facet, Value, Field), RestOptions);
apply_options(Field, [{_, _} | RestOptions]) ->
    apply_options(Field, RestOptions).

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
    IncludeFields1 = construct_include_fields_msg(IncludeFields),
    Counts1 = construct_counts_msg(Counts),
    Ranges1 = construct_ranges_msg(Ranges),
    DrillDown1 = construct_drilldown_msg(DrillDown),
    Msg = #{
        index => Prefix,
        query => Query1,
        limit => Limit,
        stale => Stale,
        sort => SortArg,
        include_fields => IncludeFields1,
        counts => Counts1,
        ranges => Ranges1,
        drilldown => DrillDown1
    },
    Msg2 = case construct_bookmark_msg(Bookmark) of
        nil -> Msg;
        Bookmark1 -> maps:put(bookmark, Bookmark1, Msg)
    end,
    Msg2.

% Need to add a check at httpd layer to make sure each field is a string
construct_include_fields_msg(nil) ->
    [];
construct_include_fields_msg(IncludeFields) when is_binary(IncludeFields) ->
    [IncludeFields];
construct_include_fields_msg(IncludeFields) when is_list(IncludeFields) ->
    IncludeFields.

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
        group_by => binary_to_list(GroupBy),
        % group_offset => GroupOffSet,
        group_limit => GroupLimit,
        group_sort => SortArg
    },
    Msg.

construct_sort_msg(relevance) ->
    #{};
construct_sort_msg(SortArg) when is_binary(SortArg) ->
    #{fields => [SortArg]};
construct_sort_msg(SortArg) when is_list(SortArg) ->
    #{fields => SortArg}.

construct_counts_msg(nil) ->
    [];
construct_counts_msg(Counts) when is_binary(Counts) ->
    [Counts];
construct_counts_msg(Counts) when is_list(Counts) ->
    Counts.

construct_ranges_msg(nil) ->
    #{};
construct_ranges_msg({Ranges}) when is_list(Ranges) ->
    MakeRangeFun = fun
        ({K, {V}}) ->
            SubList = [{?b2l(F), ?b2l(R)} || {F, R} <- V],
            SubMap = maps:from_list(SubList),
            {?b2l(K), #{ranges => SubMap}}
    end,
    RangesList = lists:map(MakeRangeFun, Ranges),
    maps:from_list(RangesList).

construct_drilldown_msg([]) ->
    [];
construct_drilldown_msg(Drilldown) when is_list(Drilldown) ->
    [#{parts => Part} || Part <- Drilldown].

% the default value would be us starting at the beginning
construct_bookmark_msg(nil) ->
    nil;
construct_bookmark_msg(<<>>) ->
    throw({bad_request, "Invalid bookmark parameter supplied"});
construct_bookmark_msg(Bookmark) when is_binary(Bookmark) ->
    Unpacked =  try binary_to_term(couch_util:decodeBase64Url(Bookmark), [safe]) of
        Unpacked0 -> Unpacked0
    catch _:_ ->
        throw({bad_request, "Invalid bookmark parameter supplied"})
    end,
    Float = lists:nth(1, Unpacked),
    Int = lists:nth(2, Unpacked),
    #{order => [#{value => Float}, #{value => Int}]};
construct_bookmark_msg(_) ->
    throw({bad_request, "Invalid bookmark parameter supplied"}).

get_channel() ->
    #{channel => ?SEARCH_CHANNEL}.
