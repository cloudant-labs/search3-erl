-module(search3_indexer).

-export([
    update/2,
    update/4
]).

-include("search3.hrl").
-include_lib("couch/include/couch_db.hrl").
-include_lib("fabric/src/fabric2.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(couch_query_servers, [
    get_os_process/1,
    ret_os_process/1,
    proc_prompt/2
]).

% TODO: 
% 1) Error Handling

update(Db, Index) ->
    Noop = fun (_) -> ok end,
    update(Db, Index, Noop, []).

update(#{} = Db, Index, ProgressCallback, ProgressArgs)
        when is_function(ProgressCallback, 6) ->
    try
        Seq = case search3_rpc:get_update_seq(Index) of
            <<"0">> -> 0;
            Else -> Else
        end,
        DbSeq = get_db_seq(Db),
        Proc = get_os_process(Index#index.def_lang),
        State = #{
            since_seq => Seq,
            count => 0,
            limit => config:get_integer("search3", "change_limit", 100),
            doc_acc => [],
            last_seq => Seq,
            callback => ProgressCallback,
            callback_args => ProgressArgs,
            index => Index,
            proc => Proc
        },
        try
            true = proc_prompt(Proc, [<<"add_fun">>, Index#index.def]),
            update_int(Db, State),
            search3_rpc:set_update_seq(Index, DbSeq)
        after
            ret_os_process(Proc)
        end
    catch error:database_does_not_exist ->
        #{db_prefix := DbPrefix} = Db,
        couch_log:error("Db - ~p database does not exist", [DbPrefix]),
        erlang:error(database_does_not_exist)
    end.

update_int(#{} = Db, State) ->
    {ok, FinalState} = fabric2_fdb:transactional(Db, fun(TxDb) ->
        State1 = maps:put(tx_db, TxDb, State),
        fold_changes(State1)
    end),
    #{
        count := Count,
        limit := Limit,
        doc_acc := DocAcc,
        last_seq := LastSeq,
        callback := Cb,
        callback_args := CallbackArgs,
        index := Index,
        proc := Proc
    } = FinalState,
    index_docs(Index, Proc, DocAcc),
    case Count < Limit of
        true ->
            Cb(undefined, finished, CallbackArgs, Db, Index, LastSeq);
        false ->
            NextState = maps:merge(FinalState, #{
                limit => Limit,
                count => 0,
                doc_acc => [],
                since_seq => LastSeq,
                last_seq => 0
            }),
            update_int(Db, NextState)
    end.

fold_changes(State) ->
    #{
        since_seq := SinceSeq,
        limit := Limit,
        tx_db := TxDb
    } = State,
    fabric2_db:fold_changes(TxDb, SinceSeq,
        fun load_changes/2, State, [{limit, Limit}]).

% grabs the document from changes feed, and stores it into the document
% accumulator for indexing later
load_changes(Change, Acc) ->
    #{
        doc_acc := DocAcc,
        count := Count,
        tx_db := TxDb
    } = Acc,
    #{
        id := Id,
        sequence := LastSeq,
        deleted := Deleted
    } = Change,
    Acc1 = case Id of
        <<"_design/", _/binary>> ->
            maps:merge(Acc, #{
                count => Count + 1,
                last_seq => LastSeq
                });
        _ ->
            Doc = if Deleted -> []; true ->
                case fabric2_db:open_doc(TxDb, Id) of
                    {ok, Doc0} -> Doc0;
                    {not_found, _} -> []
                end
            end,
            Change1 = maps:put(doc, Doc, Change),
            maps:merge(Acc, #{
                doc_acc => DocAcc ++ [Change1],
                count => Count + 1,
                last_seq => LastSeq
            })
    end,
    {ok, Acc1}.

index_docs(Index, Proc, Docs) ->
    DocIndexerFun = fun
        (#{deleted := true, id:= Id}) ->
            ok = search3_rpc:update_index(Index, Id, []);
        (Change) ->
            #{doc := Doc, id:= Id} = Change,
            %% Revisit later for exact format
            Fields = extract_fields(Proc, Doc),
            search3_rpc:update_index(Index, Id, Fields)
    end,
    lists:foreach(DocIndexerFun, Docs),
    ok.

extract_fields(Proc, Doc) ->
    Json = couch_doc:to_json_obj(Doc, []),
    [Fields|_] = proc_prompt(Proc, [<<"index_doc">>, Json]),
    [list_to_tuple(Field) || Field <- Fields].

get_db_seq(Db) ->
    fabric2_fdb:transactional(Db, fun(TxDb) ->
        fabric2_fdb:get_last_change(TxDb)
    end).
