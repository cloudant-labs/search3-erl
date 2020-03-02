-module(search3_indexer).

-export([
    update/2
]).

-export([
    spawn_link/0
]).


-export([
    init/0
]).

-include("search3.hrl").
-include_lib("couch/include/couch_db.hrl").
-include_lib("fabric/include/fabric2.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(couch_query_servers, [
    get_os_process/1,
    ret_os_process/1,
    proc_prompt/2
]).

spawn_link() ->
    proc_lib:spawn_link(?MODULE, init, []).

init() ->
    {ok, Job, JobData} = couch_jobs:accept(?SEARCH_JOB_TYPE, #{}),
    #{
        <<"db_name">> := DbName,
        <<"ddoc_id">> := DDocId,
        <<"name">> := IndexName,
        <<"sig">> := JobSig
    } = JobData,
    {ok, Db} = fabric2_db:open(DbName, []),
    {ok, DDoc} = fabric2_db:open_doc(Db, DDocId),
    {ok, Index} = search3_util:design_doc_to_index(Db, DDoc, IndexName),
    Index1 = Index#index{dbname = DbName},
    HexSig = fabric2_util:to_hex(Index1#index.sig),

    if  HexSig == JobSig -> ok; true ->
        couch_jobs:finish(undefined, Job, JobData#{
            <<"error">> => <<"sig_changed">>,
            <<"reason">> => <<"Design document was modified">>
        }),
        exit(normal)
    end,
    State = #{
        tx_db => undefined,
        search_seq => undefined,
        count => 0,
        limit => config:get_integer("search3", "change_limit", 100),
        doc_acc => [],
        last_seq => undefined,
        job => Job,
        job_data => JobData,
        index => Index1,
        proc => undefined
    },
    update(Db, State).

update(#{} = Db, State) ->
    try
        Index = maps:get(index, State),
        {InitSession, Seq} = search3_rpc:get_update_seq(Index),
        Proc = get_os_process(Index#index.def_lang),
        % Start of a new session
        Index1 = Index#index{session=InitSession},
        NewState = State#{
            search_seq => Seq,
            last_seq => Seq,
            proc => Proc,
            index => Index1
        },
        try
            true = proc_prompt(Proc, [<<"add_fun">>, Index1#index.def]),
            search3_util:update_ddoc_list(Db, Index),
            update_int(Db, NewState)
        after
            ret_os_process(Proc)
        end
    catch error:database_does_not_exist ->
        #{db_prefix := DbPrefix} = Db,
        couch_log:error("Db - ~p database does not exist", [DbPrefix]),
        erlang:error(database_does_not_exist)
    end.

update_int(Db, State) ->
    State5 = fabric2_fdb:transactional(Db, fun(TxDb) ->
        State1 = State#{tx_db := TxDb},
        {ok, State2} = fold_changes(State1),
        #{
            count := Count,
            limit := Limit,
            doc_acc := DocAcc,
            last_seq := LastSeq,
            index := Index,
            proc := Proc
        } = State2,

        DocAcc1 = fetch_docs(TxDb, DocAcc),

        % 1) purge_seq is empty for now, subsequent releases will support this
        % 2) We are indexing inside a transaction, which has a timeout of 5s.
        % This can potentially be a problem for large documents. We should
        % revisit this design later.
        try
            Session = index_docs(Index, Proc, LastSeq, <<>>, DocAcc1),
             % Update the session after each update
            Index1 = Index#index{session = Session},
            State3 = maps:put(index, Index1, State2),
            case Count < Limit of
                true ->
                    report_progress(State3, finished),
                    finished;
                false ->
                    State4 = report_progress(State3, update),
                    State4 #{
                        tx_db := undefined,
                        count := 0,
                        doc_acc := [],
                        search_seq := LastSeq
                    }
           end
        catch throw:session_mismatch ->
            % if there is a session mismatch, we just finish this job and
            % restart it again
            report_progress(State2, finished)
        end
    end),
    case State5 of
        finished ->
            % should we ret_os_process(Proc) here? or in the after clause
            % in update/2?
            ok;
        _ ->
            update_int(Db, State5)
    end.

fold_changes(State) ->
    #{
        search_seq := SearchSeq,
        limit := Limit,
        tx_db := TxDb
    } = State,
    Opts = [{limit, Limit}, {restart_tx, false}],
    fabric2_db:fold_changes(TxDb, SearchSeq,
        fun load_changes_count/2, State, Opts).

load_changes_count(Change, Acc) ->
    #{
        doc_acc := DocAcc,
        count := Count
    } = Acc,
    #{
        sequence := LastSeq
    } = Change,
    Acc1 = Acc#{
        doc_acc := DocAcc ++ [Change],
        count := Count +1,
        last_seq := LastSeq
    },
    {ok, Acc1}.

index_docs(Index, Proc, Seq, PurgeSeq, Docs) ->
    InitSession = Index#index.session,
    DocIndexerFun = fun
        (#{deleted := true, id:= Id}, _) ->
            search3_rpc:delete_index(Index, Id, Seq, PurgeSeq);
        (#{deleted := false, doc := Doc, id:= Id}, _) ->
            Fields = extract_fields(Proc, Doc),
            search3_rpc:update_index(Index, Id, Seq, PurgeSeq, Fields)
    end,
    {Session, _} = lists:foldl(DocIndexerFun, {InitSession, Seq}, Docs),
    Session.

fetch_docs(Db, Changes) ->
    {Deleted, NotDeleted} = lists:partition(fun(Doc) ->
        #{deleted := Deleted} = Doc,
        Deleted
    end, Changes),

    RevState = lists:foldl(fun(Change, Acc) ->
        #{id := Id} = Change,
        RevFuture = fabric2_fdb:get_winning_revs_future(Db, Id, 1),
        Acc#{
            RevFuture => {Id, Change}
        }
    end, #{}, NotDeleted),

    RevFutures = maps:keys(RevState),
    BodyState = lists:foldl(fun(RevFuture, Acc) ->
        {Id, Change} = maps:get(RevFuture, RevState),
        Revs = fabric2_fdb:get_winning_revs_wait(Db, RevFuture),

        % I'm assuming that in this changes transaction that the winning
        % doc body exists since it is listed in the changes feed as not deleted
        #{winner := true} = RevInfo = lists:last(Revs),
        BodyFuture = fabric2_fdb:get_doc_body_future(Db, Id, RevInfo),
        Acc#{
            BodyFuture => {Id, RevInfo, Change}
        }
    end, #{}, erlfdb:wait_for_all(RevFutures)),

    BodyFutures = maps:keys(BodyState),
    ChangesWithDocs = lists:map(fun (BodyFuture) ->
        {Id, RevInfo, Change} = maps:get(BodyFuture, BodyState),
        Doc = fabric2_fdb:get_doc_body_wait(Db, Id, RevInfo, BodyFuture),
        Change#{doc => Doc}
    end, erlfdb:wait_for_all(BodyFutures)),

    % This combines the deleted changes with the changes that contain docs
    % Important to note that this is now unsorted. Which is fine for now
    % But later could be an issue if we split this across transactions
    Deleted ++ ChangesWithDocs.

extract_fields(Proc, Doc) ->
    Json = couch_doc:to_json_obj(Doc, []),
    [Fields|_] = proc_prompt(Proc, [<<"index_doc">>, Json]),
    [list_to_tuple(Field) || Field <- Fields].

report_progress(State, UpdateType) ->
    #{
        tx_db := TxDb,
        job := Job1,
        job_data := JobData,
        last_seq := LastSeq,
        index := Index
    } = State,

    Session = Index#index.session,

    #{
        <<"db_name">> := DbName,
        <<"ddoc_id">> := DDocId,
        <<"name">> := IndexName,
        <<"sig">> := Sig
    } = JobData,

    % Reconstruct from scratch to remove any
    % possible existing error state.
    NewData = #{
        <<"db_name">> => DbName,
        <<"ddoc_id">> => DDocId,
        <<"name">> => IndexName,
        <<"sig">> => Sig,
        <<"search_seq">> => LastSeq,
        <<"session">> => Session
    },

    case UpdateType of
        update ->
            case couch_jobs:update(TxDb, Job1, NewData) of
                {ok, Job2} ->
                    State#{job := Job2};
                {error, halt} ->
                    couch_log:error("~s job halted :: ~w", [?MODULE, Job1]),
                    exit(normal)
            end;
        finished ->
            case couch_jobs:finish(TxDb, Job1, NewData) of
                ok ->
                    State;
                {error, halt} ->
                    couch_log:error("~s job halted :: ~w", [?MODULE, Job1]),
                    exit(normal)
            end
    end.
