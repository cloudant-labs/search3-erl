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
    {ok, Index} = search3_util:design_doc_to_index(DDoc, IndexName),
    Index1 = Index#index{dbname = DbName},
    HexSig = fabric2_util:to_hex(Index1#index.sig),

    if  HexSig == JobSig -> ok; true ->
        couch_jobs:finish(undefined, Job, JobData#{
            error => sig_changed,
            reason => <<"Design document was modified">>
        }),
        exit(normal)
    end,
    State = #{
        txdb => undefined,
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
            update_int(Db, NewState)
        after
            ret_os_process(Proc)
        end
    catch error:database_does_not_exist ->
        #{db_prefix := DbPrefix} = Db,
        couch_log:error("Db - ~p database does not exist", [DbPrefix]),
        erlang:error(database_does_not_exist)
    end.

update_int(#{} = Db, State) ->
    State4 = fabric2_fdb:transactional(Db, fun(TxDb) ->
        State1 = maps:put(tx_db, TxDb, State),
        {ok, State2} = fold_changes(State1),
        #{
            count := Count,
            limit := Limit,
            doc_acc := DocAcc,
            last_seq := LastSeq,
            index := Index,
            proc := Proc
        } = State2,

        % 1) purge_seq is empty for now, subsequent releases will support this
        % 2) We are indexing inside a transaction, which has a timeout of 5s.
        % This can potentially be a problem for large documents. We should
        % revisit this design later.
        try
            Session = index_docs(Index, Proc, LastSeq, <<>>, DocAcc),
             % Update the session after each update
            Index1 = Index#index{session = Session},
            State3 = maps:put(index, Index1, State2),
            case Count < Limit of
                true ->
                    % this should be only called once afer all docs are index
                    search3_rpc:set_update_seq(Index1, LastSeq, <<>>),
                    report_progress(State3, finished),
                    finished;
                false ->
                    report_progress(State3, update),
                    State2#{
                        tx_db := undefined,
                        count := 0,
                        doc_acc := [],
                        search_seq => LastSeq,
                        % make sure this reset here is correct
                        last_seq := 0
                    }
           end
        catch error:session_mismatch ->
            % if there is a session mismatch, we just finish this job and
            % restart it again
            report_progress(State2, finished),
            exit(normal)
        end
    end),
    case State4 of
        finished ->
            % should we ret_os_process(Proc) here? or in the after clause
            % in update/2?
            ok;
        _ ->
            update_int(Db, State4)
    end.

fold_changes(State) ->
    #{
        search_seq := SearchSeq,
        limit := Limit,
        tx_db := TxDb
    } = State,
    fabric2_db:fold_changes(TxDb, SearchSeq,
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

index_docs(Index, Proc, Seq, PurgeSeq, Docs) ->
    InitSession = Index#index.session,
    DocIndexerFun = fun
        (#{deleted := true, id:= Id}, _) ->
            search3_rpc:delete_index(Index, Id, Seq, PurgeSeq);
        (Change, _) ->
            #{doc := Doc, id:= Id} = Change,
            Fields = extract_fields(Proc, Doc),
            search3_rpc:update_index(Index, Id, Seq, PurgeSeq, Fields)
    end,
    {Session, _} = lists:foldl(DocIndexerFun, {InitSession, Seq}, Docs),
    Session.

extract_fields(Proc, Doc) ->
    Json = couch_doc:to_json_obj(Doc, []),
    [Fields|_] = proc_prompt(Proc, [<<"index_doc">>, Json]),
    [list_to_tuple(Field) || Field <- Fields].

report_progress(State, UpdateType) ->
    #{
        tx_db := TxDb,
        job := Job,
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
            couch_jobs:update(TxDb, Job, NewData);
        finished ->
            couch_jobs:finish(TxDb, Job, NewData)
    end.
