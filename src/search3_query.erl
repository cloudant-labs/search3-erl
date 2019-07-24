-module(search3_query).

-export([
    run_query/4
]).

-include("search3.hrl").

run_query(Db, DDoc, IndexName, QueryArgs) ->
    #{name := DbName} = Db,
    #index_query_args{q = Query} = QueryArgs,
    {ok, Index} = search3_util:design_doc_to_index(DDoc, IndexName),
    Index1 = Index#index{dbname = DbName},
    run_query(Db, Index1, Query).

run_query(Db, Index, Query) ->
    % The UpdateSeq here returned supposedly means the index us up to date.
    % However there is a scenario were a pod dies at indexing time and the
    % CommitedSeq is behind the UpdateSeq. In this case we need to re-run the
    % indexer and search requests again.
    UpdateSeq = maybe_build_index(Db, Index),
    {ok, Response, _} = search3_rpc:search_index(Index, Query),
    #{
        seq := ComittedSeq, 
        bookmark := Bookmark,
        matches := Matches,
        hits := Hits
    } = Response,
    case ComittedSeq < UpdateSeq of
        true ->
            couch_log:error("Query ~p produced stale results. Re-Running",
                [Query]),
            run_query(Db, Index, Query);
        _ -> {Bookmark, Matches, Hits}
    end.

maybe_build_index(Db, Index) ->
    {Status, Seq} = fabric2_fdb:transactional(Db, fun(TxDb) ->
        case is_index_updated(TxDb, Index) of
            {true, UpdateSeq} ->
                {ready, UpdateSeq};
            {false, LatestSeq} ->
                maybe_add_couch_job(TxDb, Index),
                {false, LatestSeq}
        end
    end),
    if Status == ready -> Seq; true ->
        subscribe_and_wait_for_index(Db, Index, Seq)
    end.

is_index_updated(Db, Index) ->
    #{name := DbName} = Db,
    Index1 = Index#index{dbname = DbName},
    fabric2_fdb:transactional(Db, fun(TxDb) ->
        UpdateSeq = search3_rpc:get_update_seq(Index1),
        LastChange = fabric2_fdb:get_last_change(TxDb),
        {UpdateSeq == LastChange, LastChange}
    end).

maybe_add_couch_job(TxDb, Index) ->
    case search3_jobs:status(TxDb, Index) of
        running ->
            ok;
        pending ->
            ok;
        Status when Status == finished orelse Status == not_found ->
            search3_jobs:add(TxDb, Index)
    end.

subscribe_and_wait_for_index(Db, Index, Seq) ->
    case search3_jobs:subscribe(Db, Index) of
        {error, Error} ->
            throw({error, Error});
        {ok, finished, _} ->
            ready;
        {ok, Subscription, _JobState, _} ->
            wait_for_index_ready(Subscription, Db, Index, Seq)
    end.

wait_for_index_ready(Subscription, Db, Index, Seq) ->
    Out = search3_jobs:wait(Subscription),
    case Out of
        {finished, _JobData} ->
            ready;
        {pending, _JobData} ->
            wait_for_index_ready(Subscription, Db, Index, Seq);
        {running, #{last_seq := LastSeq}} ->
            if LastSeq =< Seq -> ready; true ->
                wait_for_index_ready(Subscription, Db, Index, Seq)
            end;
        {running, _JobData} ->
            wait_for_index_ready(Subscription, Db, Index, Seq);
        {error, Error} ->
            throw({error, Error})
    end.
