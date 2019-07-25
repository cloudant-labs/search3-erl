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
            couch_log:notice("Query ~p produced stale results. Re-Running",
                [Query]),
            run_query(Db, Index, Query);
        _ -> {Bookmark, Matches, Hits}
    end.

maybe_build_index(Db, Index) ->
    WaitSeq = fabric2_fdb:transactional(Db, fun(TxDb) ->
        DbSeq = fabric2_db:get_update_seq(TxDb),
        SearchSeq = search3_rpc:get_update_seq(Index),
        case DbSeq == SearchSeq of
            true -> ready;
            false -> SearchSeq
        end
    end),
    if WaitSeq == ready -> ok; true ->
        search3_jobs:build_search(Db, Index, WaitSeq)
    end.
