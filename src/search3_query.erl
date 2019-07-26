-module(search3_query).

-export([
    run_query/4
]).

-include("search3.hrl").

run_query(Db, DDoc, IndexName, QueryArgs) ->
    #{name := DbName} = Db,
    {ok, Index} = search3_util:design_doc_to_index(DDoc, IndexName),
    Index1 = Index#index{dbname = DbName},
    run_query(Db, Index1, QueryArgs).

run_query(Db, Index, QueryArgs) ->
    % The UpdateSeq here returned supposedly means the index us up to date.
    % However there is a scenario were a pod dies at indexing time and the
    % CommitedSeq is behind the UpdateSeq. In this case we need to re-run the
    % indexer and search requests again.
    UpdateSeq = maybe_build_index(Db, Index),
    {ok, Response, _} = search3_rpc:search_index(Index, QueryArgs),
    % TODO: should move this response processing into separate function
    #{
        seq := ComittedSeq, 
        matches := Matches,
        hits := Hits
    } = Response,
    Bookmark = maps:get(bookmark, Response, <<>>),
    % TODO: do this re-try thing in a separate function
    #{
        seq := CommitedSeqVal
    } = ComittedSeq,
    case CommitedSeqVal < UpdateSeq of
        true -> run_query(Db, Index, QueryArgs);
        _ -> {Bookmark, Matches, Hits}
    end.

maybe_build_index(Db, Index) ->
    {Action, WaitSeq} = fabric2_fdb:transactional(Db, fun(TxDb) ->
        DbSeq = fabric2_db:get_update_seq(TxDb),
        SearchSeq = search3_rpc:get_update_seq(Index),
        case DbSeq == SearchSeq of
            true -> {ready, DbSeq};
            false -> {build, DbSeq}
        end
    end),
    if Action == ready -> ok; true ->
        search3_jobs:build_search(Db, Index, WaitSeq)
    end,
    WaitSeq.
