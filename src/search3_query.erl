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
    % A scenario exists wjere a pod dies at indexing time and the
    % CommitedSeq is behind the UpdateSeq. This can be detected when the
    % Session id after indexing is different than the session id of a query.
    Session = maybe_build_index(Db, Index),
    try
        Response = search3_rpc:search_index(Index, QueryArgs),
        search3_response:handle_search_response(Response, Session)
    catch throw:session_mismatch ->
        run_query(Db, Index, QueryArgs)
    end.

maybe_build_index(Db, Index) ->
    {Action, Session, WaitSeq} = fabric2_fdb:transactional(Db, fun(TxDb) ->
        DbSeq = fabric2_db:get_update_seq(TxDb),
        {UpdateSession, SearchSeq} = search3_rpc:get_update_seq(Index),
        case DbSeq == SearchSeq of
            true -> {ready, UpdateSession, DbSeq};
            false -> {build, UpdateSession, DbSeq}
        end
    end),
    % This returns a Session Id. It either returns the session from the last
    % get_update_seq, or it returns the seq from the building of the index.
    if Action == ready -> Session; true ->
        search3_jobs:build_search(Db, Index, WaitSeq)
    end.