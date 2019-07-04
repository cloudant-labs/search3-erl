-module(search3_worker).

-export([
    start/2,
    job_progress/6
]).

-include("search3.hrl").

start(Job, JobData) ->
    {ok, Db, Index} = get_indexing_info(JobData),
    % maybe we should spawn here
    search3_indexer:update(Db, Index, fun job_progress/6, Job).

job_progress(Tx, Progress, Job, Db, Index, LastSeq) ->
    case Progress of
        update ->
            search3_jobs:update(Tx, Job, Db, Index, LastSeq);
        finished ->
            search3_jobs:finish(Tx, Job, Db, Index, LastSeq)
    end.

get_indexing_info(JobData) ->
    #{
        <<"db_name">> := DbName,
        <<"ddoc_id">> := DDocId,
        <<"name">> := IndexName
    } = JobData,
    {ok, Db} = fabric2_db:open(DbName, []),
    {ok, DDoc} = fabric2_db:open_doc(Db, DDocId),
    % TODO: check to see if we pass in Index instead of calling
    % design_doc_to_index again.
    {ok, Index} = search3_util:design_doc_to_index(DDoc, IndexName),
    Index1 = Index#index{dbname = DbName},
    {ok, Db, Index1}.
