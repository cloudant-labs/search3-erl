-module(search3_worker).

-export([
    start/2,
    job_progress/6
]).


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
        <<"ddoc_id">> := DDocId
    } = JobData,
    {ok, Db} = fabric2_db:open(DbName, []),
    {ok, DDoc} = fabric2_db:open_doc(Db, DDocId),
    {ok, Index} = search3_util:ddoc_to_mrst(DbName, DDoc),
    {ok, Db, Index}.
