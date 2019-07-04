-module(search3_jobs).

-export([
    status/2,
    add/2,

    accept/0,
    get_job_data/1,
    update/5,
    finish/5,
    set_timeout/0,

    subscribe/2,
    wait/1,
    unsubscribe/1,

    create_job_id/2
]).

-define(SEARCH_JOB_TYPE, <<"search3">>).

-include("search3.hrl").

% Query request usage of jobs

status(TxDb, Index) ->
    JobId = create_job_id(TxDb, Index),

    case couch_jobs:get_job_state(TxDb, ?SEARCH_JOB_TYPE, JobId) of
        {ok, State} -> State;
        {error, not_found} -> not_found;
        Error -> Error
    end.

add(TxDb, Index) ->
    JobData = create_job_data(TxDb, Index, 0),

    JobId = create_job_id(TxDb, Index),
    JTx = couch_jobs_fdb:get_jtx(TxDb),
    couch_log:notice("Job Db ~p, transaction: ~p", [TxDb, JTx]),
    couch_jobs:add(JTx, ?SEARCH_JOB_TYPE, JobId, JobData).

% search3_worker api

accept() ->
    couch_jobs:accept(?SEARCH_JOB_TYPE).

get_job_data(JobId) ->
    couch_jobs:get_job_data(undefined, ?SEARCH_JOB_TYPE, JobId).

update(JTx, Job, Db, Index, LastSeq) ->
    JobData = create_job_data(Db, Index, LastSeq),
    couch_jobs:update(JTx, Job, JobData).

finish(JTx, Job, Db, Index, LastSeq) ->
    JobData = create_job_data(Db, Index, LastSeq),
    couch_jobs:finish(JTx, Job, JobData).

set_timeout() ->
    couch_log:notice("Calling set_timeout", []),
    couch_jobs:set_type_timeout(?SEARCH_JOB_TYPE, 6 * 1000).

% Watcher Job api

subscribe(Db, Index) ->
    JobId = create_job_id(Db, Index),
    couch_jobs:subscribe(?SEARCH_JOB_TYPE, JobId).

wait(JobSubscription) ->
    case couch_jobs:wait(JobSubscription, infinity) of
        {?SEARCH_JOB_TYPE, _JobId, JobState, JobData} -> {JobState, JobData};
        {timeout} -> {error, timeout}
    end.

unsubscribe(JobSubscription) ->
    couch_jobs:unsubscribe(JobSubscription).

% Internal

create_job_id(#{name := DbName}, #index{sig = Sig}) ->
    create_job_id(DbName, Sig);

create_job_id(DbName, Sig) ->
    <<DbName/binary, Sig/binary>>.

create_job_data(Db, Index, LastSeq) ->
    #{name := DbName} = Db,

    #index{
        ddoc_id = DDocId,
        name = IndexName
    } = Index,

    #{
        db_name => DbName,
        ddoc_id => DDocId,
        last_seq => LastSeq,
        name => IndexName
    }.
