% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

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


-include("search3.hrl").


% Query request usage of jobs


status(TxDb, Index) ->
    JobId = create_job_id(TxDb, Index),

    case couch_jobs:get_job_state(TxDb, ?INDEX_JOB_TYPE, JobId) of
        {ok, State} -> State;
        {error, not_found} -> not_found;
        Error -> Error
    end.


add(TxDb, Index) ->
    JobData = create_job_data(TxDb, Index, 0),

    JobId = create_job_id(TxDb, Index),
    JTx = couch_jobs_fdb:get_jtx(),
    couch_jobs:add(JTx, ?INDEX_JOB_TYPE, JobId, JobData).


% search3_worker api


accept() ->
    couch_jobs:accept(?INDEX_JOB_TYPE).


get_job_data(JobId) ->
    couch_jobs:get_job_data(undefined, ?INDEX_JOB_TYPE, JobId).


update(JTx, Job, Db, Index, LastSeq) ->
    JobData = create_job_data(Db, Index, LastSeq),
    couch_jobs:update(JTx, Job, JobData).


finish(JTx, Job, Db, Index, LastSeq) ->
    JobData = create_job_data(Db, Index, LastSeq),
    couch_jobs:finish(JTx, Job, JobData).


set_timeout() ->
    couch_jobs:set_type_timeout(?INDEX_JOB_TYPE, 6 * 1000).


% Watcher Job api


subscribe(Db, Index) ->
    JobId = create_job_id(Db, Index),
    couch_jobs:subscribe(?INDEX_JOB_TYPE, JobId).


wait(JobSubscription) ->
    case couch_jobs:wait(JobSubscription, infinity) of
        {?INDEX_JOB_TYPE, _JobId, JobState, JobData} -> {JobState, JobData};
        {timeout} -> {error, timeout}
    end.


unsubscribe(JobSubscription) ->
    couch_jobs:unsubscribe(JobSubscription).


create_job_id(#{name := DbName}, #index{sig = Sig}) ->
    create_job_id(DbName, Sig);

create_job_id(DbName, Sig) ->
    <<DbName/binary, Sig/binary>>.


create_job_data(Db, Index, LastSeq) ->
    #{name := DbName} = Db,

    #index{
        name = DDocId
    } = Index,

    #{
        db_name => DbName,
        ddoc_id => DDocId,
        last_seq => LastSeq
    }.
