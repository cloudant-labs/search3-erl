-module(search3_jobs).

-export([
    set_timeout/0,
    build_search/3,
    build_search_async/2
]).

-include("search3.hrl").

set_timeout() ->
    couch_jobs:set_type_timeout(?SEARCH_JOB_TYPE, 6 * 1000).

build_search(TxDb, Index, UpdateSeq) ->
    {ok, JobId} = build_search_async(TxDb, Index),
    case wait_for_job(JobId, UpdateSeq) of
        ok -> ok;
        retry -> build_search(TxDb, Index, UpdateSeq)
    end.

build_search_async(TxDb, Index) ->
    JobId = job_id(TxDb, Index),
    JobData = job_data(TxDb, Index),
    ok = couch_jobs:add(undefined, ?SEARCH_JOB_TYPE, JobId, JobData),
    {ok, JobId}.

wait_for_job(JobId, UpdateSeq) ->
    case couch_jobs:subscribe(?SEARCH_JOB_TYPE, JobId) of
        {ok, Subscription, _State, _Data} ->
            wait_for_job(JobId, Subscription, UpdateSeq);
        {ok, finished, Data} ->
            case Data of
                #{<<"search_seq">> := SearchSeq} when SearchSeq >= UpdateSeq ->
                    ok;
                _ ->
                    retry
            end
    end.

wait_for_job(JobId, Subscription, UpdateSeq) ->
    case wait(Subscription) of
        {error, Error} ->
            erlang:error(Error);
        {finished, #{<<"error">> := Error, <<"reason">> := Reason}} ->
            erlang:error({binary_to_atom(Error, latin1), Reason});
        {finished, #{<<"search_seq">> := SearchSeq}} when SearchSeq >= UpdateSeq ->
            ok;
        {finished, _} ->
            wait_for_job(JobId, UpdateSeq);
        {_State, #{<<"search_seq">> := SearchSeq}} when SearchSeq >= UpdateSeq ->
            couch_jobs:unsubscribe(Subscription),
            ok;
        {_, _} ->
            wait_for_job(JobId, Subscription, UpdateSeq)
    end.

job_id(#{name := DbName}, #index{sig = Sig}) ->
    job_id(DbName, Sig);
job_id(DbName, Sig) ->
    HexSig = fabric2_util:to_hex(Sig),
    <<DbName/binary, "-", HexSig/binary>>.

job_data(Db, Index) ->
    #index{
        ddoc_id = DDocId,
        name = IndexName,
        sig = Sig
    } = Index,

    #{
        db_name => fabric2_db:name(Db),
        ddoc_id => DDocId,
        name => IndexName,
        sig => fabric2_util:to_hex(Sig)
    }.

wait(Subscription) ->
    case couch_jobs:wait(Subscription, infinity) of
        {?SEARCH_JOB_TYPE, _JobId, JobState, JobData} ->
            {JobState, JobData};
        timeout ->
            {error, timeout}
    end.
