-module(search3_sup).

-behaviour(supervisor).

-export([
    start_link/0
]).

-export([
    init/1
]).

start_link() ->
    Arg = case fabric2_node_types:is_type(search_indexing) of
        true -> normal;
        false -> builds_disabled
    end,
    supervisor:start_link({local, ?MODULE}, ?MODULE, Arg).


init(normal) ->
    Children = [
        #{
            id => search3_worker_manager,
            start => {search3_worker_manager, start_link, []}
        }
    ],
    {ok, {flags(), Children}};

init(builds_disabled) ->
    couch_log:notice("~p : search_indexing disabled", [?MODULE]),
    search3_jobs:set_timeout(),
    {ok, {flags(), []}}.


flags() ->
    #{
        strategy => one_for_all,
        intensity => 1,
        period => 5
    },
