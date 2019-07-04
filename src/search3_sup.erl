-module(search3_sup).

-behaviour(supervisor).

-export([
    start_link/1
]).

-export([
    init/1
]).

start_link(Args) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, Args).


init([]) ->
    Flags = #{
        strategy => one_for_all,
        intensity => 1,
        period => 5
    },
    Children = [
        #{
            id => search3_worker_manager,
            start => {search3_worker_manager, start_link, []}
        }
    ],
    {ok, {Flags, Children}}.
