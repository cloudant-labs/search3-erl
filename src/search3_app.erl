%%%-------------------------------------------------------------------
%% @doc search3 public API
%% @end
%%%-------------------------------------------------------------------

-module(search3_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, StartArgs) ->
    search3_sup:start_link(StartArgs).

stop(_State) ->
    ok.

%% internal functions
