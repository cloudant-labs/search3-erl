%%%-------------------------------------------------------------------
%% @doc search3 public API
%% @end
%%%-------------------------------------------------------------------

-module(search3_app).

-behaviour(application).

-include("search3.hrl").

-export([
    start/2,
    stop/1
]).

start(_StartType, _StartArgs) ->
    search3_sup:start_link().

stop(_State) ->
    ok.
