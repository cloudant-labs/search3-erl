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

start(_StartType, StartArgs) ->
    {ok, _} = application:ensure_all_started(grpcbox),
    Url = config:get("search3", "service_url", "localhost"),
    Port = case config:get("search3", "service_port", 8443) of
        8443 -> 8443;
        Port1 -> list_to_integer(Port1)
    end,
    Endpoints = [{http, Url, Port, []}],
    {ok, _} = grpcbox_channel_sup:start_child(?SEARCH_CHANNEL, Endpoints, #{}),
    search3_sup:start_link(StartArgs).

stop(_State) ->
    ok.