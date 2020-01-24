%%%-------------------------------------------------------------------
%% @doc search3 public API
%% @end
%%%-------------------------------------------------------------------

-module(search3_app).

-behaviour(application).

-include_lib("kernel/include/inet.hrl").
-include("search3.hrl").

-export([
    start/2,
    stop/1
]).

start(_StartType, _StartArgs) ->
    EndPoint = get_endpoints(),
    application:set_env(search3, service_endpoint, EndPoint),
    search3_sup:start_link().

stop(_State) ->
    ok.

get_endpoints() ->
    Host = config:get("search3", "service_url", "localhost"),
    case Host of
        ":discover" ->
            discover_endpoints();
        _ ->
            Port = config:get_integer("search3", "service_port", 8443),
            to_conn_str(Host, Port)
    end.

discover_endpoints() ->
    DNSName = config:get("search3", "service_dns"),
    case inet_res:getbyname(DNSName, srv) of
        {ok, #hostent{h_addr_list = AddrList}} ->
            lists:map(fun({_Priority, _Weight, Port, Host}) ->
                to_conn_str(Host, Port)
            end, AddrList);
        {error, Reason} ->
            erlang:error({service_lookup_failed, Reason})
    end.

to_conn_str(Host, Port) ->
    Str = io_lib:format("http://~s:~b/Search/", [Host, Port]),
    lists:flatten(Str).
