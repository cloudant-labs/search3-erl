%%%-------------------------------------------------------------------
%% @doc Client module for grpc service Search.
%% @end
%%%-------------------------------------------------------------------

%% this module was generated on 2019-07-04T19:00:51+00:00 and should not be modified manually

-module(search_client).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("grpcbox/include/grpcbox.hrl").

-define(is_ctx(Ctx), is_tuple(Ctx) andalso element(1, Ctx) =:= ctx).

-define(SERVICE, 'Search').
-define(PROTO_MODULE, 'search3_pb').
-define(MARSHAL_FUN(T), fun(I) -> ?PROTO_MODULE:encode_msg(I, T) end).
-define(UNMARSHAL_FUN(T), fun(I) -> ?PROTO_MODULE:decode_msg(I, T) end).
-define(DEF(Input, Output, MessageType), #grpcbox_def{service=?SERVICE,
                                                      message_type=MessageType,
                                                      marshal_fun=?MARSHAL_FUN(Input),
                                                      unmarshal_fun=?UNMARSHAL_FUN(Output)}).

%% @doc Unary RPC
-spec delete(search3_pb:index()) ->
    {ok, search3_pb:service_response(), grpcbox:metadata()} | grpcbox_stream:grpc_error_response().
delete(Input) ->
    delete(ctx:new(), Input, #{}).

-spec delete(ctx:t() | search3_pb:index(), search3_pb:index() | grpcbox_client:options()) ->
    {ok, search3_pb:service_response(), grpcbox:metadata()} | grpcbox_stream:grpc_error_response().
delete(Ctx, Input) when ?is_ctx(Ctx) ->
    delete(Ctx, Input, #{});
delete(Input, Options) ->
    delete(ctx:new(), Input, Options).

-spec delete(ctx:t(), search3_pb:index(), grpcbox_client:options()) ->
    {ok, search3_pb:service_response(), grpcbox:metadata()} | grpcbox_stream:grpc_error_response().
delete(Ctx, Input, Options) ->
    grpcbox_client:unary(Ctx, <<"/Search/Delete">>, Input, ?DEF(index, service_response, <<"Index">>), Options).

%% @doc Unary RPC
-spec get_update_sequence(search3_pb:index()) ->
    {ok, search3_pb:update_seq(), grpcbox:metadata()} | grpcbox_stream:grpc_error_response().
get_update_sequence(Input) ->
    get_update_sequence(ctx:new(), Input, #{}).

-spec get_update_sequence(ctx:t() | search3_pb:index(), search3_pb:index() | grpcbox_client:options()) ->
    {ok, search3_pb:update_seq(), grpcbox:metadata()} | grpcbox_stream:grpc_error_response().
get_update_sequence(Ctx, Input) when ?is_ctx(Ctx) ->
    get_update_sequence(Ctx, Input, #{});
get_update_sequence(Input, Options) ->
    get_update_sequence(ctx:new(), Input, Options).

-spec get_update_sequence(ctx:t(), search3_pb:index(), grpcbox_client:options()) ->
    {ok, search3_pb:update_seq(), grpcbox:metadata()} | grpcbox_stream:grpc_error_response().
get_update_sequence(Ctx, Input, Options) ->
    grpcbox_client:unary(Ctx, <<"/Search/GetUpdateSequence">>, Input, ?DEF(index, update_seq, <<"Index">>), Options).

%% @doc Unary RPC
-spec info(search3_pb:index()) ->
    {ok, search3_pb:info_response(), grpcbox:metadata()} | grpcbox_stream:grpc_error_response().
info(Input) ->
    info(ctx:new(), Input, #{}).

-spec info(ctx:t() | search3_pb:index(), search3_pb:index() | grpcbox_client:options()) ->
    {ok, search3_pb:info_response(), grpcbox:metadata()} | grpcbox_stream:grpc_error_response().
info(Ctx, Input) when ?is_ctx(Ctx) ->
    info(Ctx, Input, #{});
info(Input, Options) ->
    info(ctx:new(), Input, Options).

-spec info(ctx:t(), search3_pb:index(), grpcbox_client:options()) ->
    {ok, search3_pb:info_response(), grpcbox:metadata()} | grpcbox_stream:grpc_error_response().
info(Ctx, Input, Options) ->
    grpcbox_client:unary(Ctx, <<"/Search/Info">>, Input, ?DEF(index, info_response, <<"Index">>), Options).

%% @doc Unary RPC
-spec set_update_sequence(search3_pb:set_update_seq()) ->
    {ok, search3_pb:service_response(), grpcbox:metadata()} | grpcbox_stream:grpc_error_response().
set_update_sequence(Input) ->
    set_update_sequence(ctx:new(), Input, #{}).

-spec set_update_sequence(ctx:t() | search3_pb:set_update_seq(), search3_pb:set_update_seq() | grpcbox_client:options()) ->
    {ok, search3_pb:service_response(), grpcbox:metadata()} | grpcbox_stream:grpc_error_response().
set_update_sequence(Ctx, Input) when ?is_ctx(Ctx) ->
    set_update_sequence(Ctx, Input, #{});
set_update_sequence(Input, Options) ->
    set_update_sequence(ctx:new(), Input, Options).

-spec set_update_sequence(ctx:t(), search3_pb:set_update_seq(), grpcbox_client:options()) ->
    {ok, search3_pb:service_response(), grpcbox:metadata()} | grpcbox_stream:grpc_error_response().
set_update_sequence(Ctx, Input, Options) ->
    grpcbox_client:unary(Ctx, <<"/Search/SetUpdateSequence">>, Input, ?DEF(set_update_seq, service_response, <<"SetUpdateSeq">>), Options).

%% @doc Unary RPC
-spec search(search3_pb:search_request()) ->
    {ok, search3_pb:search_response(), grpcbox:metadata()} | grpcbox_stream:grpc_error_response().
search(Input) ->
    search(ctx:new(), Input, #{}).

-spec search(ctx:t() | search3_pb:search_request(), search3_pb:search_request() | grpcbox_client:options()) ->
    {ok, search3_pb:search_response(), grpcbox:metadata()} | grpcbox_stream:grpc_error_response().
search(Ctx, Input) when ?is_ctx(Ctx) ->
    search(Ctx, Input, #{});
search(Input, Options) ->
    search(ctx:new(), Input, Options).

-spec search(ctx:t(), search3_pb:search_request(), grpcbox_client:options()) ->
    {ok, search3_pb:search_response(), grpcbox:metadata()} | grpcbox_stream:grpc_error_response().
search(Ctx, Input, Options) ->
    grpcbox_client:unary(Ctx, <<"/Search/Search">>, Input, ?DEF(search_request, search_response, <<"SearchRequest">>), Options).

%% @doc Unary RPC
-spec group_search(search3_pb:group_search_request()) ->
    {ok, search3_pb:group_search_response(), grpcbox:metadata()} | grpcbox_stream:grpc_error_response().
group_search(Input) ->
    group_search(ctx:new(), Input, #{}).

-spec group_search(ctx:t() | search3_pb:group_search_request(), search3_pb:group_search_request() | grpcbox_client:options()) ->
    {ok, search3_pb:group_search_response(), grpcbox:metadata()} | grpcbox_stream:grpc_error_response().
group_search(Ctx, Input) when ?is_ctx(Ctx) ->
    group_search(Ctx, Input, #{});
group_search(Input, Options) ->
    group_search(ctx:new(), Input, Options).

-spec group_search(ctx:t(), search3_pb:group_search_request(), grpcbox_client:options()) ->
    {ok, search3_pb:group_search_response(), grpcbox:metadata()} | grpcbox_stream:grpc_error_response().
group_search(Ctx, Input, Options) ->
    grpcbox_client:unary(Ctx, <<"/Search/GroupSearch">>, Input, ?DEF(group_search_request, group_search_response, <<"GroupSearchRequest">>), Options).

%% @doc 
-spec update() ->
    {ok, grpcbox_client:stream()} | grpcbox_stream:grpc_error_response().
update() ->
    update(ctx:new(), #{}).

-spec update(ctx:t() | grpcbox_client:options()) ->
    {ok, grpcbox_client:stream()} | grpcbox_stream:grpc_error_response().
update(Ctx) when ?is_ctx(Ctx) ->
    update(Ctx, #{});
update(Options) ->
    update(ctx:new(), Options).

-spec update(ctx:t(), grpcbox_client:options()) ->
    {ok, grpcbox_client:stream()} | grpcbox_stream:grpc_error_response().
update(Ctx, Options) ->
    grpcbox_client:stream(Ctx, <<"/Search/Update">>, ?DEF(document_update, service_response, <<"DocumentUpdate">>), Options).

