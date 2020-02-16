-module(search3_cleanup).

-export([
    clear_deleted_indexes/1
]).

-include("search3.hrl").
-include_lib("couch/include/couch_db.hrl").
-include_lib("fabric/include/fabric2.hrl").

clear_deleted_indexes(Db) ->
    #{
        db_prefix := DbPrefix
    } = Db,
    fabric2_fdb:transactional(Db, fun(TxDb) ->
        #{
            tx := Tx
        } = TxDb,
        Indexes = search3_util:list_indexes(TxDb),
        DDocIds  = [element(4, Index) || Index <- Indexes],
        IdsFromList = get_ddocids_from_list(Tx, DbPrefix),
        ClearList = lists:filter(fun ({Id, _Val}) ->
            not lists:member(Id, DDocIds)
        end, IdsFromList),
        clear_index_range(Tx, ClearList, DbPrefix),
        remove_indexes_from_list(Tx, ClearList, DbPrefix)
    end).

get_ddocids_from_list(Tx, DbPrefix) -> 
    Prefix = erlfdb_tuple:pack({?DB_SEARCH, ?INDEX_LIST}, DbPrefix),
    Opts = [{streaming_mode, want_all}],
    Rows = erlfdb:wait(erlfdb:get_range_startswith(Tx, Prefix, Opts)),
    [{element(6, erlfdb_tuple:unpack(Id)), Val} || {Id, Val} <- Rows].

clear_index_range(Tx, ClearList, DbPrefix) ->
    % SearchPrefix has a value:
    % erlfdb_tuple:pack({?DB_SEARCH, Sig}, DbPrefix)
    lists:foreach(fun ({_, SearchPrefix}) ->
        erlfdb:clear_range_startswith(Tx, SearchPrefix)
    end, ClearList).

remove_indexes_from_list(Tx, ClearList, DbPrefix) ->
    lists:foreach(fun ({Id, _}) ->
        Prefix = search3_util:create_key(Id, DbPrefix),
        erlfdb:clear(Tx, Prefix)
    end, ClearList).
