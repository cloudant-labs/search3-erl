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
        ExistingSigs  = [element(10, Index) || Index <- Indexes],
        SigsFromList = get_sigs_from_list(Tx, DbPrefix),
        ClearList = lists:filter(fun (Sig) ->
            not lists:member(Sig, ExistingSigs)
        end, SigsFromList),
        clear_index_range(Tx, ClearList, DbPrefix),
        remove_indexes_from_list(Tx, ClearList, DbPrefix)
    end).

get_sigs_from_list(Tx, DbPrefix) ->
    Prefix = erlfdb_tuple:pack({?DB_SEARCH, ?INDEX_LIST}, DbPrefix),
    Opts = [{streaming_mode, want_all}],
    Rows = erlfdb:wait(erlfdb:get_range_startswith(Tx, Prefix, Opts)),
    [element(6, erlfdb_tuple:unpack(Key)) || {Key, _} <- Rows].

clear_index_range(Tx, ClearList, DbPrefix) ->
    lists:foreach(fun (Sig) ->
        SearchPrefix = erlfdb_tuple:pack({?DB_SEARCH, Sig}, DbPrefix),
        erlfdb:clear_range_startswith(Tx, SearchPrefix)
    end, ClearList).

remove_indexes_from_list(Tx, ClearList, DbPrefix) ->
    lists:foreach(fun (Sig) ->
        Prefix = search3_util:create_key(Sig, DbPrefix),
        erlfdb:clear(Tx, Prefix)
    end, ClearList).
