-module(search3_cleanup).

-export([
    clear_unreachable_indexes/1
]).

-include("search3.hrl").
-include_lib("couch/include/couch_db.hrl").
-include_lib("fabric/include/fabric2.hrl").

clear_unreachable_indexes(Db) ->
    Batch = config:get_integer("search3", "index_delete_batch", 300),
    #{
        db_prefix := DbPrefix
    } = Db,
    Remaining = fabric2_fdb:transactional(Db, fun(TxDb) ->
        #{
            tx := Tx
        } = TxDb,
        Indexes = search3_util:list_indexes(TxDb),
        ExistingSigs  = [Index#index.sig || Index <- Indexes],
        SigsFromList = get_sigs_from_list(Tx, DbPrefix),
        ClearList = lists:filter(fun (Sig) ->
            not lists:member(Sig, ExistingSigs)
        end, SigsFromList),
        ListToClear = lists:sublist(ClearList, Batch),
        lists:foreach(fun (Sig) ->
            SearchPrefix = erlfdb_tuple:pack({?DB_SEARCH, Sig},
                DbPrefix),
            ListPrefix = search3_util:create_key(Sig, DbPrefix),
            erlfdb:clear_range_startswith(Tx, SearchPrefix),
            erlfdb:clear(Tx, ListPrefix)
        end, ListToClear),
        length(ClearList) - Batch
    end),
    case Remaining of
        N when N > 0 ->
            clear_unreachable_indexes(Db);
        _ ->
         ok
    end.

get_sigs_from_list(Tx, DbPrefix) ->
    Prefix = erlfdb_tuple:pack({?DB_SEARCH, ?INDEX_LIST}, DbPrefix),
    Opts = [{streaming_mode, want_all}],
    Rows = erlfdb:wait(erlfdb:get_range_startswith(Tx, Prefix, Opts)),
    lists:map(fun ({Key, _}) ->
        {_, _, _, _, _, Sig} = erlfdb_tuple:unpack(Key),
        Sig
    end, Rows).
