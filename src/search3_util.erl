-module(search3_util).

-export([design_doc_to_index/3,
    get_doc/2,
    validate/2,
    list_indexes/1,
    update_index_list/2]).

-include_lib("couch/include/couch_db.hrl").
-include_lib("fabric/include/fabric2.hrl").

-include("search3.hrl").

design_doc_to_index(#{db_prefix := DbPrefix}, #doc{id=Id,body={Fields}}, IndexName) ->
    Language = couch_util:get_value(<<"language">>, Fields, <<"javascript">>),
    {RawIndexes} = couch_util:get_value(<<"indexes">>, Fields, {[]}),
    InvalidDDocError = {invalid_design_doc,
        <<"index `", IndexName/binary, "` must have parameter `index`">>},
    case lists:keyfind(IndexName, 1, RawIndexes) of
        false ->
            {error, {not_found, <<IndexName/binary, " not found.">>}};
        {IndexName, {Index}} ->
            Analyzer = couch_util:get_value(<<"analyzer">>, Index, <<"standard">>),
            case couch_util:get_value(<<"index">>, Index) of
                undefined ->
                    {error, InvalidDDocError};
                Def ->
                    Hash = crypto:hash(sha256, term_to_binary({Analyzer, Def})),
                    Sig = ?l2b(couch_util:to_hex(Hash)),
                    IndexPrefix = erlfdb_tuple:pack({?DB_SEARCH, Sig}, DbPrefix),
                    {ok, #index{
                        analyzer=Analyzer,
                        ddoc_id=Id,
                        def=Def,
                        def_lang=Language,
                        name=IndexName,
                        prefix=IndexPrefix,
                        sig=Sig}}
            end;
        _ ->
            {error, InvalidDDocError}
    end.

design_doc_to_indexes(Db, #doc{body={Fields}}=Doc) ->
    RawIndexes = couch_util:get_value(<<"indexes">>, Fields, {[]}),
    case RawIndexes of
        {IndexList} when is_list(IndexList) ->
            {IndexNames, _} = lists:unzip(IndexList),
            lists:flatmap(
                fun(IndexName) ->
                    case (catch design_doc_to_index(Db, Doc, IndexName)) of
                        {ok, #index{}=Index} -> [Index];
                        _ -> []
                    end
                end,
                IndexNames);
        _ -> []
    end.

get_doc(Db, DocId) ->
    DocObj = case fabric2_db:open_doc(Db, DocId) of
        {ok, Doc} -> couch_doc:to_json_obj(Doc, []);
        {not_found, _} -> null
    end,
    {doc, DocObj}.

validate(Db, DDoc) ->
    Indexes = design_doc_to_indexes(Db, DDoc),
    Lang = <<"javascript">>,
    ValidateIndexes = fun(Proc, #index{def = Def, name = Name}) ->
        couch_query_servers:try_compile(Proc, search, Name, Def)
    end,
    Proc = couch_query_servers:get_os_process(Lang),
    try
        lists:foreach(fun(I) -> ValidateIndexes(Proc, I) end, Indexes)
    after
        couch_query_servers:ret_os_process(Proc)
    end.

list_indexes(Db) ->
    Acc0 = #{
        db => Db,
        rows => []
    },
    {ok, Indexes} = fabric2_db:fold_design_docs(Db,
        fun ddoc_fold_cb/2, Acc0, []),
    Indexes.

ddoc_fold_cb({meta, _}, Acc) ->
    {ok, Acc};
ddoc_fold_cb(complete, Acc) ->
    #{rows := Rows} = Acc,
    {ok, Rows};
ddoc_fold_cb({row, Row}, Acc) ->
    #{
        db := Db,
        rows := Rows
    } = Acc,
    DDoc = load_ddoc_row(Db, Row),
    Indexes = design_doc_to_indexes(Db, DDoc),
    {ok, Acc#{rows:= Rows ++ Indexes}}.

load_ddoc_row(Db, Row) ->
    {_, DDocId} = lists:keyfind(id, 1, Row),
    {ok, DDoc} = fabric2_db:open_doc(Db, DDocId),
    DDoc.

update_index_list(Db, Index) ->
    #{
        db_prefix := DbPrefix
    } = Db,
    #index{
        ddoc_id = Id
    } = Index,
    Key = create_index_key(Id, DbPrefix),
    fabric2_fdb:transactional(Db, fun(TxDb) ->
        add_index_to_list(TxDb, Key)
    end).

create_index_key(DDocId, DbPrefix) ->
    erlfdb_tuple:pack({?DB_SEARCH, ?INDEX_LIST, DDocId}, DbPrefix).

add_index_to_list(TxDb, Key) ->
    #{
        tx := Tx
    } = TxDb,
    erlfdb:set(Tx, Key, <<"0">>).

remove_index_from_list(TxDb, Key) ->
    #{
        tx := Tx
    } = TxDb,
    erlfdb:clear(Tx, Key).