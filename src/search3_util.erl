-module(search3_util).

-export([design_doc_to_index/3, get_doc/2]).

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

get_doc(Db, DocId) ->
    DocObj = case fabric2_db:open_doc(Db, DocId) of
        {ok, Doc} -> couch_doc:to_json_obj(Doc, []);
        {not_found, _} -> null
    end,
    {doc, DocObj}.
