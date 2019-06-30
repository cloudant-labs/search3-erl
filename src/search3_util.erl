
-module(search3_util).


-export([design_doc_to_index/2, design_doc_to_indexes/1]).


-include_lib("couch/include/couch_db.hrl").
-include("search3.hrl").


design_doc_to_indexes(#doc{body={Fields}}=Doc) ->
    RawIndexes = couch_util:get_value(<<"indexes">>, Fields, {[]}),
    case RawIndexes of
        {IndexList} when is_list(IndexList) ->
            {IndexNames, _} = lists:unzip(IndexList),
            lists:flatmap(
                fun(IndexName) ->
                    case (catch design_doc_to_index(Doc, IndexName)) of
                        {ok, #index{}=Index} -> [Index];
                        _ -> []
                    end
                end,
                IndexNames);
        _ -> []
    end.


design_doc_to_index(#doc{id=Id,body={Fields}}, IndexName) ->
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
                    Sig = ?l2b(couch_util:to_hex(crypto:hash(md5,
                        term_to_binary({Analyzer, Def})))),
                    {ok, #index{
                        analyzer=Analyzer,
                        ddoc_id=Id,
                        def=Def,
                        def_lang=Language,
                        name=IndexName,
                        sig=Sig}}
            end;
        _ ->
            {error, InvalidDDocError}
    end.


