-define(SEARCH_JOB_TYPE, <<"search">>).

-record(index, {
    current_seq=0,
    dbname,
    ddoc_id,
    analyzer,
    def,
    def_lang,
    name,
    sig=nil,
    session = <<>>
}).

-record(grouping, {
    by=nil,
    groups=[],
    offset=0,
    limit=10,
    sort=relevance,
    new_api=true
}).

-record(index_query_args, {
    q,
    partition=nil,
    limit=25,
    stale=false,
    include_docs=false,
    bookmark=nil,
    sort=relevance,
    grouping=#grouping{},
    stable=false,
    counts=nil,
    ranges=nil,
    drilldown=[],
    include_fields=nil,
    highlight_fields=nil,
    highlight_pre_tag = <<"<em>">>,
    highlight_post_tag = <<"</em>">>,
    highlight_number=1,
    highlight_size=0,
    raw_bookmark=false
}).

-record(sortable, {
    order, % sort order
    % shard, % originating shard
    item   % the item itself
}).

-record(top_docs, {
    update_seq,
    total_hits,
    hits,
    counts,
    ranges
}).

-record(hit, {
    order,
    fields
}).
