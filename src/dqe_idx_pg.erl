-module(dqe_idx_pg).
-behaviour(dqe_idx).

-include_lib("dqe_idx_pg/include/dqe_idx_pg.hrl").

-include_lib("eunit/include/eunit.hrl").
-compile([{nowarn_unused_function, [{defaut_pool_config_test,0},
                                    {extra_pool_config_test,0},
                                    {extra_pool_init_test,0}]}]).

%% API exports
-export([
         init/0,
         lookup/4, lookup/5, lookup_tags/1,
         collections/0, metrics/1, metrics/2, metrics/3,
         namespaces/1, namespaces/2,
         tags/2, tags/3, values/3, values/4, expand/2,
         add/5, add/6, update/5, touch/1,
         delete/4, delete/5
        ]).

-export_type([sql_stmt/0]).

-type row_id()  :: pos_integer().
-type sql_error() :: {'error', term()}.
-type not_found() :: {'error', not_found}.
-type sql_stmt() :: {ok, iolist(), [term()]}.

-define(POOL_MAP_TAB, dqe_idx_pg_poolmap).

-define(TIMEOUT, 5 * 1000).
%% 1 hour in milliseconds

-define(HOUR, 3600000).
-define(GRACE, ?HOUR).
-define(S1970, 62167219200).

%%====================================================================
%% API functions
%%====================================================================

init() ->
    ets:new(?POOL_MAP_TAB, [named_table]),
    {ok, PoolConfigs} = application:get_env(dqe_idx_pg, pool),
    [init_pool(Pool, Config) || {Pool, Config} <- PoolConfigs],
    ok.

init_pool(Pool, Config) ->
    Opts = [size, max_overflow, database, username, password],
    Opts1 = [{O, proplists:get_value(dqe_idx_pg, Config, undefined)}
         || O <- Opts],
    {Host, Port} = case proplists:get_value(server, Config) of
                       {H, P} ->
                           {H, P};
                       _ ->
                           H = proplists:get_value(host, Config),
                           P = proplists:get_value(port, Config),
                           {H, P}
                   end,
    pgapp:connect(Pool, [{host, Host}, {port, Port} | Opts1]),

    CollectionsProp = proplists:get_value(collections, Config, ""),
    Collections = string:tokens(CollectionsProp, ", "),
    Rows = [{list_to_binary(C), Pool} || C <- Collections],
    ets:insert(?POOL_MAP_TAB, Rows),

    sql_migration:run(dqe_idx_pg, Pool).

lookup(Query, Start, Finish, Opts) ->
    Grace = proplists:get_value(grace, Opts, ?GRACE),
    {ok, Q, Vs} = query_builder:lookup_query(
                    Query, Start - Grace, Finish + Grace, []),
    Collection = lookup_collection(Query),
    {ok, Rows} = execute({select, "lookup/1", Collection, Q, Vs}),
    Rows1 = [translate_row(Row, Start, Finish, Grace) || Row <- Rows],
    {ok, Rows1}.

lookup(Query, Start, Finish, Groupings, Opts) ->
    Grace = proplists:get_value(grace, Opts, ?GRACE),
    {ok, Q, Vs} = query_builder:lookup_query(
                    Query, Start - Grace, Finish + Grace, Groupings),
    Collection = lookup_collection(Query),
    {ok, Rows} = execute({select, "lookup/2", Collection, Q, Vs}),
    Rows1 = [{translate_row({Bucket, Key, S, F}, Start, Finish, Grace),
              get_values(Groupings, Dimensions)} ||
                {Bucket, Key, S, F, Dimensions} <- Rows],
    {ok, Rows1}.

lookup_tags(Query) ->
    {ok, Q, Vs} = query_builder:lookup_tags_query(Query),
    Collection = lookup_collection(Query),
    {ok, Rows} = execute({select, "lookup_tags/1", Collection, Q, Vs}),
    R = [dqe_idx_pg_utils:kvpair_to_tag(KV) || KV <- Rows],
    {ok, R}.

collections() ->
    {ok, Q, Vs} = query_builder:collections_query(),
    {ok, Rows} = execute({select, "collections/0", Q, Vs}),
    {ok, strip_tpl(Rows)}.

metrics(Collection) ->
    {ok, Q, Vs} = query_builder:metrics_query(Collection),
    {ok, Rows} = execute({select, "metrics/1", Collection, Q, Vs}),
    R = [M || {M} <- Rows],
    {ok, R}.

metrics(Collection, Tags) ->
    {ok, Q, Vs} = query_builder:metrics_query(Collection, Tags),
    {ok, Rows} = execute({select, "metrics/1", Collection, Q, Vs}),
    R = [M || {M} <- Rows],
    {ok, R}.

metrics(Collection, Prefix, Depth) ->
    {ok, Q, Vs} = query_builder:metrics_query(Collection, Prefix, Depth),
    {ok, Rows} = execute({select, "metrics/3", Collection, Q, Vs}),
    R = [M || {M} <- Rows],
    {ok, R}.

namespaces(Collection) ->
    {ok, Q, Vs} = query_builder:namespaces_query(Collection),
    {ok, Rows} = execute({select, "namespaces/1", Collection, Q, Vs}),
    {ok, decode_ns_rows(Rows)}.

namespaces(Collection, Metric) ->
    {ok, Q, Vs} = query_builder:namespaces_query(Collection, Metric),
    {ok, Rows} = execute({select, "namespaces/2", Collection, Q, Vs}),
    {ok, decode_ns_rows(Rows)}.

tags(Collection, Namespace) ->
    {ok, Q, Vs} = query_builder:tags_query(Collection, Namespace),
    {ok, Rows} = execute({select, "tags/2", Collection, Q, Vs}),
    {ok, strip_tpl(Rows)}.

tags(Collection, Metric, Namespace) ->
    {ok, Q, Vs} = query_builder:tags_query(Collection, Metric, Namespace),
    {ok, Rows} = execute({select, "tags/3", Collection, Q, Vs}),
    {ok, strip_tpl(Rows)}.

values(Collection, Namespace, Tag) ->
    {ok, Q, Vs} = query_builder:values_query(Collection, Namespace, Tag),
    {ok, Rows} = execute({select, "values/3", Collection, Q, Vs}),
    {ok, strip_tpl(Rows)}.

values(Collection, Metric, Namespace, Tag) ->
    {ok, Q, Vs} = query_builder:values_query(Collection, Metric,
                                             Namespace, Tag),
    {ok, Rows} = execute({select, "values/4", Collection, Q, Vs}),
    {ok, strip_tpl(Rows)}.

expand(Bucket, []) when is_binary(Bucket) ->
    {ok, {Bucket, []}};

expand(Bucket, Globs) when
      is_binary(Bucket),
      is_list(Globs) ->
    {ok, Q, Vs} = query_builder:glob_query(Bucket, Globs),
    {ok, Rows} = execute({select, "expand/2", Q, Vs}),
    Metrics = [M || {M} <- Rows],
    {ok, {Bucket, Metrics}}.


touch(Data) ->
    {ok, Q, Vs} = command_builder:touch(Data),
    case execute({command, "touch/1", Q, Vs}) of
        {ok, 0, []} ->
            ok;
        {ok, _Count, _} ->
            ok;
        EAdd ->
            EAdd
    end.

-spec add(dqe_idx:collection(),
          dqe_idx:metric(),
          dqe_idx:bucket(),
          dqe_idx:key(),
          dqe_idx:timestamp()) -> ok | {ok, row_id()} | sql_error().
add(Collection, Metric, Bucket, Key, Timestamp) ->
    add(Collection, Metric, Bucket, Key, Timestamp, []).

-spec add(dqe_idx:collection(),
          dqe_idx:metric(),
          dqe_idx:bucket(),
          dqe_idx:key(),
          dqe_idx:timestamp(),
          dqe_idx:tags()) -> ok | {ok, row_id()} | sql_error().
%% TODO: handle timestamp
add(Collection, Metric, Bucket, Key, Timestamp, Tags) ->
    {ok, Q, Vs} = command_builder:add_metric(
                    Collection, Metric, Bucket, Key, Timestamp, Tags),
    case execute({command, "add/6", Collection, Q, Vs}) of
        {ok, 0, []} ->
            ok;
        {ok, _Count, [{Dims}]} ->
            {ok, dqe_idx_pg_utils:hstore_to_tags(Dims)};
        EAdd ->
            EAdd
    end.

-spec update(dqe_idx:collection(),
             dqe_idx:metric(),
             dqe_idx:bucket(),
             dqe_idx:key(),
             dqe_idx:tags()) -> {ok, row_id()} | not_found() | sql_error().
update(Collection, Metric, Bucket, Key, NVs) ->
    {ok, Q, Vs} = command_builder:update_tags(
                    Collection, Metric, Bucket, Key, NVs),
    case execute({command, "update/5", Collection, Q, Vs}) of
        {ok, 0, []} ->
            ok;
        {ok, _Count, [{Dims}]} ->
            {ok, dqe_idx_pg_utils:hstore_to_tags(Dims)};
        EAdd ->
            EAdd
    end.

-spec delete(dqe_idx:collection(),
             dqe_idx:metric(),
             dqe_idx:bucket(),
             dqe_idx:key()) -> ok | sql_error().
delete(Collection, Metric, Bucket, Key) ->
    {ok, Q, Vs} = command_builder:delete_metric(Collection, Metric,
                                                Bucket, Key),
    case execute({command, "delete/4", Collection, Q, Vs}) of
        {ok, _Count, _Rows} ->
            ok;
        E ->
            E
    end.

-spec delete(dqe_idx:collection(),
             dqe_idx:metric(),
             dqe_idx:bucket(),
             dqe_idx:key(),
             [dqe_idx:tag()]) -> ok | sql_error().
delete(Collection, Metric, Bucket, Key, Tags) ->
    {ok, Q, Vs} = command_builder:delete_tags(
                    Collection, Metric, Bucket, Key, Tags),
    case execute({command, "delete/5", Collection, Q, Vs}) of
        {ok, _Count, _Rows} ->
            ok;
        E ->
            E
    end.

%%====================================================================
%% Internal functions
%%====================================================================

tdelta(T0) ->
    (erlang:system_time(nano_seconds) - T0)/1000/1000.

strip_tpl(L) ->
    [E || {E} <- L].

decode_ns_rows(Rows) ->
    [dqe_idx_pg_utils:decode_ns(E) || {E} <- Rows].

execute({select, Name, Q, Vs}) ->
    lists:foldl(fun (Pool, {ok, AccRows}) ->
                    case execute({select, Name, Pool, Q, Vs}) of
                        {ok, Rows} ->
                            {ok, AccRows ++ Rows};
                        Err ->
                            Err
                    end;
                    (_, Err) ->
                        Err
                end, {ok, []}, all_pg_pools());
execute({command, Name, Q, Vs}) ->
    lists:foldl(fun (Pool, {ok, AccCount, AccRows}) ->
                    case execute({command, Name, Pool, Q, Vs}) of
                        {ok, Count, Rows} ->
                            {ok, AccCount + Count, AccRows ++ Rows};
                        Err ->
                            Err
                    end;
                    (_, Err) ->
                        Err
                end, {ok, 0, []}, all_pg_pools());
execute({select, Name, PoolOrCollection, Q, Vs}) ->
    T0 = erlang:system_time(nano_seconds),
    Pool = pg_pool(PoolOrCollection),
    case pgapp:equery(Pool, Q, Vs, timeout()) of
        {ok, _Cols, Rows} ->
            lager:debug("[dqe_idx:pg:~p] PG Query took ~pms: ~s <- ~p",
                        [Name, tdelta(T0), Q, Vs]),
            {ok, Rows};
        E ->
            report_error(Name, Q, Vs, T0, E)
    end;
execute({command, Name, PoolOrCollection, Q, Vs}) ->
    T0 = erlang:system_time(nano_seconds),
    Pool = pg_pool(PoolOrCollection),
    case pgapp:equery(Pool, Q, Vs, timeout()) of
        {ok, Count} ->
            lager:debug("[dqe_idx:pg:~p] PG Query took ~pms: ~s <- ~p",
                        [Name, tdelta(T0), Q, Vs]),
            {ok, Count, []};
        {ok, Count, _Cols, Rows} ->
            lager:debug("[dqe_idx:pg:~p] PG Query took ~pms: ~s <- ~p",
                        [Name, tdelta(T0), Q, Vs]),
            {ok, Count, Rows};
        E ->
            report_error(Name, Q, Vs, T0, E)
    end.

report_error(Name, Q, Vs, T0, E) ->
    lager:info("[dqe_idx:pg:~p] PG Query failed after ~pms: ~s <- ~p:"
               " ~p", [Name, tdelta(T0), Q, Vs, E]),
    E.

get_values(Grouping, {KVs}) when is_list(KVs) ->
    Tags = [dqe_idx_pg_utils:kvpair_to_tag(KV) || KV <- KVs],
    get_values(Grouping, Tags, []).

get_values([], _Tags, Acc) ->
    lists:reverse(Acc);
get_values([TagKey | Rest], Tags, Acc) ->
    Value = get_tag_value(TagKey, Tags),
    get_values(Rest, Tags, [Value | Acc]).

get_tag_value({_, _}, []) ->
    undefined;
get_tag_value({Ns, Name}, [{TNs, TName, TValue} | _])
  when Ns =:= TNs, Name =:= TName ->
    TValue;
get_tag_value(TagKey, [_ | Rest]) ->
    get_tag_value(TagKey, Rest).

timeout() ->
    application:get_env(dqe_idx_pg, timeout, ?TIMEOUT).

date_to_ms({Date , {H, M, S}}) ->
    GSecs = calendar:datetime_to_gregorian_seconds({Date, {H, M, round(S)}}),
    Secs = GSecs - ?S1970,
    erlang:convert_time_unit(Secs, second, millisecond).

translate_row({B, K, StartD, FinishD}, Start, Finish, Grace) ->
    StartMSG = date_to_ms(StartD) - Grace,
    FinishMSG = date_to_ms(FinishD) + Grace,
    %% If our Finish, including grace goes further then the
    %% requested finish we do not need to padd the end
    %% otherwise we padd with null.
    Cs0 = case FinishMSG of
              FinishMSG when FinishMSG >= Finish ->
                  [];
              _ ->
                  [{FinishMSG + 1, Finish, null}]
          end,
    %% The datachunk, we take the maxiumum of the  requested and he
    %% found chunk as start end the minimum of the requested and found
    %% finish.
    Cs1 = [{max(StartMSG, Start), min(FinishMSG, Finish), default} | Cs0],
    Cs2 = case StartMSG of
              StartMSG when StartMSG =< Start ->
                  Cs1;
              _ ->
                  [{Start, StartMSG -1, null} | Cs1]
          end,
    {B, K, Cs2}.

lookup_collection({in, Collection, _Metric}) ->
    Collection;
lookup_collection({in, Collection, _Metric, _Where}) ->
    Collection.

pg_pool(Pool) when is_atom(Pool) ->
    Pool;
pg_pool(Collection) ->
    case ets:lookup(?POOL_MAP_TAB, Collection) of
        [{_Col, Pool}| _] ->
            Pool;
        [] ->
            default
    end.

all_pg_pools() ->
    Pools = [P || [P] <- ets:match(?POOL_MAP_TAB, {'_', '$1'})],
    Pools1 = [default | Pools],
    lists:usort(Pools1).

%%====================================================================
%% test functions
%%====================================================================

defaut_pool_config_test() ->
    Conf = [{["idx", "pg", "backend"], "127.0.0.2:5432"}],
    Config = cuttlefish_unit:generate_config("priv/dqe_idx_pg.schema", Conf),
    cuttlefish_unit:assert_valid_config(Config),
    cuttlefish_unit:assert_config(Config, "dqe_idx_pg.pool.default.server",
                                  {"127.0.0.2", 5432}),
    cuttlefish_unit:assert_config(Config, "dqe_idx_pg.pool.default.database",
                                  "metric_metadata").

extra_pool_config_test() ->
    Conf = [{["idx", "pg", "extra1", "backend"], "127.0.0.2:5432"},
            {["idx", "pg", "extra1", "collections"], "col1, col2"},
            {["idx", "pg", "extra2", "database"], "extra_db"},
            {["idx", "pg", "extra2", "collections"], "col3"}],
    Config = cuttlefish_unit:generate_config("priv/dqe_idx_pg.schema", Conf),
    cuttlefish_unit:assert_valid_config(Config),
    cuttlefish_unit:assert_config(Config, "dqe_idx_pg.pool.default.host",
                                  "127.0.0.1"),
    cuttlefish_unit:assert_config(Config, "dqe_idx_pg.pool.default.port",
                                  5432),
    cuttlefish_unit:assert_config(Config, "dqe_idx_pg.pool.extra1.server",
                                  {"127.0.0.2", 5432}),
    cuttlefish_unit:assert_config(Config, "dqe_idx_pg.pool.extra1.database",
                                  "metric_metadata"),
    cuttlefish_unit:assert_config(Config, "dqe_idx_pg.pool.extra2.server",
                                  {"127.0.0.1", 5432}),
    cuttlefish_unit:assert_config(Config, "dqe_idx_pg.pool.extra2.database",
                                  "extra_db").

extra_pool_init_test() ->
    Conf = [{["idx", "pg", "database"], "metric0"},
            {["idx", "pg", "extra1", "database"], "metric1"},
            {["idx", "pg", "extra1", "collections"], "col1"},
            {["idx", "pg", "extra2", "database"], "metric2"},
            {["idx", "pg", "extra2", "collections"], "col2"}],
    Config = cuttlefish_unit:generate_config("priv/dqe_idx_pg.schema", Conf),
    {ok, PoolConfig} = cuttlefish_unit:path(["dqe_idx_pg", "pool"], Config),

    meck:new(application, [unstick]),
    meck:new(pgapp, []),
    meck:new(sql_migration, []),
    meck:expect(application, get_env, fun(_, pool) -> {ok, PoolConfig} end),
    meck:expect(pgapp, connect, fun(_, _) -> ok end),
    meck:expect(sql_migration, run, fun(_, _) -> ok end),

    init(),

    ?assert(meck:called(pgapp, connect, [default, '_'])),
    ?assert(meck:called(pgapp, connect, [extra1, '_'])),
    ?assert(meck:called(pgapp, connect, [extra2, '_'])),
    ?assert(meck:called(sql_migration, run, ['_', default])),
    ?assert(meck:called(sql_migration, run, ['_', extra1])),
    ?assert(meck:called(sql_migration, run, ['_', extra2])),
    ?assertEqual([default, extra1, extra2], all_pg_pools()),
    ?assertEqual(extra1, pg_pool(<<"col1">>)),
    ?assertEqual(default, pg_pool(<<"unknown">>)),
    meck:unload(application),
    meck:unload(pgapp),
    meck:unload(sql_migration).
