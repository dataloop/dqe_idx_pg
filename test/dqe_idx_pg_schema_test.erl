-module(dqe_idx_pg_schema_test).

-include_lib("eunit/include/eunit.hrl").

pools_test() -> 
    Conf = [],
    Config = cuttlefish_unit:generate_templated_config(
               "priv/dqe_idx_pg.schema", Conf, []),    
    cuttlefish_unit:assert_config(Config, "riak_core.ring_creation_size", 8),
    cuttlefish_unit:assert_config(Config, "riak_core.ssl.certfile",
                                  "/absolute/etc/cert.pem").
