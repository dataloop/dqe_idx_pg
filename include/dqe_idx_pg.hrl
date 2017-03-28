-define(MET_TABLE, "metrics").

-type collection() :: binary().
-type metric() :: [binary()].
-type bucket() :: binary().
-type key() :: [binary()].
-type row_id()  :: pos_integer().
-type sql_error() :: {'error', term()}.
-type not_found() :: {'error', not_found}.
-type tag_ns() :: binary().
-type tag_name() :: binary().
-type tag_value() :: binary().
-type tag() :: {tag_ns(), tag_name(), tag_value()}.
