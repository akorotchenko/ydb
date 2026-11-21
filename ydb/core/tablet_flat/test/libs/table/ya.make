LIBRARY()

SRCS(
    misc.cpp
    cache_context.cpp
)

PEERDIR(
    ydb/core/tablet_flat/test/libs/rows
    ydb/core/tablet_flat/test/libs/table/model
    ydb/core/tablet_flat
    ydb/core/testlib
)

END()

RECURSE(
    model
)
