LIBRARY()

SRCS(
    misc.cpp
)

SRCS(GLOBAL shared_cache_context.cpp)

PEERDIR(
    library/cpp/testing/hook
    ydb/core/tablet_flat/test/libs/rows
    ydb/core/tablet_flat/test/libs/table/model
    ydb/core/tablet_flat
    ydb/core/testlib/default
)

END()

RECURSE(
    model
)
