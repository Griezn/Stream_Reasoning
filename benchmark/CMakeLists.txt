include(FetchContent)

FetchContent_Declare(
        googlebenchmark
        GIT_REPOSITORY https://github.com/google/benchmark.git
        GIT_TAG origin/main
)

FetchContent_MakeAvailable(googlebenchmark)

include_directories(../include)

add_executable(
        SR_BENCH
        QueryBenchmarks.cpp
)

target_link_libraries(SR_BENCH benchmark::benchmark benchmark::benchmark_main)