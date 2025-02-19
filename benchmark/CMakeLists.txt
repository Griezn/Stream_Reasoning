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
        data/traffic_data.hpp
        Query1.cpp
        query2.cpp
        data/weather_data.hpp
        default_checks.hpp
        Query7.cpp
        data/parking_data.hpp
)

target_link_libraries(SR_BENCH SR benchmark::benchmark benchmark::benchmark_main)