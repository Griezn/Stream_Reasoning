cmake_minimum_required(VERSION 3.28)
project(SR C CXX)

set(CMAKE_C_STANDARD 11)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra -Werror")

if (CMAKE_BUILD_TYPE MATCHES "Debug")
    add_compile_options(-fsanitize=address,undefined)
    add_link_options(-fsanitize=address,undefined)
endif()

add_subdirectory(tests)

# Option to build benchmarks
option(BUILD_BENCHMARKS "Enable building benchmarks" OFF)

if (BUILD_BENCHMARKS)
    add_subdirectory(benchmark)
endif()

include_directories(include)

add_library(SR STATIC include/query.h src/query.c
        src/generator.c
        include/generator.h
        include/source.h
        include/data.h
        include/utils.h
        src/data.c
        include/defs.h
        include/file_source.h
        src/file_source.c
)

enable_testing()