cmake_minimum_required(VERSION 3.28)
project(SR C CXX)

set(CMAKE_C_STANDARD 11)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if (CMAKE_C_COMPILER_ID STREQUAL "MSVC")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} /W4 /WX")
else()
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra -Werror")
    add_compile_options(-fsanitize=address,undefined)
    add_link_options(-fsanitize=address,undefined)
endif()

add_subdirectory(tests)

include_directories(include)

add_library(SR STATIC include/query.h src/query.c
        src/generator.c
        include/generator.h
        include/source.h
        include/data.h
        include/utils.h
        src/data.c
        include/defs.h
)

enable_testing()