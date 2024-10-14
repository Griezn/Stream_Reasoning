cmake_minimum_required(VERSION 3.28)
project(SR C CXX)

set(CMAKE_C_STANDARD 11)
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra -Werror")

add_subdirectory(tests)

include_directories(include)

add_library(SR STATIC include/query.h src/query.c)