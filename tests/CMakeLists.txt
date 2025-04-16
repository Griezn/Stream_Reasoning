include(FetchContent)
FetchContent_Declare(
        googletest
        URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
)
# For Windows: Prevent overriding the parent project's compiler/linker settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

enable_testing()

include_directories(../include)

add_executable(
        SR_tests
        queryTests.cpp
        test_utils.hpp
        dataTests.cpp
        benchTests.cpp
        QueueTests.cpp
        HashTests.cpp
)
target_link_libraries(
        SR_tests
        GTest::gtest_main
        SR
)

include(GoogleTest)
gtest_discover_tests(SR_tests)