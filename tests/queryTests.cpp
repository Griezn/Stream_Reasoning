//
// Created by Seppe Degryse on 14/10/2024.
//
#include <gtest/gtest.h>

#include "utils.hpp"

extern "C" {
    #include "query.h"
    #include "source.h"
    #include "generator.h"
}


class QueryTestFixture : public ::testing::Test {
protected:
    source_t gsource = {};
    sink_t gsink = {};

    void SetUp() override
    {
        gsource = create_generator_source();
        gsink = create_generator_sink();
    }

    void TearDown() override
    {
        free_generator_source(&gsource);
        free_generator_sink(&gsink);
    }
};


bool check_join(const int in1, const int in2)
{
    return in1 > in2;
}

TEST_F(QueryTestFixture, test_query_join_death_no_children)
{
    operator_t join_op = {
        .type = JOIN,
        .left = nullptr,
        .right = nullptr,
        .params = {.join= {check_join}}
    };
    query_t query_join = {.root = &join_op};

    // Program should abort because .left and .right are NULL
    ASSERT_DEATH(execute_query(&query_join, &gsource, &gsink), "");
}

TEST_F(QueryTestFixture, test_query_join_death_no_left_child)
{
    operator_t join_op = {
        .type = JOIN,
        .left = nullptr,
        .right = &join_op,
        .params = {.join = {check_join}}
    };
    query_t query_join = {.root = &join_op};

    // Program should abort because .left is NULL
    ASSERT_DEATH(execute_query(&query_join, &gsource, &gsink), "");
}

TEST_F(QueryTestFixture, test_query_join_death_no_right_child)
{
    operator_t join_op = {
        .type = JOIN,
        .left = &join_op,
        .right = nullptr,
        .params = {.join = {check_join}}
    };
    query_t query_join = {.root = &join_op};

    // Program should abort because .right is NULL
    ASSERT_DEATH(execute_query(&query_join, &gsource, &gsink), "");
}


bool check_filter(const int in)
{
    return in < 10;
}

TEST_F(QueryTestFixture, test_query_filter)
{
    operator_t filter_op = {
        .type = FILTER,
        .left = nullptr,
        .right = nullptr,
        .params = {.filter = {check_filter}}
    };

    query_t query_filter = {.root = &filter_op};

    execute_query(&query_filter, &gsource, &gsink);
    int expected[10] = {17, 11, 17, 16, 2, 17, 9, 12, 15, 14};
    ASSERT_ARR_EQ(gsink.buffer.data, expected, 10);
}


TEST_F(QueryTestFixture, test_query_filter2)
{
    operator_t filter_op = {
        .type = FILTER,
        .left = nullptr,
        .right = nullptr,
        .params = {.filter = {check_filter}}
    };

    operator_t filter_op2 = {
        .type = FILTER,
        .left = &filter_op,
        .right = nullptr,
        .params = {.filter = {check_filter}}
    };

    query_t query_filter = {.root = &filter_op2};

    execute_query(&query_filter, &gsource, &gsink);
    int expected[10] = {16, 10, 16, 15, 3, 16, 10, 11, 14, 13};
    ASSERT_ARR_EQ(gsink.buffer.data, expected, 10);
}


TEST_F(QueryTestFixture, test_query_window)
{
    operator_t window_op = {
        .type = WINDOW,
        .left = nullptr,
        .right = nullptr,
        .params = {.window = {7}}
    };

    query_t query_window = {.root = &window_op};

    execute_query(&query_window, &gsource, &gsink);
    int expected[7] = {18, 12, 18, 17, 1, 18, 10};
    ASSERT_ARR_EQ(gsink.buffer.data, expected, 7);
    ASSERT_EQ(gsource.buffer.size, 3);
}


TEST_F(QueryTestFixture, test_query_window2)
{
    operator_t window_op2 = {
        .type = WINDOW,
        .left = nullptr,
        .right = nullptr,
        .params = {.window = 10}
    };

    operator_t window_op = {
        .type = WINDOW,
        .left = &window_op2,
        .right = nullptr,
        .params = {.window = 5}
    };

    query_t query_window = {.root = &window_op};

    execute_query(&query_window, &gsource, &gsink);
    int expected[10] = {18, 12, 18, 17, 1};
    ASSERT_ARR_EQ(gsink.buffer.data, expected, 5);
}


bool check_filter2(const int in)
{
    return in < 15;
}


TEST_F(QueryTestFixture, test_query_join)
{
    operator_t filter_op = {
        .type = FILTER,
        .left = nullptr,
        .right = nullptr,
        .params = {.filter = {check_filter}}
    };

    operator_t filter2_op = {
        .type = FILTER,
        .left = nullptr,
        .right = nullptr,
        .params = {.filter = {check_filter2}}
    };

    operator_t join_op = {
        .type = JOIN,
        .left = &filter_op,
        .right = &filter2_op,
        .params = {.join = check_join}
    };

    query_t query = {.root = &join_op};

    execute_query(&query, &gsource, &gsink);

    int expected[10] = {34, 24, 34, 32, 4, 34, 20, 26, 30, 28};
    ASSERT_ARR_EQ(gsink.buffer.data, expected, 10);
}