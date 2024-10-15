//
// Created by Seppe Degryse on 14/10/2024.
//
#include <gtest/gtest.h>

extern "C" {
    #include "query.h"
}

bool check_join(const int *in1, const int *in2)
{
    return *in1 > *in2;
}

TEST(QueryTests, test_query_join_death_no_children)
{
    int *ptr = nullptr;
    operator_t join_op = {
        .type = JOIN,
        .left = nullptr,
        .right = nullptr,
        .params = {check_join}
    };
    query_t query_join = {.root = &join_op};

    // Program should abort because .left and .right are NULL
    ASSERT_DEATH(execute_query(&query_join, ptr, &ptr), "");
}

TEST(QueryTests, test_query_join_death_no_left_child)
{
    int *ptr = nullptr;
    operator_t join_op = {
        .type = JOIN,
        .left = nullptr,
        .right = &join_op,
        .params = {check_join}
    };
    query_t query_join = {.root = &join_op};

    // Program should abort because .left is NULL
    ASSERT_DEATH(execute_query(&query_join, ptr, &ptr), "");
}

TEST(QueryTests, test_query_join_death_no_right_child)
{
    int *ptr = nullptr;
    operator_t join_op = {
        .type = JOIN,
        .left = &join_op,
        .right = nullptr,
        .params = {check_join}
    };
    query_t query_join = {.root = &join_op};

    // Program should abort because .right is NULL
    ASSERT_DEATH(execute_query(&query_join, ptr, &ptr), "");
}


bool check_filter(const int *in)
{
    return *in < 2;
}

TEST(QueryTests, test_query_filter)
{
    int input = 0;
    int *out;

    operator_t filter_op = {
        .type = FILTER,
        .left = nullptr,
        .right = nullptr,
        .params = {.filter = check_filter}
    };

    query_t query_filter = {.root = &filter_op};

    execute_query(&query_filter, &input, &out);
    ASSERT_EQ(*out, 1);

    free(out);
}


TEST(QueryTests, test_query_filter2)
{
    int input = 0;
    int *out;

    operator_t filter_op = {
        .type = FILTER,
        .left = nullptr,
        .right = nullptr,
        .params = {.filter = check_filter}
    };

    operator_t filter_op2 = {
        .type = FILTER,
        .left = &filter_op,
        .right = nullptr,
        .params = {.filter = check_filter}
    };

    query_t query_filter = {.root = &filter_op2};

    execute_query(&query_filter, &input, &out);
    ASSERT_EQ(*out, 2);

    free(out);
}


TEST(QueryTests, test_query_window)
{
    int input = 1;
    int *out;

    operator_t window_op = {
        .type = WINDOW,
        .left = nullptr,
        .right = nullptr,
        .params = {.window = 10}
    };

    query_t query_window = {.root = &window_op};

    execute_query(&query_window, &input, &out);
    ASSERT_EQ(*out, 10);

    free(out);
}


TEST(QueryTests, test_query_window2)
{
    int input = 1;
    int *out;

    operator_t window_op = {
        .type = WINDOW,
        .left = nullptr,
        .right = nullptr,
        .params = {.window = 10}
    };

    operator_t window_op2 = {
        .type = WINDOW,
        .left = &window_op,
        .right = nullptr,
        .params = {.window = 5}
    };

    query_t query_window = {.root = &window_op2};

    execute_query(&query_window, &input, &out);
    ASSERT_EQ(*out, 50);

    free(out);
}


TEST(QueryTests, test_query_join)
{
    int input = 1;
    int *out;

    operator_t window_op = {
        .type = WINDOW,
        .left = nullptr,
        .right = nullptr,
        .params = {.window = 10}
    };

    operator_t filter_op = {
        .type = FILTER,
        .left = nullptr,
        .right = nullptr,
        .params = {.filter = check_filter}
    };

    operator_t join_op = {
        .type = JOIN,
        .left = &window_op,
        .right = &filter_op,
        .params = {.join = check_join}
    };

    query_t query = {.root = &join_op};

    execute_query(&query, &input, &out);

    ASSERT_EQ(*out, 12);

    free(out);
}