//
// Created by Seppe Degryse on 14/10/2024.
//
#include <gtest/gtest.h>

extern "C" {
    #include "query.h"
}

bool check_join(int *in1, int *in2)
{
    return *in1 > *in2;
}

TEST(QueryTests, test_query_join_no_children) {
    int *ptr;
    operator_t join_op = {
        .type = JOIN,
        .left = NULL,
        .right = NULL,
        .params = {check_join}
    };
    query_t query_join = {.root = &join_op};

    // Program should abort because .left and .right are NULL
    ASSERT_DEATH(execute_query(&query_join, ptr, &ptr), "");
}

TEST(QueryTests, test_query_join_death_no_left_child) {
    int *ptr;
    operator_t join_op = {
        .type = JOIN,
        .left = NULL,
        .right = &join_op,
        .params = {check_join}
    };
    query_t query_join = {.root = &join_op};

    // Program should abort because .left is NULL
    ASSERT_DEATH(execute_query(&query_join, ptr, &ptr), "");
}

TEST(QueryTests, test_query_join_death_no_right_child) {
    int *ptr;
    operator_t join_op = {
        .type = JOIN,
        .left = &join_op,
        .right = NULL,
        .params = {check_join}
    };
    query_t query_join = {.root = &join_op};

    // Program should abort because .right is NULL
    ASSERT_DEATH(execute_query(&query_join, ptr, &ptr), "");
}


bool check_filter(int *in)
{
    return *in < 2;
}

TEST(QueryTests, test_query_filter) {
    int input = 0;
    int *out;

    operator_t filter_op = {
        .type = FILTER,
        .left = NULL,
        .right = NULL,
        .params.filter = {check_filter}
    };

    query_t query_filter = {.root = &filter_op};

    execute_query(&query_filter, &input, &out);
    ASSERT_EQ(*out, 1);

    free(out);
}