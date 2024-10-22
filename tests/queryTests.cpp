//
// Created by Seppe Degryse on 14/10/2024.
//
#include <gtest/gtest.h>

#include "test_utils.hpp"

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


bool check_join(const triple_t in1, const triple_t in2)
{
    return in1.object == in2.object;
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


bool check_filter(const triple_t in)
{
    return in.predicate == PREDICATE_HAS_SKILL;
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
    triple_t expected[8] = {
        {SUBJECT_ALICE, PREDICATE_HAS_SKILL, OBJECT_PROGRAMMING},
        {SUBJECT_BOB, PREDICATE_HAS_SKILL, OBJECT_DATA_ANALYSIS},
        {SUBJECT_CHARLIE, PREDICATE_HAS_SKILL, OBJECT_PROGRAMMING},
        {SUBJECT_DAVID, PREDICATE_HAS_SKILL, OBJECT_PROGRAMMING},
        {SUBJECT_EMILY, PREDICATE_HAS_SKILL, OBJECT_PROGRAMMING},
        {SUBJECT_FRANK, PREDICATE_HAS_SKILL, OBJECT_GRAPHIC_DESIGN},
        {SUBJECT_GRACE, PREDICATE_HAS_SKILL, OBJECT_MARKETING},
        {SUBJECT_HELEN, PREDICATE_HAS_SKILL, OBJECT_PROJECT_MANAGEMENT},
    };
    ASSERT_TRUE(ARR_EQ(gsink.buffer.data, expected, 5));
}

bool check_filter2(const triple_t in)
{
    return in.object == OBJECT_PROGRAMMING;
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
        .params = {.filter = {check_filter2}}
    };

    query_t query_filter = {.root = &filter_op2};

    execute_query(&query_filter, &gsource, &gsink);
    triple_t expected[4] = {
        {SUBJECT_ALICE, PREDICATE_HAS_SKILL, OBJECT_PROGRAMMING},
        {SUBJECT_CHARLIE, PREDICATE_HAS_SKILL, OBJECT_PROGRAMMING},
        {SUBJECT_DAVID, PREDICATE_HAS_SKILL, OBJECT_PROGRAMMING},
        {SUBJECT_EMILY, PREDICATE_HAS_SKILL, OBJECT_PROGRAMMING}
    };
    ASSERT_TRUE(ARR_EQ(gsink.buffer.data, expected, 4));
}


TEST_F(QueryTestFixture, test_query_window)
{
    operator_t window_op = {
        .type = WINDOW,
        .left = nullptr,
        .right = nullptr,
        .params = {.window = {8}}
    };

    query_t query_window = {.root = &window_op};

    execute_query(&query_window, &gsource, &gsink);
    triple_t expected[8] = {
        {SUBJECT_ALICE, PREDICATE_HAS_NAME, OBJECT_ALICE},
        {SUBJECT_ALICE, PREDICATE_HAS_AGE, 30},
        {SUBJECT_ALICE, PREDICATE_HAS_SKILL, OBJECT_PROGRAMMING},
        {SUBJECT_ALICE, PREDICATE_WORKS_ON, SUBJECT_PROJECT1},

        {SUBJECT_BOB, PREDICATE_HAS_NAME, OBJECT_BOB},
        {SUBJECT_BOB, PREDICATE_HAS_AGE, 25},
        {SUBJECT_BOB, PREDICATE_HAS_SKILL, OBJECT_DATA_ANALYSIS},
        {SUBJECT_BOB, PREDICATE_WORKS_ON, SUBJECT_PROJECT2},
    };
    ASSERT_TRUE(ARR_EQ(gsink.buffer.data, expected, 8));
}


TEST_F(QueryTestFixture, test_query_window2)
{
    operator_t window_op2 = {
        .type = WINDOW,
        .left = nullptr,
        .right = nullptr,
        .params = {.window = 16}
    };

    operator_t window_op = {
        .type = WINDOW,
        .left = &window_op2,
        .right = nullptr,
        .params = {.window = 8}
    };

    query_t query_window = {.root = &window_op};

    execute_query(&query_window, &gsource, &gsink);
    triple_t expected[8] = {
        {SUBJECT_ALICE, PREDICATE_HAS_NAME, OBJECT_ALICE},
        {SUBJECT_ALICE, PREDICATE_HAS_AGE, 30},
        {SUBJECT_ALICE, PREDICATE_HAS_SKILL, OBJECT_PROGRAMMING},
        {SUBJECT_ALICE, PREDICATE_WORKS_ON, SUBJECT_PROJECT1},

        {SUBJECT_BOB, PREDICATE_HAS_NAME, OBJECT_BOB},
        {SUBJECT_BOB, PREDICATE_HAS_AGE, 25},
        {SUBJECT_BOB, PREDICATE_HAS_SKILL, OBJECT_DATA_ANALYSIS},
        {SUBJECT_BOB, PREDICATE_WORKS_ON, SUBJECT_PROJECT2},
    };
    ASSERT_TRUE(ARR_EQ(gsink.buffer.data, expected, 8));
}


bool check_filter3(const triple_t in)
{
    return in.predicate == PREDICATE_REQUIRES_SKILL;
}


TEST_F(QueryTestFixture, test_query_join)
{
    operator_t filter_has_skill = {
        .type = FILTER,
        .left = nullptr,
        .right = nullptr,
        .params = {.filter = {check_filter}}
    };

    operator_t filter_req_skill = {
        .type = FILTER,
        .left = nullptr,
        .right = nullptr,
        .params = {.filter = {check_filter3}}
    };

    operator_t join_op = {
        .type = JOIN,
        .left = &filter_has_skill,
        .right = &filter_req_skill,
        .params = {.join = check_join}
    };

    query_t query = {.root = &join_op};

    execute_query(&query, &gsource, &gsink);

    triple_t expected[5] = {
        {SUBJECT_ALICE, PREDICATE_HAS_SKILL, OBJECT_PROGRAMMING},
        {SUBJECT_BOB, PREDICATE_HAS_SKILL, OBJECT_DATA_ANALYSIS},
        {SUBJECT_CHARLIE, PREDICATE_HAS_SKILL, OBJECT_PROGRAMMING},
        {SUBJECT_DAVID, PREDICATE_HAS_SKILL, OBJECT_PROGRAMMING},
        {SUBJECT_EMILY, PREDICATE_HAS_SKILL, OBJECT_PROGRAMMING},
    };
    ASSERT_TRUE(ARR_EQ(gsink.buffer.data, expected, 5));
}