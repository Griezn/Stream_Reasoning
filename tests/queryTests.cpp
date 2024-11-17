//
// Created by Seppe Degryse on 14/10/2024.
//
#include <gtest/gtest.h>

#include "test_utils.hpp"
#include "../benchmark/traffic_data.hpp"

extern "C" {
    #include "query.h"
    #include "source.h"
    #include "generator.h"
    #include "file_source.h"
}


class QueryTestFixture : public ::testing::Test {
protected:
    source_t *gsource = nullptr;
    sink_t *gsink = nullptr;
    bool skip_teardown = false;

    void SetUp() override
    {
        gsource = create_generator_source();
        gsink = create_generator_sink();
    }

    void TearDown() override
    {
        if (skip_teardown)
            return;
        free_generator_source(gsource);
        free_generator_sink(gsink);
    }
};


bool check_join(const triple_t in1, const triple_t in2)
{
    return in1.object == in2.object;
}

TEST_F(QueryTestFixture, test_query_join_death_no_children)
{
    skip_teardown = true;
    join_check_t conditions[1] = {check_join};
    operator_t join_op = {
        .type = JOIN,
        .left = nullptr,
        .right = nullptr,
        .params = {.join = {.size = 1, .checks = conditions}}
    };
    query_t query_join = {.root = &join_op};

    // Program should abort because .left and .right are NULL
    ASSERT_DEATH(execute_query(&query_join, gsource, gsink), "");
}

TEST_F(QueryTestFixture, test_query_join_death_no_left_child)
{
    skip_teardown = true;
    join_check_t conditions[1] = {check_join};
    operator_t join_op = {
        .type = JOIN,
        .left = nullptr,
        .right = &join_op,
        .params = {.join = {.size = 1, .checks = conditions}}
    };
    query_t query_join = {.root = &join_op};

    // Program should abort because .left is NULL
    ASSERT_DEATH(execute_query(&query_join, gsource, gsink), "");
}

TEST_F(QueryTestFixture, test_query_join_death_no_right_child)
{
    skip_teardown = true;
    join_check_t conditions[1] = {check_join};
    operator_t join_op = {
        .type = JOIN,
        .left = &join_op,
        .right = nullptr,
        .params = {.join = {.size = 1, .checks = conditions}}
    };
    query_t query_join = {.root = &join_op};

    // Program should abort because .right is NULL
    ASSERT_DEATH(execute_query(&query_join, gsource, gsink), "");
}


bool check_filter(const triple_t in)
{
    return in.predicate == PREDICATE_HAS_SKILL;
}

TEST_F(QueryTestFixture, test_query_filter)
{
    filter_check_t conditions[1] = {check_filter};
    operator_t filter_op = {
        .type = FILTER,
        .left = nullptr,
        .right = nullptr,
        .params = {.filter = {.size = 1, .checks = conditions}}
    };

    query_t query_filter = {.root = &filter_op};

    execute_query(&query_filter, gsource, gsink);
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
    ASSERT_TRUE(ARR_EQ(gsink->buffer.data, expected, 8));
}

bool check_filter2(const triple_t in)
{
    return in.object == OBJECT_PROGRAMMING;
}

TEST_F(QueryTestFixture, test_query_filter2)
{
    filter_check_t conditions1[1] = {check_filter};
    filter_check_t conditions2[1] = {check_filter2};
    operator_t filter_op = {
        .type = FILTER,
        .left = nullptr,
        .right = nullptr,
        .params = {.filter = {.size = 1, .checks = conditions1}}
    };

    operator_t filter_op2 = {
        .type = FILTER,
        .left = &filter_op,
        .right = nullptr,
        .params = {.filter = {.size = 1, .checks = conditions2}}
    };

    query_t query_filter = {.root = &filter_op2};

    execute_query(&query_filter, gsource, gsink);
    triple_t expected[4] = {
        {SUBJECT_ALICE, PREDICATE_HAS_SKILL, OBJECT_PROGRAMMING},
        {SUBJECT_CHARLIE, PREDICATE_HAS_SKILL, OBJECT_PROGRAMMING},
        {SUBJECT_DAVID, PREDICATE_HAS_SKILL, OBJECT_PROGRAMMING},
        {SUBJECT_EMILY, PREDICATE_HAS_SKILL, OBJECT_PROGRAMMING}
    };
    ASSERT_TRUE(ARR_EQ(gsink->buffer.data, expected, 4));
}


bool check_filter7(const triple_t in)
{
    return in.subject == SUBJECT_ALICE;
}


TEST_F(QueryTestFixture, test_query_filter3)
{
    filter_check_t conditions1[1] = {check_filter};
    filter_check_t conditions2[1] = {check_filter2};
    filter_check_t conditions3[1] = {check_filter7};
    operator_t filter_op = {
        .type = FILTER,
        .left = nullptr,
        .right = nullptr,
        .params = {.filter = {.size = 1, .checks = conditions1}}
    };

    operator_t filter_op2 = {
        .type = FILTER,
        .left = &filter_op,
        .right = nullptr,
        .params = {.filter = {.size = 1, .checks = conditions2}}
    };

    operator_t filter_op3 = {
        .type = FILTER,
        .left = &filter_op2,
        .right = nullptr,
        .params = {.filter = {.size = 1, .checks = conditions3}}
    };

    query_t query_filter = {.root = &filter_op3};

    execute_query(&query_filter, gsource, gsink);
    triple_t expected[1] = {
        {SUBJECT_ALICE, PREDICATE_HAS_SKILL, OBJECT_PROGRAMMING},
    };
    ASSERT_TRUE(ARR_EQ(gsink->buffer.data, expected, 1));
}


TEST_F(QueryTestFixture, test_query_window)
{
    operator_t window_op = {
        .type = WINDOW,
        .left = nullptr,
        .right = nullptr,
        .params = {.window = 8}
    };

    query_t query_window = {.root = &window_op};

    execute_query(&query_window, gsource, gsink);
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
    ASSERT_TRUE(ARR_EQ(gsink->buffer.data, expected, 8));
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

    execute_query(&query_window, gsource, gsink);
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
    ASSERT_TRUE(ARR_EQ(gsink->buffer.data, expected, 8));
}


bool check_filter3(const triple_t in)
{
    return in.predicate == PREDICATE_REQUIRES_SKILL;
}


/// @test Filter to get people who have a skill that is required by a project
TEST_F(QueryTestFixture, test_query_join)
{
    filter_check_t conditions1[1] = {check_filter};
    filter_check_t conditions2[1] = {check_filter3};
    join_check_t conditions3[1] = {check_join};

    operator_t filter_has_skill = {
        .type = FILTER,
        .left = nullptr,
        .right = nullptr,
        .params = {.filter = {.size = 1, .checks = conditions1}}
    };

    operator_t filter_req_skill = {
        .type = FILTER,
        .left = nullptr,
        .right = nullptr,
        .params = {.filter = {.size = 1, .checks = conditions2}}
    };

    operator_t join_op = {
        .type = JOIN,
        .left = &filter_has_skill,
        .right = &filter_req_skill,
        .params = {.join = {.size = 1, .checks = conditions3}}
    };

    query_t query = {.root = &join_op};

    execute_query(&query, gsource, gsink);

    triple_t expected[10] = {
        {SUBJECT_ALICE, PREDICATE_HAS_SKILL, OBJECT_PROGRAMMING},
        {SUBJECT_PROJECT1, PREDICATE_REQUIRES_SKILL, OBJECT_PROGRAMMING},

        {SUBJECT_BOB, PREDICATE_HAS_SKILL, OBJECT_DATA_ANALYSIS},
        {SUBJECT_PROJECT2, PREDICATE_REQUIRES_SKILL, OBJECT_DATA_ANALYSIS},

        {SUBJECT_CHARLIE, PREDICATE_HAS_SKILL, OBJECT_PROGRAMMING},
        {SUBJECT_PROJECT1, PREDICATE_REQUIRES_SKILL, OBJECT_PROGRAMMING},

        {SUBJECT_DAVID, PREDICATE_HAS_SKILL, OBJECT_PROGRAMMING},
        {SUBJECT_PROJECT1, PREDICATE_REQUIRES_SKILL, OBJECT_PROGRAMMING},

        {SUBJECT_EMILY, PREDICATE_HAS_SKILL, OBJECT_PROGRAMMING},
        {SUBJECT_PROJECT1, PREDICATE_REQUIRES_SKILL, OBJECT_PROGRAMMING},
    };
    ASSERT_TRUE(ARR_EQ(gsink->buffer.data, expected, 10));
}


TEST_F(QueryTestFixture, test_query_select)
{
    filter_check_t conditions1[1] = {check_filter};
    filter_check_t conditions2[1] = {check_filter3};
    join_check_t conditions3[1] = {check_join};
    uint8_t predicates[1] = {PREDICATE_HAS_SKILL};

    operator_t filter_has_skill = {
        .type = FILTER,
        .left = nullptr,
        .right = nullptr,
        .params = {.filter = {.size = 1, .checks = conditions1}}
    };

    operator_t filter_req_skill = {
        .type = FILTER,
        .left = nullptr,
        .right = nullptr,
        .params = {.filter = {.size = 1, .checks = conditions2}}
    };

    operator_t join_op = {
        .type = JOIN,
        .left = &filter_has_skill,
        .right = &filter_req_skill,
        .params = {.join = {.size = 1, .checks = conditions3}}
    };

    operator_t select_op = {
        .type = SELECT,
        .left = &join_op,
        .right = nullptr,
        .params = {.select = {.size = 1, .colums = predicates}}
    };

    query_t query = {.root = &select_op};

    execute_query(&query, gsource, gsink);

    triple_t expected[5] = {
        {SUBJECT_ALICE, PREDICATE_HAS_SKILL, OBJECT_PROGRAMMING},

        {SUBJECT_BOB, PREDICATE_HAS_SKILL, OBJECT_DATA_ANALYSIS},

        {SUBJECT_CHARLIE, PREDICATE_HAS_SKILL, OBJECT_PROGRAMMING},

        {SUBJECT_DAVID, PREDICATE_HAS_SKILL, OBJECT_PROGRAMMING},

        {SUBJECT_EMILY, PREDICATE_HAS_SKILL, OBJECT_PROGRAMMING},
    };
    ASSERT_TRUE(ARR_EQ(gsink->buffer.data, expected, 5));
}


bool check_filter4(const triple_t in)
{
    return in.predicate == PREDICATE_HAS_AGE;
}


bool check_join2(const triple_t in1, const triple_t in2)
{
    return in1.subject == in2.subject;
}


bool check_filter5(const triple_t in)
{
    return in.object > 30;
}


/// @test Filter to get people who have a skill that is required by a project
/// And that are older than 30
TEST_F(QueryTestFixture, test_query_1)
{
    filter_check_t conditions1[1] = {check_filter};
    filter_check_t conditions2[1] = {check_filter3};
    join_check_t conditions3[1] = {check_join};
    filter_check_t conditions4[1] = {check_filter4};
    join_check_t conditions5[1] = {check_join2};
    filter_check_t conditions6[1] = {check_filter5};

    operator_t filter_has_skill = {
        .type = FILTER,
        .left = nullptr,
        .right = nullptr,
        .params = {.filter = {.size = 1, .checks = conditions1}}
    };

    operator_t filter_req_skill = {
        .type = FILTER,
        .left = nullptr,
        .right = nullptr,
        .params = {.filter = {.size = 1, .checks = conditions2}}
    };

    operator_t join_skill = {
        .type = JOIN,
        .left = &filter_has_skill,
        .right = &filter_req_skill,
        .params = {.join = {.size = 1, .checks = conditions3}}
    };

    operator_t filter_has_age = {
        .type = FILTER,
        .left = nullptr,
        .right = nullptr,
        .params = {.filter = {.size = 1, .checks = conditions4}}
    };

    operator_t join_age = {
        .type = JOIN,
        .left = &filter_has_age,
        .right = &join_skill,
        .params = {.join = {.size = 1, .checks = conditions5}}
    };

    operator_t filter_older = {
        .type = FILTER,
        .left = &join_age,
        .right = nullptr,
        .params = {.filter = {.size = 1, .checks = conditions6}}
    };

    query_t query = {.root = &filter_older};

    execute_query(&query, gsource, gsink);

    triple_t expected[6] = {
        {SUBJECT_CHARLIE, PREDICATE_HAS_AGE, 35},
        {SUBJECT_CHARLIE, PREDICATE_HAS_SKILL, OBJECT_PROGRAMMING},
        {SUBJECT_PROJECT1, PREDICATE_REQUIRES_SKILL, OBJECT_PROGRAMMING},

        {SUBJECT_DAVID, PREDICATE_HAS_AGE, 40},
        {SUBJECT_DAVID, PREDICATE_HAS_SKILL, OBJECT_PROGRAMMING},
        {SUBJECT_PROJECT1, PREDICATE_REQUIRES_SKILL, OBJECT_PROGRAMMING},
    };
    ASSERT_TRUE(ARR_EQ(gsink->buffer.data, expected, 6));
}

bool check_join3(const triple_t in1, const triple_t in2)
{
    return in1.subject == in2.subject;
}

TEST_F(QueryTestFixture, test_query_select_join_file)
{
    skip_teardown = true;
    uint8_t predicates[1] = {SOSA_OBSERVATION};
    uint8_t predicates2[1] = {SOSA_HAS_SIMPLE_RESULT};
    join_check_t conditions3[1] = {check_join3};

    operator_t select_obs = {
        .type = SELECT,
        .left = nullptr,
        .right = nullptr,
        .params = {.select = {.size = 1, .colums = predicates}}
    };

    operator_t select_simple_result = {
        .type = SELECT,
        .left = nullptr,
        .right = nullptr,
        .params = {.select = {.size = 1, .colums = predicates2}}
    };

    operator_t join_obs = {
        .type = JOIN,
        .left = &select_obs,
        .right = &select_simple_result,
        .params = {.join = {.size = 1, .checks = conditions3}}
    };


    const query_t query = {.root = &join_obs};

    // Create generator source and sink
    source_t *source = create_file_source("../../benchmark/traffic_triples1.bin", 5, 255);
    sink_t *sink = create_file_sink();

    // Benchmark loop
    execute_query(&query, source, sink);

    // Clean up
    free_file_source(source);
    free_file_sink(sink);
}
