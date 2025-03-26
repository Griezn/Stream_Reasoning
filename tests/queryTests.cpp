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
    source_t *gsource = nullptr;
    sink_t *gsink = nullptr;
    query_t gquery = {};

    void SetUp() override
    {
        gsource = create_generator_source(1);
        gsink = create_generator_sink();
    }

    void TearDown() override
    {
        free_generator_source(gsource);
        free_generator_sink(gsink);
    }
};


bool check_join(const triple_t in1, const triple_t in2)
{
    return in1.object == in2.object;
}


bool check_filter(const triple_t in)
{
    return in.predicate == PREDICATE_HAS_SKILL;
}


bool check_filter_join(const triple_t in1, const triple_t in2)
{
    return in1.predicate == PREDICATE_HAS_SKILL;
}

TEST_F(QueryTestFixture, test_query_filter)
{
    window_params_t wparams = {36, 36,  gsource};
    operator_t window_op = {
        .type = WINDOW,
        .left = nullptr,
        .right = nullptr,
        .params = {.window = wparams}
    };

    filter_check_t conditions[1] = {check_filter};
    operator_t filter_op = {
        .type = FILTER,
        .left = &window_op,
        .right = nullptr,
        .params = {.filter = {.size = 1, .checks = conditions}}
    };

    gquery = {.root = &filter_op};

    execute_query(&gquery, gsink);
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
    window_params_t wparams = {36, 36,  gsource};
    operator_t window_op = {
        .type = WINDOW,
        .left = nullptr,
        .right = nullptr,
        .params = {.window = wparams}
    };

    filter_check_t conditions1[1] = {check_filter};
    filter_check_t conditions2[1] = {check_filter2};
    operator_t filter_op = {
        .type = FILTER,
        .left = &window_op,
        .right = nullptr,
        .params = {.filter = {.size = 1, .checks = conditions1}}
    };

    operator_t filter_op2 = {
        .type = FILTER,
        .left = &filter_op,
        .right = nullptr,
        .params = {.filter = {.size = 1, .checks = conditions2}}
    };

    gquery = {.root = &filter_op2};

    execute_query(&gquery, gsink);
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
    window_params_t wparams = {36, 36,  gsource};
    operator_t window_op = {
        .type = WINDOW,
        .left = nullptr,
        .right = nullptr,
        .params = {.window = wparams}
    };

    filter_check_t conditions1[1] = {check_filter};
    filter_check_t conditions2[1] = {check_filter2};
    filter_check_t conditions3[1] = {check_filter7};
    operator_t filter_op = {
        .type = FILTER,
        .left = &window_op,
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

    gquery = {.root = &filter_op3};

    execute_query(&gquery, gsink);
    triple_t expected[1] = {
        {SUBJECT_ALICE, PREDICATE_HAS_SKILL, OBJECT_PROGRAMMING},
    };
    ASSERT_TRUE(ARR_EQ(gsink->buffer.data, expected, 1));
}


/*
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
*/


bool check_filter3(const triple_t in)
{
    return in.predicate == PREDICATE_REQUIRES_SKILL;
}


bool check_filter3_join(const triple_t in1, const triple_t in2)
{
    return in2.predicate == PREDICATE_REQUIRES_SKILL;
}


/// @test Filter to get people who have a skill that is required by a project
TEST_F(QueryTestFixture, test_query_join)
{
    //source_set_comsumers(gsource, 2);
    source_t *source2 = create_generator_source(1);

    window_params_t wparams = {36, 36,  gsource};
    operator_t window_op = {
        .type = WINDOW,
        .left = nullptr,
        .right = nullptr,
        .params = {.window = wparams}
    };

    window_params_t wparams2 = {36, 36,  source2};
    operator_t window_op2 = {
        .type = WINDOW,
        .left = nullptr,
        .right = nullptr,
        .params = {.window = wparams2}
    };

    filter_check_t conditions1[1] = {check_filter};
    filter_check_t conditions2[1] = {check_filter3};
    join_check_t conditions3[1] = {check_join};

    operator_t filter_has_skill = {
        .type = FILTER,
        .left = &window_op,
        .right = nullptr,
        .params = {.filter = {.size = 1, .checks = conditions1}}
    };

    operator_t filter_req_skill = {
        .type = FILTER,
        .left = &window_op2,
        .right = nullptr,
        .params = {.filter = {.size = 1, .checks = conditions2}}
    };

    operator_t join_op = {
        .type = JOIN,
        .left = &filter_has_skill,
        .right = &filter_req_skill,
        .params = {.join = {.size = 1, .checks = conditions3}}
    };

    gquery = {.root = &join_op};

    execute_query(&gquery, gsink);

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

    free_generator_source(source2);
}


TEST_F(QueryTestFixture, test_query_join_joined)
{
    //source_set_comsumers(gsource, 2);
    source_t *source2 = create_generator_source(1);

    window_params_t wparams = {36, 36,  gsource};
    operator_t window_op = {
        .type = WINDOW,
        .left = nullptr,
        .right = nullptr,
        .params = {.window = wparams}
    };

    window_params_t wparams2 = {36, 36,  source2};
    operator_t window_op2 = {
        .type = WINDOW,
        .left = nullptr,
        .right = nullptr,
        .params = {.window = wparams2}
    };

    join_check_t conditions3[3] = {check_join, check_filter_join, check_filter3_join};

    operator_t join_op = {
        .type = JOIN,
        .left = &window_op,
        .right = &window_op2,
        .params = {.join = {.size = 3, .checks = conditions3}}
    };

    gquery = {.root = &join_op};

    execute_query(&gquery, gsink);

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

    free_generator_source(source2);
}


TEST_F(QueryTestFixture, test_query_select)
{
    //source_set_comsumers(gsource, 2);
    source_t *source2 = create_generator_source(1);
    window_params_t wparams = {36, 36,   gsource};

    operator_t window_op = {
        .type = WINDOW,
        .left = nullptr,
        .right = nullptr,
        .params = {.window = wparams}
    };

    window_params_t wparams2 = {36, 36,   source2};
    operator_t window_op2 = {
        .type = WINDOW,
        .left = nullptr,
        .right = nullptr,
        .params = {.window = wparams2}
    };

    filter_check_t conditions1[1] = {check_filter};
    filter_check_t conditions2[1] = {check_filter3};
    join_check_t conditions3[1] = {check_join};
    uint8_t predicates[1] = {PREDICATE_HAS_SKILL};

    operator_t filter_has_skill = {
        .type = FILTER,
        .left = &window_op,
        .right = nullptr,
        .params = {.filter = {.size = 1, .checks = conditions1}}
    };

    operator_t filter_req_skill = {
        .type = FILTER,
        .left = &window_op2,
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
        .params = {.select = {.width = 1, .size = 1, .colums = predicates}}
    };

    gquery = {.root = &select_op};

    execute_query(&gquery, gsink);

    triple_t expected[5] = {
        {SUBJECT_ALICE, PREDICATE_HAS_SKILL, OBJECT_PROGRAMMING},

        {SUBJECT_BOB, PREDICATE_HAS_SKILL, OBJECT_DATA_ANALYSIS},

        {SUBJECT_CHARLIE, PREDICATE_HAS_SKILL, OBJECT_PROGRAMMING},

        {SUBJECT_DAVID, PREDICATE_HAS_SKILL, OBJECT_PROGRAMMING},

        {SUBJECT_EMILY, PREDICATE_HAS_SKILL, OBJECT_PROGRAMMING},
    };
    ASSERT_TRUE(ARR_EQ(gsink->buffer.data, expected, 5));

    free_generator_source(source2);
}


TEST_F(QueryTestFixture, test_query_select2)
{
    //source_set_comsumers(gsource, 2);
    source_t *source2 = create_generator_source(1);
    window_params_t wparams = {36, 36, gsource};

    operator_t window_op = {
        .type = WINDOW,
        .left = nullptr,
        .right = nullptr,
        .params = {.window = wparams}
    };

    window_params_t wparams2 = {36, 36, source2};
    operator_t window_op2 = {
        .type = WINDOW,
        .left = nullptr,
        .right = nullptr,
        .params = {.window = wparams2}
    };

    filter_check_t conditions1[1] = {check_filter};
    filter_check_t conditions2[1] = {check_filter3};
    join_check_t conditions3[1] = {check_join};
    uint8_t predicates[2] = {PREDICATE_HAS_SKILL, PREDICATE_REQUIRES_SKILL};

    operator_t filter_has_skill = {
        .type = FILTER,
        .left = &window_op,
        .right = nullptr,
        .params = {.filter = {.size = 1, .checks = conditions1}}
    };

    operator_t filter_req_skill = {
        .type = FILTER,
        .left = &window_op2,
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
        .params = {.select = {.width = 2, .size = 2, .colums = predicates}}
    };

    gquery = {.root = &select_op};

    execute_query(&gquery, gsink);

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

    free_generator_source(source2);
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
    //source_set_comsumers(gsource, 3);
    source_t *source2 = create_generator_source(1);
    source_t *source3 = create_generator_source(1);

    window_params_t wparams = {36, 36,   gsource};
    operator_t window_op = {
        .type = WINDOW,
        .left = nullptr,
        .right = nullptr,
        .params = {.window = wparams}
    };

    window_params_t wparams2 = {36, 36,   source2};
    operator_t window_op2 = {
        .type = WINDOW,
        .left = nullptr,
        .right = nullptr,
        .params = {.window = wparams2}
    };

    window_params_t wparams3 = {36, 36,   source3};
    operator_t window_op3 = {
        .type = WINDOW,
        .left = nullptr,
        .right = nullptr,
        .params = {.window = wparams3}
    };

    filter_check_t conditions1[1] = {check_filter};
    filter_check_t conditions2[1] = {check_filter3};
    join_check_t conditions3[1] = {check_join};
    filter_check_t conditions4[1] = {check_filter4};
    join_check_t conditions5[1] = {check_join2};
    filter_check_t conditions6[1] = {check_filter5};

    operator_t filter_has_skill = {
        .type = FILTER,
        .left = &window_op,
        .right = nullptr,
        .params = {.filter = {.size = 1, .checks = conditions1}}
    };

    operator_t filter_req_skill = {
        .type = FILTER,
        .left = &window_op2,
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
        .left = &window_op3,
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

    gquery = {.root = &filter_older};

    execute_query(&gquery, gsink);

    triple_t expected[6] = {
        {SUBJECT_CHARLIE, PREDICATE_HAS_AGE, 35},
        {SUBJECT_CHARLIE, PREDICATE_HAS_SKILL, OBJECT_PROGRAMMING},
        {SUBJECT_PROJECT1, PREDICATE_REQUIRES_SKILL, OBJECT_PROGRAMMING},

        {SUBJECT_DAVID, PREDICATE_HAS_AGE, 40},
        {SUBJECT_DAVID, PREDICATE_HAS_SKILL, OBJECT_PROGRAMMING},
        {SUBJECT_PROJECT1, PREDICATE_REQUIRES_SKILL, OBJECT_PROGRAMMING},
    };
    ASSERT_TRUE(ARR_EQ(gsink->buffer.data, expected, 6));

    free_generator_source(source2);
    free_generator_source(source3);
}
