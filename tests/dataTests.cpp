//
// Created by Seppe Degryse on 30/10/2024.
//
#include <gtest/gtest.h>

#include "test_utils.hpp"


extern "C" {
    #include "data.h"
    #include "defs.h"
    #include "generator.h"
    #include "file_source.h"
}


bool condition1(const triple_t in1, const triple_t in2)
{
    return in1.subject == in2.subject;
}

bool condition2(const triple_t in1, const triple_t in2)
{
    return in1.predicate == PREDICATE_REQUIRES_SKILL && in2.predicate == PREDICATE_HAS_SKILL &&
        in1.object != in2.object;
}


TEST(DataTests, test_join_check)
{
    triple_t input1[8] = {
        {SUBJECT_ALICE, PREDICATE_WORKS_ON, SUBJECT_PROJECT1},
        {SUBJECT_PROJECT1, PREDICATE_REQUIRES_SKILL, OBJECT_PROGRAMMING},

        {SUBJECT_BOB, PREDICATE_WORKS_ON, SUBJECT_PROJECT2},
        {SUBJECT_PROJECT2, PREDICATE_REQUIRES_SKILL, OBJECT_DATA_ANALYSIS},

        {SUBJECT_CHARLIE, PREDICATE_WORKS_ON, SUBJECT_PROJECT1},
        {SUBJECT_PROJECT1, PREDICATE_REQUIRES_SKILL, OBJECT_PROGRAMMING},

        {SUBJECT_FRANK, PREDICATE_WORKS_ON, SUBJECT_PROJECT1}, // wrong project
        {SUBJECT_PROJECT1, PREDICATE_REQUIRES_SKILL, OBJECT_PROGRAMMING},
    };

    data_t i1 = {
        .data = input1,
        .size = 8,
        .width = 2
    };

    triple_t input2[4] = {
        {SUBJECT_ALICE, PREDICATE_HAS_SKILL, OBJECT_PROGRAMMING},

        {SUBJECT_BOB, PREDICATE_HAS_SKILL, OBJECT_DATA_ANALYSIS},

        {SUBJECT_CHARLIE, PREDICATE_HAS_SKILL, OBJECT_PROGRAMMING},

         {SUBJECT_FRANK, PREDICATE_HAS_SKILL, OBJECT_GRAPHIC_DESIGN},
    };


    data_t i2 = {
        .data = input2,
        .size = 4,
        .width = 1
    };


    join_check_t conidtions[2] = {condition1, condition2};

    join_params_t checks = {
        .size = 2,
        .checks = conidtions
    };

    ASSERT_FALSE(join_check(&i1, 0, &i2, 0, checks));
    ASSERT_FALSE(join_check(&i1, 2, &i2, 0, checks));
    ASSERT_FALSE(join_check(&i1, 4, &i2, 0, checks));
    ASSERT_FALSE(join_check(&i1, 6, &i2, 0, checks));

    ASSERT_FALSE(join_check(&i1, 0, &i2, 1, checks));
    ASSERT_FALSE(join_check(&i1, 2, &i2, 1, checks));
    ASSERT_FALSE(join_check(&i1, 4, &i2, 1, checks));
    ASSERT_FALSE(join_check(&i1, 6, &i2, 1, checks));

    ASSERT_FALSE(join_check(&i1, 0, &i2, 2, checks));
    ASSERT_FALSE(join_check(&i1, 2, &i2, 2, checks));
    ASSERT_FALSE(join_check(&i1, 4, &i2, 1, checks));
    ASSERT_FALSE(join_check(&i1, 6, &i2, 1, checks));

    ASSERT_FALSE(join_check(&i1, 0, &i2, 3, checks));
    ASSERT_FALSE(join_check(&i1, 2, &i2, 3, checks));
    ASSERT_FALSE(join_check(&i1, 4, &i2, 3, checks));
    ASSERT_TRUE(join_check(&i1, 6, &i2, 3, checks));
}

bool fcondition1(const triple_t in)
{
    return in.predicate == PREDICATE_WORKS_ON && in.object == SUBJECT_PROJECT1;
}

bool fcondition2(const triple_t in)
{
    return in.predicate == PREDICATE_HAS_AGE && in.object >= 30;
}

TEST(DataTests, test_filter_check)
{
    triple_t input[12] = {
        {SUBJECT_ALICE, PREDICATE_WORKS_ON, SUBJECT_PROJECT1},
        {SUBJECT_ALICE, PREDICATE_HAS_AGE, 30},
        {SUBJECT_PROJECT1, PREDICATE_REQUIRES_SKILL, OBJECT_PROGRAMMING},

        {SUBJECT_BOB, PREDICATE_WORKS_ON, SUBJECT_PROJECT2},
        {SUBJECT_BOB, PREDICATE_HAS_AGE, 25},
        {SUBJECT_PROJECT2, PREDICATE_REQUIRES_SKILL, OBJECT_DATA_ANALYSIS},

        {SUBJECT_CHARLIE, PREDICATE_WORKS_ON, SUBJECT_PROJECT1},
        {SUBJECT_CHARLIE, PREDICATE_HAS_AGE, 35},
        {SUBJECT_PROJECT1, PREDICATE_REQUIRES_SKILL, OBJECT_PROGRAMMING},

        {SUBJECT_FRANK, PREDICATE_WORKS_ON, SUBJECT_PROJECT1},
        {SUBJECT_FRANK, PREDICATE_HAS_AGE, 32},
        {SUBJECT_PROJECT1, PREDICATE_REQUIRES_SKILL, OBJECT_PROGRAMMING},
    };

    data_t in = {
        .data = input,
        .size = 12,
        .width = 3
    };

    filter_check_t conidtions[2] = {fcondition1, fcondition2};

    filter_params_t checks = {
        .size = 2,
        .checks = conidtions
    };

    ASSERT_TRUE(filter_check(&in, 0, checks));
    ASSERT_FALSE(filter_check(&in, 3, checks));
    ASSERT_TRUE(filter_check(&in, 6, checks));
    ASSERT_TRUE(filter_check(&in, 9, checks));
}


TEST(DataTests, test_file_source)
{
    source_t *gsource = create_generator_source();
    source_t *fsource = create_file_source("../../tests/triples.bin" ,1, 255);

    sink_t *gsink = create_generator_sink();
    sink_t *fsink = create_generator_sink();

    gsink->push_next(gsink, gsource->get_next(gsource));
    fsink->push_next(fsink, fsource->get_next(fsource));

    ASSERT_TRUE(ARR_EQ(gsink->buffer.data, fsink->buffer.data, gsink->buffer.size));

    free_generator_source(gsource);
    free_file_source(fsource);
    free(gsink); // not the normal because it is still the array allocated int he lib
    free(fsink); // not the normal because buffer is freed in source
}


TEST(DataTests, test_file_source_inc)
{
    constexpr uint32_t increment = 12;
    source_t *gsource = create_generator_source();
    source_t *fsource = create_file_source("../../tests/triples.bin" ,1, increment);

    sink_t *gsink = create_generator_sink();
    sink_t *fsink = create_generator_sink();

    data_t *next_gdata = gsource->get_next(gsource);
    data_t *next_fdata = fsource->get_next(fsource);

    gsink->push_next(gsink, next_gdata);
    fsink->push_next(fsink, next_fdata);

    ASSERT_TRUE(ARR_EQ(gsink->buffer.data, fsink->buffer.data, increment));

    free(next_fdata);
    next_fdata = fsource->get_next(fsource);
    fsink->push_next(fsink, next_fdata);

    ASSERT_TRUE(ARR_EQ(gsink->buffer.data + increment, fsink->buffer.data, increment));

    free(next_fdata);
    next_fdata = fsource->get_next(fsource);
    fsink->push_next(fsink, next_fdata);

    ASSERT_TRUE(ARR_EQ(gsink->buffer.data + 2*increment, fsink->buffer.data, increment));

    free_generator_source(gsource);
    free_file_source(fsource);
    free(gsink); // not the normal because it is still the array allocated int he lib
    free(fsink); // not the normal because buffer is freed in source
}
