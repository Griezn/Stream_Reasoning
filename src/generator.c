//
// Created by Seppe Degryse on 16/10/2024.
//
#include "generator.h"

#include <assert.h>
#include <stdlib.h>


triple_t triples[] = {
    {SUBJECT_ALICE, PREDICATE_HAS_NAME, OBJECT_ALICE},
    {SUBJECT_ALICE, PREDICATE_HAS_AGE, 30},
    {SUBJECT_ALICE, PREDICATE_HAS_SKILL, OBJECT_PROGRAMMING},
    {SUBJECT_ALICE, PREDICATE_WORKS_ON, SUBJECT_PROJECT1},

    {SUBJECT_BOB, PREDICATE_HAS_NAME, OBJECT_BOB},
    {SUBJECT_BOB, PREDICATE_HAS_AGE, 25},
    {SUBJECT_BOB, PREDICATE_HAS_SKILL, OBJECT_DATA_ANALYSIS},
    {SUBJECT_BOB, PREDICATE_WORKS_ON, SUBJECT_PROJECT2},

    {SUBJECT_CHARLIE, PREDICATE_HAS_NAME, OBJECT_CHARLIE},
    {SUBJECT_CHARLIE, PREDICATE_HAS_AGE, 35},
    {SUBJECT_CHARLIE, PREDICATE_HAS_SKILL, OBJECT_PROGRAMMING},
    {SUBJECT_CHARLIE, PREDICATE_WORKS_ON, SUBJECT_PROJECT1},

    {SUBJECT_DAVID, PREDICATE_HAS_NAME, OBJECT_DAVID},
    {SUBJECT_DAVID, PREDICATE_HAS_AGE, 40},
    {SUBJECT_DAVID, PREDICATE_HAS_SKILL, OBJECT_PROGRAMMING},
    {SUBJECT_DAVID, PREDICATE_WORKS_ON, SUBJECT_PROJECT1},

    {SUBJECT_EMILY, PREDICATE_HAS_NAME, OBJECT_EMILY},
    {SUBJECT_EMILY, PREDICATE_HAS_AGE, 28},
    {SUBJECT_EMILY, PREDICATE_HAS_SKILL, OBJECT_PROGRAMMING},
    {SUBJECT_EMILY, PREDICATE_WORKS_ON, SUBJECT_PROJECT1},

    {SUBJECT_FRANK, PREDICATE_HAS_NAME, OBJECT_FRANK},
    {SUBJECT_FRANK, PREDICATE_HAS_AGE, 32},
    {SUBJECT_FRANK, PREDICATE_HAS_SKILL, OBJECT_GRAPHIC_DESIGN},
    {SUBJECT_FRANK, PREDICATE_WORKS_ON, SUBJECT_PROJECT1},

    {SUBJECT_GRACE, PREDICATE_HAS_NAME, OBJECT_GRACE},
    {SUBJECT_GRACE, PREDICATE_HAS_AGE, 29},
    {SUBJECT_GRACE, PREDICATE_HAS_SKILL, OBJECT_MARKETING},
    {SUBJECT_GRACE, PREDICATE_WORKS_ON, SUBJECT_PROJECT2},

    {SUBJECT_HELEN, PREDICATE_HAS_NAME, OBJECT_HELEN},
    {SUBJECT_HELEN, PREDICATE_HAS_AGE, 45},
    {SUBJECT_HELEN, PREDICATE_HAS_SKILL, OBJECT_PROJECT_MANAGEMENT},
    {SUBJECT_HELEN, PREDICATE_WORKS_ON, SUBJECT_PROJECT1},

    {SUBJECT_PROJECT1, PREDICATE_HAS_NAME, OBJECT_AI_RESEARCH},
    {SUBJECT_PROJECT1, PREDICATE_REQUIRES_SKILL, OBJECT_PROGRAMMING},

    {SUBJECT_PROJECT2, PREDICATE_HAS_NAME, OBJECT_DATA_SCIENCE},
    {SUBJECT_PROJECT2, PREDICATE_REQUIRES_SKILL, OBJECT_DATA_ANALYSIS}
};

#define NUM_TRIPLES (sizeof(triples) / sizeof(triples[0]))


data_t* get_next_generator(source_t *generator)
{
    generator->has_next = false;
    return  &generator->buffer;
}


source_t create_generator_source()
{
    return (source_t) {
        .buffer = {triples, NUM_TRIPLES, 1},
        .has_next = true,
        .get_next = get_next_generator
    };
}


void free_generator_source(const source_t *source)
{
    (void)source;
}


void push_next_sink(sink_t *gsink, const data_t *data)
{
    free_generator_sink(gsink);
    gsink->buffer = *data;
}


sink_t create_generator_sink()
{
    return (sink_t) {
        .buffer = {malloc(NUM_TRIPLES * sizeof(triple_t)), 0, 1},
        .push_next = push_next_sink
    };
}


void free_generator_sink(sink_t *sink)
{
    assert(sink->buffer.data);

    free(sink->buffer.data);
    sink->buffer.data = NULL;
}
