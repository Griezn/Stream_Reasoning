//
// Created by Seppe Degryse on 16/10/2024.
//
#ifndef GENERATOR_H
#define GENERATOR_H
#include "source.h"

source_t create_generator_source();

sink_t create_generator_sink();

void free_generator_source(const source_t *source);

void free_generator_sink(sink_t *sink);

#define GENERATOR_SIZE 10

enum subject {
    SUBJECT_ALICE,
    SUBJECT_BOB,
    SUBJECT_CHARLIE,
    SUBJECT_DAVID,
    SUBJECT_EMILY,
    SUBJECT_FRANK,
    SUBJECT_GRACE,
    SUBJECT_HELEN,
    SUBJECT_PROJECT1,
    SUBJECT_PROJECT2,
};

enum predicate {
    PREDICATE_HAS_NAME,
    PREDICATE_HAS_AGE,
    PREDICATE_HAS_SKILL,
    PREDICATE_WORKS_ON,
    PREDICATE_REQUIRES_SKILL,
};

enum object {
    OBJECT_ALICE,
    OBJECT_BOB,
    OBJECT_CHARLIE,
    OBJECT_DAVID,
    OBJECT_EMILY,
    OBJECT_FRANK,
    OBJECT_GRACE,
    OBJECT_HELEN,
    OBJECT_PROGRAMMING,
    OBJECT_DATA_ANALYSIS,
    OBJECT_AI_RESEARCH,
    OBJECT_DATA_SCIENCE,
    OBJECT_GRAPHIC_DESIGN,
    OBJECT_MARKETING,
    OBJECT_PROJECT_MANAGEMENT
};

#endif //GENERATOR_H
