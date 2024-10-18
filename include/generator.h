//
// Created by Seppe Degryse on 16/10/2024.
//
#ifndef GENERATOR_H
#define GENERATOR_H
#include "source.h"

source_t create_generator_source();

sink_t create_generator_sink();

void free_generator_source(const source_t *source);

void free_generator_sink(const sink_t *sink);

#define GENERATOR_SIZE 10

#endif //GENERATOR_H
