//
// Created by Seppe Degryse on 16/10/2024.
//
#include "generator.h"

#include <stdlib.h>
#include <stdbool.h>
#include <string.h>

int random_arr[10] = {18, 12, 18, 17, 1, 18, 10, 13, 16, 15};


data_t get_next_generator(source_t *generator)
{
    for (int i = 0; i < GENERATOR_SIZE; ++i) {
        generator->buffer.data[i] = random_arr[i];
    }

    generator->has_next = false;
    return  generator->buffer;
}


source_t create_generator_source()
{
    return (source_t) {
        .buffer = {malloc(GENERATOR_SIZE * sizeof(int)), GENERATOR_SIZE},
        .has_next = true,
        .get_next = get_next_generator
    };
}


void free_generator_source(const source_t *source)
{
    free(source->buffer.data);
}


void push_next_sink(const sink_t *gsink, const data_t data)
{
    memcpy(gsink->buffer.data, data.data, 10 * sizeof(int));
}


sink_t create_generator_sink()
{
    return (sink_t) {
        .buffer = {malloc(GENERATOR_SIZE * sizeof(int)), GENERATOR_SIZE},
        .push_next = push_next_sink
    };
}


void free_generator_sink(const sink_t *sink)
{
    free(sink->buffer.data);
}
