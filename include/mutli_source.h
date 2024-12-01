//
// Created by Seppe Degryse on 01/12/2024.
//

#ifndef MUTLISOURCE_H
#define MUTLISOURCE_H
#include "source.h"

typedef struct MultiSource {
    source_t source;
    uint8_t num_sources;
    uint8_t capacity;
    source_t* *sources; // array of source ptrs
} multi_source_t;

source_t *create_multi_source();

sink_t *create_multi_sink();

void multi_source_add(source_t* multi_source, const source_t* source);

void free_multi_source(source_t *source);

void free_multi_sink(sink_t *sink);

#endif //MUTLISOURCE_H
