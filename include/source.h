//
// Created by Seppe Degryse on 16/10/2024.
//
#ifndef SOURCE_H
#define SOURCE_H
#include "data.h"

// Created a source to enable other sources than generator e.g. network
typedef struct Source {
    data_t buffer;
    uint32_t index;
    uint8_t consumers;
    uint8_t consumed;
    data_t* (*get_next)(const struct Source *self, const uint8_t size, const uint8_t step);
} source_t;

typedef struct Sink {
    data_t buffer;
    void (*push_next)(struct Sink *self, const data_t *data);
} sink_t;

static inline void source_set_comsumers(source_t *source, const uint8_t consumers)
{
    source->consumers = consumers;
}

#endif //SOURCE_H
