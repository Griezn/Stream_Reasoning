//
// Created by Seppe Degryse on 16/10/2024.
//
#ifndef SOURCE_H
#define SOURCE_H
#include "data.h"

#include <stdbool.h>

// Created a source to enable other sources than generator e.g. network
typedef struct Source {
    data_t buffer;
    uint8_t index;
    uint8_t calls;
    data_t* (*get_next)(const struct Source *self, const uint8_t size, const uint8_t step, const uint8_t calls);
} source_t;

typedef struct Sink {
    data_t buffer;
    void (*push_next)(struct Sink *self, const data_t *data);
} sink_t;

#endif //SOURCE_H
