//
// Created by Seppe Degryse on 16/10/2024.
//
#ifndef DATA_H
#define DATA_H
#include <stdint.h>

typedef struct Triple {
    uint32_t subject;
    uint32_t predicate;
    uint32_t object;
} triple_t;

typedef struct Data {
    struct Triple *data;
    uint32_t size;
    uint8_t width;
    uint32_t cap;
} data_t;

void free_data(data_t *data);

#endif //DATA_H
