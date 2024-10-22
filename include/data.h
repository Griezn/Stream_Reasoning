//
// Created by Seppe Degryse on 16/10/2024.
//
#ifndef DATA_H
#define DATA_H

typedef struct Triple {
    unsigned char subject;
    unsigned char predicate;
    unsigned char object;
} triple_t;

typedef struct Data {
    triple_t *data;
    unsigned char size;
} data_t;

#endif //DATA_H
